import hashlib
import json
import sqlite3
import tempfile
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

import backend.closing_tape.historical_import as historical_import
from backend.closing_tape.config import DEFAULT_OPTION_PARENTS
from backend.closing_tape.contracts import HISTORICAL_EVIDENCE_CONTRACT_VERSION
from backend.closing_tape.historical_import import (
    HistoricalImportResult,
    _bundle_identity,
    _catalog_capacity_requirement,
    _canonical_hash,
    _canonical_windows,
    _disk_usage_probe_path,
    _mapping_summary,
    _provenance_identity,
    _same_storage_volume,
    describe_historical_import,
    import_historical_bundle,
    verify_historical_bundle_manifest,
)


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_manifest(tmp_path):
    trading_day = date(2026, 8, 27)
    windows = _canonical_windows(trading_day)
    components = []
    mapping_hash = "1" * 64
    family_counts = {
        "SPX": 1,
        "SPXW": 1,
        "NDX": 1,
        "NDXP": 1,
        "RUT": 1,
        "RUTW": 1,
        "VIX": 1,
        "VIXW": 1,
        "SPY": 1,
    }
    for window in windows:
        schema = window["schema"]
        component_path = tmp_path / f"fixture.{schema}.dbn.zst"
        component_path.write_bytes(f"immutable-{schema}".encode())
        validation = {
            "dbn_compression": "zstd",
            "dbn_dataset": "OPRA.PILLAR",
            "dbn_end_utc": window["end_utc"],
            "dbn_format_version": 1,
            "dbn_mapping_count": 9,
            "dbn_mapping_sha256": mapping_hash,
            "dbn_schema": schema,
            "dbn_start_utc": window["start_utc"],
            "dbn_stype_in": "parent",
            "dbn_stype_out": "instrument_id",
            "dbn_symbols": list(DEFAULT_OPTION_PARENTS),
            "family_coverage_complete": True,
            "family_record_counts": family_counts,
            "first_ts_event_ns": 1,
            "first_ts_recv_ns": 2,
            "last_ts_event_ns": 3,
            "last_ts_recv_ns": 4,
            "missing_open_interest_families": [],
            "missing_record_families": [],
            "open_interest_record_counts": family_counts if schema == "statistics" else {},
            "option_definition_count": 9 if schema == "definition" else 0,
            "record_count": 9,
            "record_types": {"19" if schema == "definition" else "24" if schema == "statistics" else "194": 9},
            "two_sided_tcbbo_count": 9 if schema == "tcbbo" else 0,
            "two_sided_tcbbo_coverage": 1.0 if schema == "tcbbo" else None,
            "two_sided_tcbbo_coverage_pass": True if schema == "tcbbo" else None,
            "undefined_open_interest_record_count": 0,
        }
        components.append(
            {
                "file": component_path.name,
                "file_bytes": component_path.stat().st_size,
                "file_sha256": _sha(component_path),
                "requested_end_utc": window["end_utc"],
                "requested_start_utc": window["start_utc"],
                "schema": schema,
                "validation": validation,
            }
        )

    request = {
        "components": windows,
        "compression": "zstd",
        "dataset": "OPRA.PILLAR",
        "encoding": "dbn",
        "parents": list(DEFAULT_OPTION_PARENTS),
        "range_filter": "ts_recv",
        "stype_in": "parent",
        "stype_out": "instrument_id",
        "version": "marketpin-databento-historical-request-v1",
    }
    legacy_payload = {
        "version": "marketpin-databento-historical-bundle-v1",
        "bundle_sha256": "2" * 64,
    }
    legacy_path = tmp_path / "manifest.json"
    legacy_path.write_text(json.dumps(legacy_payload), encoding="utf-8")
    payload = {
        "acquisition": {
            "attested_at_utc": "2026-09-01T14:47:58Z",
            "original_completed_at_utc": "2026-09-01T13:52:31Z",
            "sdk_versions": {"databento": "test", "databento-dbn": "test"},
        },
        "components": components,
        "dataset": "OPRA.PILLAR",
        "legacy_evidence": {
            "bundle_sha256": legacy_payload["bundle_sha256"],
            "manifest_file": legacy_path.name,
            "manifest_sha256": _sha(legacy_path),
        },
        "planning": {
            "estimate_currency": "USD",
            "estimate_enforcement_scope": "client_preflight_only",
            "estimated_billable_bytes": 100,
            "estimated_cost_usd": 0.0,
            "legacy_manifest_version": legacy_payload["version"],
        },
        "provider_condition": "available",
        "provider_condition_at_attestation": "available",
        "provider_condition_last_modified_date": "2026-08-27",
        "provider_condition_last_modified_date_at_attestation": "2026-08-27",
        "request": request,
        "request_sha256": _canonical_hash(request),
        "source_kind": "databento_historical",
        "status": "complete",
        "trading_date": trading_day.isoformat(),
        "version": "marketpin-databento-historical-bundle-v2",
    }
    payload["bundle_sha256"] = _canonical_hash(_bundle_identity(payload))
    payload["provenance_sha256"] = _canonical_hash(_provenance_identity(payload))
    payload["attestation_sha256"] = _canonical_hash(
        {
            "bundle_sha256": payload["bundle_sha256"],
            "provenance_sha256": payload["provenance_sha256"],
        }
    )
    manifest_path = tmp_path / "manifest.v2.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    return manifest_path, payload


def test_verifier_accepts_complete_content_addressed_bundle(tmp_path):
    manifest_path, payload = _write_manifest(tmp_path)

    verified = verify_historical_bundle_manifest(manifest_path)

    assert verified.bundle_sha256 == payload["bundle_sha256"]
    assert verified.trading_date == date(2026, 8, 27)
    assert set(verified.components) == {"definition", "statistics", "tcbbo"}
    assert verified.manifest_sha256 == _sha(manifest_path)


def test_verifier_rejects_component_byte_tampering(tmp_path):
    manifest_path, _ = _write_manifest(tmp_path)
    (tmp_path / "fixture.tcbbo.dbn.zst").write_bytes(b"tampered")

    with pytest.raises(ValueError, match="size mismatch|hash mismatch"):
        verify_historical_bundle_manifest(manifest_path)


def test_dry_run_exposes_source_contract_and_target_without_writes(tmp_path):
    manifest_path, payload = _write_manifest(tmp_path)
    project_root = tmp_path / "project"
    project_root.mkdir()

    plan = describe_historical_import(project_root, manifest_path)

    assert plan["mode"] == "dry-run"
    assert plan["executable"] is True
    assert plan["evidence_contract_version"] == HISTORICAL_EVIDENCE_CONTRACT_VERSION
    assert plan["live_operational_counters_applicable"] is False
    assert plan["bundle"]["bundle_sha256"] == payload["bundle_sha256"]
    assert plan["catalog_exists"] is False
    assert plan["existing_sessions"] == []
    assert not (project_root / "data").exists()


def test_dry_run_checks_custom_catalog_volume_without_creating_target(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("CLOSING_TAPE_MIN_FREE_BYTES", str(8 * 1024**3))
    manifest_path, _ = _write_manifest(tmp_path)
    project_root = tmp_path / "project"
    project_root.mkdir()
    catalog_path = tmp_path / "alternate" / "catalog.sqlite"

    plan = describe_historical_import(
        project_root,
        manifest_path,
        catalog_path=catalog_path,
    )

    assert plan["catalog_path"] == str(catalog_path.resolve())
    assert plan["catalog_volume_probe_path"] == str(tmp_path.resolve())
    assert plan["catalog_volume_free_bytes"] > 0
    assert plan["minimum_catalog_reserve_bytes"] == 8 * 1024**3
    assert plan["estimated_catalog_growth_bytes"] > 0
    assert plan["required_catalog_free_bytes"] == (
        plan["minimum_catalog_reserve_bytes"]
        + plan["estimated_catalog_growth_bytes"]
        + plan["estimated_wal_headroom_bytes"]
        + plan["shared_volume_temporary_bytes"]
    )
    assert plan["predicted_catalog_volume_free_after_import_bytes"] == (
        plan["catalog_volume_free_bytes"]
        - plan["estimated_catalog_growth_bytes"]
        - plan["estimated_wal_headroom_bytes"]
    )
    assert plan["predicted_catalog_volume_free_at_peak_bytes"] == (
        plan["predicted_catalog_volume_free_after_import_bytes"]
        - plan["shared_volume_temporary_bytes"]
    )
    assert not catalog_path.parent.exists()


def test_dry_run_checkpointed_wal_catalog_does_not_create_auxiliary_files(tmp_path):
    manifest_path, _ = _write_manifest(tmp_path)
    project_root = tmp_path / "project"
    catalog_path = (
        project_root
        / "data"
        / "closing_tape"
        / "2026-08-27"
        / "closing_tape.sqlite"
    )
    catalog_path.parent.mkdir(parents=True)
    connection = sqlite3.connect(catalog_path)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(
            """
            CREATE TABLE tape_sessions (
                session_id TEXT,
                status TEXT,
                created_at_utc TEXT,
                completed_at_utc TEXT
            )
            """
        )
        connection.commit()
    finally:
        connection.close()
    for suffix in ("-wal", "-shm"):
        auxiliary_path = Path(f"{catalog_path}{suffix}")
        if auxiliary_path.exists():
            auxiliary_path.unlink()
    before = {
        path.name: (path.stat().st_size, path.stat().st_mtime_ns)
        for path in catalog_path.parent.iterdir()
    }

    plan = describe_historical_import(project_root, manifest_path)

    after = {
        path.name: (path.stat().st_size, path.stat().st_mtime_ns)
        for path in catalog_path.parent.iterdir()
    }
    assert plan["catalog_has_nonempty_wal"] is False
    assert plan["catalog_inspection_deferred"] is False
    assert before == after
    assert not Path(f"{catalog_path}-wal").exists()
    assert not Path(f"{catalog_path}-shm").exists()


def test_dry_run_reports_unknown_incremental_state_for_completed_wal_catalog(
    monkeypatch,
    tmp_path,
):
    manifest_path, _ = _write_manifest(tmp_path)
    bundle = verify_historical_bundle_manifest(manifest_path)
    project_root = tmp_path / "project"
    catalog_path = (
        project_root
        / "data"
        / "closing_tape"
        / "2026-08-27"
        / "closing_tape.sqlite"
    )
    catalog_path.parent.mkdir(parents=True)
    session_id = f"2026-08-27-historical-{bundle.bundle_sha256[:12]}"
    connection = sqlite3.connect(catalog_path)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA wal_autocheckpoint=0")
        connection.executescript(
            """
            CREATE TABLE tape_sessions (session_id TEXT, status TEXT);
            CREATE TABLE tape_feed_status (
                session_id TEXT,
                feed_name TEXT,
                status TEXT,
                source_kind TEXT,
                sha256 TEXT,
                source_manifest_path TEXT
            );
            CREATE TABLE tape_finalization_runs (
                session_id TEXT,
                feed_name TEXT,
                source_sha256 TEXT,
                evidence_contract_version TEXT,
                complete INTEGER,
                attempted_at_utc TEXT,
                run_key TEXT,
                report_json TEXT
            );
            CREATE TABLE tape_observed_minute (session_id TEXT);
            CREATE TABLE tape_inferred_minute_flow (session_id TEXT);
            CREATE TABLE tape_observed_contract_minute (session_id TEXT);
            CREATE TABLE tape_inferred_contract_minute_flow (session_id TEXT);
            CREATE TABLE tape_open_interest_observations (session_id TEXT);
            CREATE TABLE tape_instrument_definition_observations (session_id TEXT);
            """
        )
        connection.execute(
            "INSERT INTO tape_sessions VALUES (?, 'complete')",
            (session_id,),
        )
        connection.execute(
            "INSERT INTO tape_feed_status VALUES (?, 'opra_options', 'complete', ?, ?, ?)",
            (
                session_id,
                historical_import.HISTORICAL_SOURCE_KIND,
                bundle.bundle_sha256,
                str(bundle.manifest_path),
            ),
        )
        connection.execute(
            """
            INSERT INTO tape_finalization_runs
            VALUES (?, 'opra_options', ?, ?, 1, '2026-09-01T00:00:00Z',
                    'fixture', '{"feature_hash": "fixture"}')
            """,
            (
                session_id,
                bundle.bundle_sha256,
                HISTORICAL_EVIDENCE_CONTRACT_VERSION,
            ),
        )
        connection.commit()
        assert Path(f"{catalog_path}-wal").stat().st_size > 0

        plan = describe_historical_import(project_root, manifest_path)
        monkeypatch.setattr(
            historical_import,
            "require_compute_window",
            lambda *_args, **_kwargs: (),
        )

        def unexpected_disk_usage(_path):
            raise AssertionError("completed WAL import must not inspect capacity")

        monkeypatch.setattr(
            historical_import.shutil,
            "disk_usage",
            unexpected_disk_usage,
        )
        result = import_historical_bundle(project_root, manifest_path)
    finally:
        connection.close()

    assert plan["catalog_has_nonempty_wal"] is True
    assert plan["catalog_inspection_deferred"] is True
    assert plan["incremental_import_required"] is None
    assert plan["capacity_assumes_incremental_import"] is True
    assert plan["existing_import_result"] is None
    assert plan["estimated_catalog_growth_bytes"] > 0
    assert plan["estimated_wal_headroom_bytes"] > 0
    assert result.status == "already_complete"
    assert result.session_id == session_id


def test_dry_run_enforces_live_recorder_disk_reserve_for_catalog_volume(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("CLOSING_TAPE_MIN_FREE_BYTES", str(8 * 1024**3))
    manifest_path, _ = _write_manifest(tmp_path)
    project_root = tmp_path / "project"
    project_root.mkdir()
    temporary_root = Path(tempfile.gettempdir()).resolve()

    def disk_usage(path):
        resolved = Path(path).resolve()
        free = 64 * 1024**3 if resolved == temporary_root else 7 * 1024**3
        return SimpleNamespace(free=free)

    monkeypatch.setattr(historical_import.shutil, "disk_usage", disk_usage)

    plan = describe_historical_import(project_root, manifest_path)

    assert plan["executable"] is False
    assert plan["blocked_reasons"] == [
        "catalog volume has insufficient free space"
    ]
    assert plan["catalog_volume_free_bytes"] == 7 * 1024**3
    assert plan["minimum_catalog_reserve_bytes"] == 8 * 1024**3
    assert plan["required_catalog_free_bytes"] > 8 * 1024**3
    assert plan["predicted_catalog_volume_free_after_import_bytes"] < 7 * 1024**3


def test_dry_run_blocks_one_byte_below_catalog_capacity_requirement(
    monkeypatch,
    tmp_path,
):
    manifest_path, _ = _write_manifest(tmp_path)
    project_root = tmp_path / "project"
    project_root.mkdir()
    baseline = describe_historical_import(project_root, manifest_path)
    required = baseline["required_catalog_free_bytes"]
    temporary_root = Path(tempfile.gettempdir()).resolve()

    def disk_usage(path):
        resolved = Path(path).resolve()
        free = 64 * 1024**3 if resolved == temporary_root else required - 1
        return SimpleNamespace(free=free)

    monkeypatch.setattr(historical_import.shutil, "disk_usage", disk_usage)

    plan = describe_historical_import(project_root, manifest_path)

    assert plan["catalog_volume_free_bytes"] == required - 1
    assert plan["required_catalog_free_bytes"] == required
    assert plan["executable"] is False
    assert plan["blocked_reasons"] == [
        "catalog volume has insufficient free space"
    ]


def test_execute_refuses_low_space_catalog_volume_before_creating_target(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("CLOSING_TAPE_MIN_FREE_BYTES", str(8 * 1024**3))
    manifest_path, _ = _write_manifest(tmp_path)
    project_root = tmp_path / "project"
    project_root.mkdir()
    catalog_path = tmp_path / "alternate" / "catalog.sqlite"
    temporary_root = Path(tempfile.gettempdir()).resolve()

    monkeypatch.setattr(
        historical_import,
        "require_compute_window",
        lambda *_args, **_kwargs: (),
    )

    def disk_usage(path):
        resolved = Path(path).resolve()
        free = 64 * 1024**3 if resolved == temporary_root else 7 * 1024**3
        return SimpleNamespace(free=free)

    monkeypatch.setattr(historical_import.shutil, "disk_usage", disk_usage)

    with pytest.raises(RuntimeError, match="catalog-volume free space"):
        import_historical_bundle(
            project_root,
            manifest_path,
            catalog_path=catalog_path,
        )

    assert not catalog_path.parent.exists()


def test_capacity_requirement_excludes_temporary_bytes_on_separate_volume(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(historical_import, "_same_storage_volume", lambda *_args: False)

    capacity = _catalog_capacity_requirement(
        minimum_reserve_bytes=8 * 1024**3,
        estimated_working_bytes=3 * 1024**3,
        estimated_catalog_growth_bytes=2 * 1024**3,
        estimated_wal_headroom_bytes=512 * 1024**2,
        catalog_probe=tmp_path,
        temporary_probe=tmp_path,
    )

    assert capacity["catalog_and_temporary_share_volume"] is False
    assert capacity["shared_volume_temporary_bytes"] == 0
    assert capacity["required_free_bytes"] == 10 * 1024**3 + 512 * 1024**2


def test_volume_identity_failure_assumes_shared_capacity():
    class UnstatablePath:
        def stat(self):
            raise PermissionError("fixture volume identity unavailable")

    assert _same_storage_volume(UnstatablePath(), UnstatablePath()) is True


def test_inaccessible_destination_is_normalized_to_runtime_error(
    monkeypatch,
    tmp_path,
):
    blocked_path = tmp_path / "blocked"
    original_resolve = Path.resolve

    def denied_resolve(path, *args, **kwargs):
        if path == blocked_path:
            raise PermissionError("fixture destination unavailable")
        return original_resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", denied_resolve)

    with pytest.raises(RuntimeError, match="cannot inspect destination path"):
        _disk_usage_probe_path(blocked_path)


def _already_complete_result(catalog_path: Path) -> HistoricalImportResult:
    return HistoricalImportResult(
        status="already_complete",
        session_id="2026-08-27-historical-fixture",
        trading_date="2026-08-27",
        catalog_path=str(catalog_path.resolve()),
        source_sha256="a" * 64,
        manifest_sha256="b" * 64,
        feature_hash="c" * 64,
        finalization_run_key="fixture",
        observed_minute_rows=1,
        inferred_minute_rows=1,
        observed_contract_minute_rows=1,
        inferred_contract_minute_rows=1,
        open_interest_observations=1,
        definition_observations=1,
    )


def test_dry_run_already_complete_requires_zero_incremental_capacity(
    monkeypatch,
    tmp_path,
):
    manifest_path, _ = _write_manifest(tmp_path)
    project_root = tmp_path / "project"
    project_root.mkdir()
    result = _already_complete_result(project_root / "catalog.sqlite")
    monkeypatch.setattr(
        historical_import,
        "_existing_import_result",
        lambda *_args, **_kwargs: result,
    )
    monkeypatch.setattr(
        historical_import.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=1),
    )

    plan = describe_historical_import(project_root, manifest_path)

    assert plan["incremental_import_required"] is False
    assert plan["existing_import_result"]["status"] == "already_complete"
    assert plan["estimated_temporary_working_bytes"] == 0
    assert plan["estimated_catalog_growth_bytes"] == 0
    assert plan["estimated_wal_headroom_bytes"] == 0
    assert plan["shared_volume_temporary_bytes"] == 0
    assert plan["required_catalog_free_bytes"] == 0
    assert plan["blocked_reasons"] == []


def test_execute_already_complete_skips_capacity_and_catalog_writes(
    monkeypatch,
    tmp_path,
):
    manifest_path, _ = _write_manifest(tmp_path)
    bundle = verify_historical_bundle_manifest(manifest_path)
    project_root = tmp_path / "project"
    project_root.mkdir()
    result = _already_complete_result(project_root / "catalog.sqlite")
    monkeypatch.setattr(
        historical_import,
        "require_compute_window",
        lambda *_args, **_kwargs: (),
    )
    monkeypatch.setattr(
        historical_import,
        "verify_historical_bundle_manifest",
        lambda _path: bundle,
    )
    monkeypatch.setattr(
        historical_import,
        "_existing_import_result",
        lambda *_args, **_kwargs: result,
    )

    def unexpected_disk_usage(_path):
        raise AssertionError("idempotent import must not inspect capacity")

    monkeypatch.setattr(historical_import.shutil, "disk_usage", unexpected_disk_usage)

    actual = import_historical_bundle(project_root, manifest_path)

    assert actual == result
    assert not (project_root / "data").exists()


def test_execute_checks_compute_window_before_reading_manifest(monkeypatch, tmp_path):
    def blocked(*_args, **_kwargs):
        raise RuntimeError("capture remains active")

    monkeypatch.setattr(historical_import, "require_compute_window", blocked)

    with pytest.raises(RuntimeError, match="capture remains active"):
        import_historical_bundle(tmp_path, tmp_path / "missing-manifest.json")


def test_mapping_identity_is_order_stable_and_point_in_time():
    mappings = {
        "SPXW  260827C07000000": [
            {"symbol": "101", "start_date": "2026-08-27", "end_date": "2026-08-28"}
        ],
        "NDXP  260827P25000000": [
            {"symbol": "202", "start_date": "2026-08-27", "end_date": "2026-08-28"}
        ],
    }

    first_hash, first_active = _mapping_summary(mappings, date(2026, 8, 27))
    second_hash, second_active = _mapping_summary(
        dict(reversed(list(mappings.items()))), date(2026, 8, 27)
    )

    assert first_hash == second_hash
    assert first_active == second_active == {101: "SPXW", 202: "NDXP"}


def test_failed_import_records_incomplete_source_audit(monkeypatch, tmp_path):
    manifest_path, _ = _write_manifest(tmp_path)
    bundle = verify_historical_bundle_manifest(manifest_path)
    project_root = tmp_path / "project"
    project_root.mkdir()

    monkeypatch.setattr(
        historical_import,
        "require_compute_window",
        lambda *_args, **_kwargs: (),
    )
    monkeypatch.setattr(
        historical_import,
        "verify_historical_bundle_manifest",
        lambda _path: bundle,
    )
    monkeypatch.setattr(
        historical_import,
        "_materialize_component",
        lambda component, destination, **_kwargs: (
            destination / f"{component.schema}.dbn",
            object(),
        ),
    )

    def fail_definition_replay(*_args, **_kwargs):
        raise ValueError("fixture replay failure")

    monkeypatch.setattr(
        historical_import,
        "replay_instrument_definitions",
        fail_definition_replay,
    )

    with pytest.raises(ValueError, match="fixture replay failure"):
        import_historical_bundle(project_root, manifest_path)

    catalog_path = (
        project_root / "data" / "closing_tape" / "2026-08-27" / "closing_tape.sqlite"
    )
    with sqlite3.connect(catalog_path) as connection:
        feed = connection.execute(
            "SELECT status, complete, source_kind FROM tape_feed_status"
        ).fetchone()
        audit = connection.execute(
            "SELECT complete, issues_json FROM tape_finalization_runs"
        ).fetchone()
    assert feed == ("incomplete", 0, "databento_historical")
    assert audit[0] == 0
    assert "fixture replay failure" in audit[1]
