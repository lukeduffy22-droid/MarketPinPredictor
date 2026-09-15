import json
import sqlite3
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.closing_tape import surface_artifact as artifact_module
from backend.closing_tape.catalog_discovery import CatalogDiscovery
from backend.closing_tape.pipeline import ResearchSurfaceReport
from backend.closing_tape.surface import build_contract_surface_features
from backend.closing_tape.surface_artifact import (
    ReplayVerifiedResearchSurfaceArtifact,
    load_research_surface_artifact,
    load_replay_verified_research_surface_artifact,
    require_replay_verified_surface_identity,
    validate_retained_surface_promotion_evidence,
    write_research_surface_artifact,
)
from tests.test_closing_tape_surface import _rows


def _surface_and_report():
    surface = build_contract_surface_features(_rows())
    report = ResearchSurfaceReport(
        catalog_count=1,
        eligible_contract_rows=2,
        surface_rows=len(surface),
        sessions=1,
        feature_schema_hash=str(surface.iloc[0]["feature_schema_hash"]),
        model_feature_contract_hash="unused-by-artifact-writer",
        model_feature_columns=(),
        source_sha256s=("a" * 64,),
        family_coverage=(),
    )
    return surface, report


def test_surface_artifact_round_trip_rehashes_provenance_and_features(tmp_path):
    surface, report = _surface_and_report()

    written = write_research_surface_artifact(
        surface,
        artifact_dir=tmp_path,
        report=report,
        max_price_age_seconds=90.0,
    )
    loaded = load_research_surface_artifact(written.manifest_path)

    assert written.parquet_path.name == f"{written.artifact_sha256}.parquet"
    assert written.manifest_path.name == f"{written.artifact_sha256}.manifest.json"
    assert loaded.artifact_sha256 == written.artifact_sha256
    assert loaded.manifest["source_sha256s"] == ["a" * 64]
    pd.testing.assert_frame_equal(
        loaded.frame.reset_index(drop=True),
        surface.reset_index(drop=True),
        check_dtype=False,
    )


def test_replay_verified_surface_rejects_fabricated_authority():
    fabricated = object.__new__(ReplayVerifiedResearchSurfaceArtifact)

    with pytest.raises(ValueError, match="replay-verified research surface"):
        require_replay_verified_surface_identity(fabricated)


def test_replay_verified_surface_rejects_current_frame_mutation(
    tmp_path,
    monkeypatch,
):
    surface, report = _surface_and_report()
    written = write_research_surface_artifact(
        surface,
        artifact_dir=tmp_path / "artifacts",
        report=report,
        max_price_age_seconds=90.0,
    )
    _stub_replay_sources(monkeypatch, tmp_path, surface, report)
    loaded = load_replay_verified_research_surface_artifact(
        written.manifest_path,
        project_root=tmp_path,
        market_db_path=tmp_path / "data" / "market_data.db",
    )
    loaded.frame.loc[loaded.frame.index[0], "reference_price"] += 1.0

    with pytest.raises(ValueError, match="current frame changed"):
        require_replay_verified_surface_identity(loaded)


def test_replay_verified_surface_rehashes_artifact_at_use(
    tmp_path,
    monkeypatch,
):
    surface, report = _surface_and_report()
    written = write_research_surface_artifact(
        surface,
        artifact_dir=tmp_path / "artifacts",
        report=report,
        max_price_age_seconds=90.0,
    )
    _stub_replay_sources(monkeypatch, tmp_path, surface, report)
    loaded = load_replay_verified_research_surface_artifact(
        written.manifest_path,
        project_root=tmp_path,
        market_db_path=tmp_path / "data" / "market_data.db",
    )
    with loaded.parquet_path.open("ab") as stream:
        stream.write(b"tamper-after-replay-verification")

    with pytest.raises(ValueError, match="byte count"):
        require_replay_verified_surface_identity(loaded)


def test_surface_artifact_rejects_tampered_parquet_bytes(tmp_path):
    surface, report = _surface_and_report()
    written = write_research_surface_artifact(
        surface,
        artifact_dir=tmp_path,
        report=report,
        max_price_age_seconds=90.0,
    )
    with written.parquet_path.open("ab") as stream:
        stream.write(b"tamper")

    with pytest.raises(ValueError, match="byte count"):
        load_research_surface_artifact(written.manifest_path)


def test_surface_artifact_parses_the_same_parquet_snapshot_it_hashes(
    tmp_path, monkeypatch
):
    surface, report = _surface_and_report()
    written = write_research_surface_artifact(
        surface,
        artifact_dir=tmp_path,
        report=report,
        max_price_age_seconds=90.0,
    )
    real_read_parquet = artifact_module.pd.read_parquet
    observed = {}

    def read_parquet(source, **kwargs):
        observed["source_type"] = type(source).__name__
        assert not isinstance(source, (str, bytes))
        return real_read_parquet(source, **kwargs)

    monkeypatch.setattr(artifact_module.pd, "read_parquet", read_parquet)

    loaded = load_research_surface_artifact(written.manifest_path)

    assert loaded.artifact_sha256 == written.artifact_sha256
    assert observed["source_type"] == "BytesIO"


def test_surface_artifact_rejects_stale_model_feature_contract(tmp_path):
    surface, report = _surface_and_report()
    written = write_research_surface_artifact(
        surface,
        artifact_dir=tmp_path,
        report=report,
        max_price_age_seconds=90.0,
    )
    payload = json.loads(written.manifest_path.read_text(encoding="utf-8"))
    payload["model_feature_contract_hash"] = "f" * 64
    written.manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="model feature contract is not current"):
        load_research_surface_artifact(written.manifest_path)


def test_surface_artifact_rejects_duplicate_feature_identity(tmp_path):
    surface, report = _surface_and_report()
    duplicate = pd.concat([surface, surface], ignore_index=True)

    with pytest.raises(ValueError, match="duplicate session/family/minute"):
        write_research_surface_artifact(
            duplicate,
            artifact_dir=tmp_path,
            report=report,
            max_price_age_seconds=90.0,
        )


def _stub_replay_sources(monkeypatch, tmp_path, replayed, report):
    catalog = tmp_path / "catalogs" / "2026-08-25" / "closing_tape.sqlite"
    selection = CatalogDiscovery(
        catalog_roots=(str(catalog.parents[1]),),
        catalog_paths=(catalog,),
        issues=(),
        resolution_id="c" * 64,
    )
    monkeypatch.setattr(
        artifact_module,
        "select_closing_tape_catalogs",
        lambda *_args, **_kwargs: selection,
    )
    captured = {}

    def verify(paths, *, claims):
        captured["verified_paths"] = tuple(paths)
        captured["claims"] = claims
        return (
            (catalog,),
            (
                {
                    "trading_date": claims[0]["trading_date"],
                    "session_id": claims[0]["session_id"],
                    "source_sha256": claims[0]["source_sha256"],
                    "source_kind": "databento_live",
                },
            ),
        )

    monkeypatch.setattr(
        artifact_module,
        "_resolve_and_verify_selected_sessions",
        verify,
    )

    def rebuild(paths, market_db, *, max_price_age_seconds):
        captured["replay_paths"] = tuple(paths)
        captured["market_db"] = market_db
        captured["max_price_age_seconds"] = max_price_age_seconds
        return replayed.copy(), report

    monkeypatch.setattr(
        artifact_module,
        "build_research_surface_dataset",
        rebuild,
    )
    return catalog, captured


def _live_catalog_fixture(tmp_path, *, source_sha256="a" * 64):
    day = tmp_path / "catalogs" / "2026-08-25"
    day.mkdir(parents=True)
    catalog = day / "closing_tape.sqlite"
    source = day / "opra_options.dbn"
    source.write_bytes(b"retained DBN fixture")
    integrity = {
        "sha256": source_sha256,
        "file_bytes": source.stat().st_size,
    }
    with sqlite3.connect(catalog) as connection:
        connection.executescript(
            """
            CREATE TABLE tape_sessions (
                session_id TEXT PRIMARY KEY,
                trading_date TEXT NOT NULL,
                status TEXT NOT NULL
            );
            CREATE TABLE tape_feed_status (
                session_id TEXT NOT NULL,
                feed_name TEXT NOT NULL,
                dataset TEXT NOT NULL,
                dbn_path TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                evidence_contract_version TEXT NOT NULL,
                operational_counters_applicable INTEGER NOT NULL,
                source_manifest_path TEXT,
                source_components_json TEXT NOT NULL,
                status TEXT NOT NULL,
                complete INTEGER NOT NULL,
                sha256 TEXT,
                reconnect_count INTEGER NOT NULL,
                slow_reader_warnings INTEGER NOT NULL,
                provider_error_count INTEGER NOT NULL,
                unmapped_trade_records INTEGER NOT NULL,
                expected_subscription_acks INTEGER NOT NULL,
                records_seen INTEGER NOT NULL,
                trade_records INTEGER NOT NULL,
                tcbbo_records INTEGER NOT NULL,
                tcbbo_timestamped_records INTEGER NOT NULL,
                tcbbo_valid_nbbo_records INTEGER NOT NULL,
                definition_records INTEGER NOT NULL,
                statistics_records INTEGER NOT NULL,
                subscription_acks INTEGER NOT NULL,
                replay_completed INTEGER NOT NULL,
                file_bytes INTEGER NOT NULL,
                PRIMARY KEY (session_id, feed_name)
            );
            CREATE TABLE tape_finalization_runs (
                session_id TEXT NOT NULL,
                feed_name TEXT NOT NULL,
                source_sha256 TEXT NOT NULL,
                evidence_contract_version TEXT NOT NULL,
                attempted_at_utc TEXT NOT NULL,
                complete INTEGER NOT NULL,
                report_json TEXT NOT NULL
            );
            """
        )
        connection.execute(
            "INSERT INTO tape_sessions VALUES (?, ?, ?)",
            ("s1", "2026-08-25", "complete"),
        )
        connection.execute(
            """
            INSERT INTO tape_feed_status VALUES (
                ?, ?, ?, ?, ?, ?, 1, NULL, '{}', 'complete', 1, ?,
                0, 0, 0, 0,
                3, 10, 8, 6, 6, 6, 1, 1, 3, 3, ?
            )
            """,
            (
                "s1",
                "opra_options",
                "OPRA.PILLAR",
                str(source),
                "databento_live",
                "tcbbo-observed-v7",
                source_sha256,
                source.stat().st_size,
            ),
        )
        connection.execute(
            "INSERT INTO tape_finalization_runs VALUES (?, ?, ?, ?, ?, 1, ?)",
            (
                "s1",
                "opra_options",
                source_sha256,
                "tcbbo-observed-v7",
                "2026-08-25T21:00:00+00:00",
                json.dumps(
                    {
                        "source_sha256": source_sha256,
                        "complete": True,
                        "integrity": integrity,
                    }
                ),
            ),
        )
    return catalog, source


def _live_integrity(source, *, source_sha256="a" * 64):
    return SimpleNamespace(
        local_file_intact=True,
        incomplete_reasons=(),
        sha256=source_sha256,
        file_bytes=source.stat().st_size,
        records_seen=10,
        trade_records=8,
        tcbbo_records=6,
        tcbbo_timestamped_records=6,
        tcbbo_valid_nbbo_records=6,
        definition_records=1,
        statistics_records=1,
        subscription_acks=3,
        replay_completed=3,
        last_trade_event_ns=None,
    )


def test_replay_verified_surface_requires_source_verification_and_exact_rebuild(
    tmp_path,
    monkeypatch,
):
    surface, report = _surface_and_report()
    written = write_research_surface_artifact(
        surface,
        artifact_dir=tmp_path / "artifacts",
        report=report,
        max_price_age_seconds=37.5,
    )
    catalog, captured = _stub_replay_sources(
        monkeypatch, tmp_path, surface, report
    )

    loaded = load_replay_verified_research_surface_artifact(
        written.manifest_path,
        project_root=tmp_path,
        market_db_path=tmp_path / "data" / "market_data.db",
    )

    assert loaded.replay_verification["semantic_equality_verified"] is True
    assert loaded.replay_verification["max_price_age_seconds"] == 37.5
    assert loaded.replay_verification["selected_catalog_paths"] == [str(catalog)]
    assert captured["verified_paths"] == (catalog,)
    assert captured["replay_paths"] == (catalog,)
    assert captured["max_price_age_seconds"] == 37.5
    pd.testing.assert_frame_equal(
        loaded.frame.reset_index(drop=True),
        surface.reset_index(drop=True),
        check_dtype=False,
    )


def test_replay_verified_surface_rejects_fabricated_feature_values(
    tmp_path,
    monkeypatch,
):
    actual, report = _surface_and_report()
    fabricated = actual.copy()
    fabricated.loc[
        fabricated.index[0], "inferred_net_at_ask_minus_bid_notional_ratio"
    ] += 0.25
    written = write_research_surface_artifact(
        fabricated,
        artifact_dir=tmp_path / "artifacts",
        report=report,
        max_price_age_seconds=90.0,
    )
    _stub_replay_sources(monkeypatch, tmp_path, actual, report)

    with pytest.raises(ValueError, match="not semantically identical"):
        load_replay_verified_research_surface_artifact(
            written.manifest_path,
            project_root=tmp_path,
            market_db_path=tmp_path / "data" / "market_data.db",
        )


def test_retained_surface_receipt_cannot_self_attest_fabricated_semantics(
    tmp_path, monkeypatch
):
    actual, report = _surface_and_report()
    original = write_research_surface_artifact(
        actual,
        artifact_dir=tmp_path / "original",
        report=report,
        max_price_age_seconds=90.0,
    )
    _stub_replay_sources(monkeypatch, tmp_path, actual, report)
    verified = load_replay_verified_research_surface_artifact(
        original.manifest_path,
        project_root=tmp_path,
        market_db_path=tmp_path / "data" / "market_data.db",
    )
    fabricated = actual.copy()
    fabricated.loc[
        fabricated.index[0], "inferred_net_at_ask_minus_bid_notional_ratio"
    ] += 0.25
    retained = write_research_surface_artifact(
        fabricated,
        artifact_dir=tmp_path / "models" / "promotion_evidence" / "surfaces",
        report=report,
        max_price_age_seconds=90.0,
    )
    receipt = dict(verified.replay_verification)
    receipt["surface_artifact_sha256"] = retained.artifact_sha256
    receipt_bytes = artifact_module._strict_json_bytes(receipt)
    receipt_hash = artifact_module.canonical_replay_receipt_sha256(receipt)
    receipt_path = (
        tmp_path
        / "models"
        / "promotion_evidence"
        / "surface_replays"
        / f"{receipt_hash}.json"
    )
    receipt_path.parent.mkdir(parents=True)
    receipt_path.write_bytes(receipt_bytes)

    with pytest.raises(ValueError, match="not semantically identical"):
        validate_retained_surface_promotion_evidence(
            tmp_path,
            surface_artifact_sha256=retained.artifact_sha256,
            surface_replay_receipt_sha256=receipt_hash,
            source_sha256s=("a" * 64,),
            market_db_path=tmp_path / "data" / "market_data.db",
        )


def test_retained_surface_receipt_must_resolve_a_real_finalized_catalog(
    tmp_path, monkeypatch
):
    surface, report = _surface_and_report()
    retained = write_research_surface_artifact(
        surface,
        artifact_dir=tmp_path / "models" / "promotion_evidence" / "surfaces",
        report=report,
        max_price_age_seconds=90.0,
    )
    claim = artifact_module._selected_session_claims(surface)[0]
    catalog = (
        tmp_path
        / "data"
        / "closing_tape"
        / claim["trading_date"]
        / "closing_tape.sqlite"
    )
    catalog.parent.mkdir(parents=True)
    with sqlite3.connect(catalog):
        pass
    source = catalog.parent / "fabricated.dbn"
    source.write_bytes(b"fabricated source")
    receipt = {
        "contract_version": artifact_module.SURFACE_REPLAY_VERIFICATION_CONTRACT,
        "surface_artifact_sha256": retained.artifact_sha256,
        "semantic_equality_verified": True,
        "max_price_age_seconds": 90.0,
        "selected_sessions": [dict(claim)],
        "selected_catalog_paths": [str(catalog.resolve())],
        "source_evidence": [
            {
                **dict(claim),
                "source_kind": "databento_live",
                "source_path": str(source.resolve()),
                "source_bytes": source.stat().st_size,
                "records_seen": 1,
                "tcbbo_records": 1,
                "catalog_path": str(catalog.resolve()),
            }
        ],
        "replay_surface_report": report.to_dict(),
    }
    receipt_hash = artifact_module.canonical_replay_receipt_sha256(receipt)
    receipt_path = (
        tmp_path
        / "models"
        / "promotion_evidence"
        / "surface_replays"
        / f"{receipt_hash}.json"
    )
    receipt_path.parent.mkdir(parents=True)
    receipt_path.write_bytes(artifact_module._strict_json_bytes(receipt))
    monkeypatch.setattr(
        artifact_module,
        "select_closing_tape_catalogs",
        lambda *_args, **_kwargs: CatalogDiscovery(
            catalog_roots=(str(catalog.parents[1]),),
            catalog_paths=(catalog.resolve(),),
            issues=(),
            resolution_id="c" * 64,
        ),
    )

    with pytest.raises(ValueError, match="missing replay evidence tables"):
        validate_retained_surface_promotion_evidence(
            tmp_path,
            surface_artifact_sha256=retained.artifact_sha256,
            surface_replay_receipt_sha256=receipt_hash,
            source_sha256s=("a" * 64,),
            market_db_path=tmp_path / "data" / "market_data.db",
        )


def test_replay_verified_surface_rejects_unresolved_artifact_session(
    tmp_path,
    monkeypatch,
):
    surface, report = _surface_and_report()
    written = write_research_surface_artifact(
        surface,
        artifact_dir=tmp_path / "artifacts",
        report=report,
        max_price_age_seconds=90.0,
    )
    monkeypatch.setattr(
        artifact_module,
        "select_closing_tape_catalogs",
        lambda *_args, **_kwargs: CatalogDiscovery(
            catalog_roots=(),
            catalog_paths=(),
            issues=(),
            resolution_id="c" * 64,
        ),
    )

    with pytest.raises(ValueError, match="exactly one selected catalog"):
        load_replay_verified_research_surface_artifact(
            written.manifest_path,
            project_root=tmp_path,
            market_db_path=tmp_path / "data" / "market_data.db",
        )


def test_replay_verified_surface_rehashes_selected_live_source(
    tmp_path,
    monkeypatch,
):
    surface, report = _surface_and_report()
    written = write_research_surface_artifact(
        surface,
        artifact_dir=tmp_path / "artifacts",
        report=report,
        max_price_age_seconds=90.0,
    )
    catalog, source = _live_catalog_fixture(tmp_path)
    selection = CatalogDiscovery(
        catalog_roots=(str(catalog.parents[1]),),
        catalog_paths=(catalog,),
        issues=(),
        resolution_id="c" * 64,
    )
    monkeypatch.setattr(
        artifact_module,
        "select_closing_tape_catalogs",
        lambda *_args, **_kwargs: selection,
    )
    monkeypatch.setattr(
        artifact_module,
        "inspect_dbn",
        lambda *_args, **_kwargs: _live_integrity(source),
    )
    monkeypatch.setattr(
        artifact_module,
        "build_research_surface_dataset",
        lambda *_args, **_kwargs: (surface.copy(), report),
    )

    loaded = load_replay_verified_research_surface_artifact(
        written.manifest_path,
        project_root=tmp_path,
        market_db_path=tmp_path / "data" / "market_data.db",
    )

    evidence = loaded.replay_verification["source_evidence"][0]
    assert evidence["source_path"] == str(source.resolve())
    assert evidence["source_sha256"] == "a" * 64
    assert evidence["tcbbo_records"] == 6


def test_replay_verified_surface_rejects_live_bytes_with_different_hash(
    tmp_path,
    monkeypatch,
):
    surface, report = _surface_and_report()
    written = write_research_surface_artifact(
        surface,
        artifact_dir=tmp_path / "artifacts",
        report=report,
        max_price_age_seconds=90.0,
    )
    catalog, source = _live_catalog_fixture(tmp_path)
    monkeypatch.setattr(
        artifact_module,
        "select_closing_tape_catalogs",
        lambda *_args, **_kwargs: CatalogDiscovery(
            catalog_roots=(str(catalog.parents[1]),),
            catalog_paths=(catalog,),
            issues=(),
            resolution_id="c" * 64,
        ),
    )
    monkeypatch.setattr(
        artifact_module,
        "inspect_dbn",
        lambda *_args, **_kwargs: _live_integrity(
            source, source_sha256="b" * 64
        ),
    )

    with pytest.raises(ValueError, match="live DBN bytes do not match"):
        load_replay_verified_research_surface_artifact(
            written.manifest_path,
            project_root=tmp_path,
            market_db_path=tmp_path / "data" / "market_data.db",
        )


def test_replay_verified_surface_binds_catalog_source_hash_before_decode(
    tmp_path,
    monkeypatch,
):
    surface, report = _surface_and_report()
    written = write_research_surface_artifact(
        surface,
        artifact_dir=tmp_path / "artifacts",
        report=report,
        max_price_age_seconds=90.0,
    )
    catalog, _source = _live_catalog_fixture(
        tmp_path, source_sha256="b" * 64
    )
    monkeypatch.setattr(
        artifact_module,
        "select_closing_tape_catalogs",
        lambda *_args, **_kwargs: CatalogDiscovery(
            catalog_roots=(str(catalog.parents[1]),),
            catalog_paths=(catalog,),
            issues=(),
            resolution_id="c" * 64,
        ),
    )

    def forbidden(*_args, **_kwargs):
        raise AssertionError("mismatched catalog identity must fail before DBN decode")

    monkeypatch.setattr(artifact_module, "inspect_dbn", forbidden)
    with pytest.raises(ValueError, match="source claim does not match"):
        load_replay_verified_research_surface_artifact(
            written.manifest_path,
            project_root=tmp_path,
            market_db_path=tmp_path / "data" / "market_data.db",
        )


def test_replay_verified_surface_fails_closed_when_live_source_is_missing(
    tmp_path,
    monkeypatch,
):
    surface, report = _surface_and_report()
    written = write_research_surface_artifact(
        surface,
        artifact_dir=tmp_path / "artifacts",
        report=report,
        max_price_age_seconds=90.0,
    )
    catalog, source = _live_catalog_fixture(tmp_path)
    source.unlink()
    monkeypatch.setattr(
        artifact_module,
        "select_closing_tape_catalogs",
        lambda *_args, **_kwargs: CatalogDiscovery(
            catalog_roots=(str(catalog.parents[1]),),
            catalog_paths=(catalog,),
            issues=(),
            resolution_id="c" * 64,
        ),
    )

    with pytest.raises(FileNotFoundError, match="opra_options.dbn"):
        load_replay_verified_research_surface_artifact(
            written.manifest_path,
            project_root=tmp_path,
            market_db_path=tmp_path / "data" / "market_data.db",
        )


def test_historical_source_verification_fails_closed_when_manifest_is_missing(
    tmp_path,
):
    missing = tmp_path / "2026-08-25" / "historical.manifest.json"
    with pytest.raises(FileNotFoundError, match="historical.manifest.json"):
        artifact_module._verify_historical_source(
            claim={
                "trading_date": "2026-08-25",
                "session_id": "historical-s1",
                "feed_name": "opra_options",
                "source_sha256": "a" * 64,
            },
            feed={
                "dbn_path": str(missing),
                "source_manifest_path": str(missing),
                "source_components_json": "{}",
            },
            finalization_report={
                "source_sha256": "a" * 64,
                "manifest_sha256": "b" * 64,
            },
        )
