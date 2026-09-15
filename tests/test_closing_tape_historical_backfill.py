from __future__ import annotations

import json
import hashlib
import math
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.closing_tape.historical_backfill import (
    HistoricalDayLock,
    HistoricalDayPlan,
    HistoricalSchemaPlan,
    discover_existing_bundle,
    download_historical_day,
    plan_historical_day,
    request_windows,
    validate_dbn_component,
    verify_bundle_manifest,
    write_v2_attestation_for_legacy_bundle,
)


UTC = timezone.utc
PARENTS = (
    "SPX.OPT",
    "SPXW.OPT",
    "NDX.OPT",
    "NDXP.OPT",
    "RUT.OPT",
    "RUTW.OPT",
    "VIX.OPT",
    "VIXW.OPT",
    "SPY.OPT",
)


class _Metadata:
    def __init__(self, *, condition: str = "available", cost: float = 0.0):
        self.condition = condition
        self.cost = cost
        self.cost_calls: list[dict[str, object]] = []

    def get_dataset_condition(self, **_: object):
        return [
            {
                "date": "2026-08-27",
                "condition": self.condition,
                "last_modified_date": "2026-08-27",
            }
        ]

    def get_dataset_range(self, **_: object):
        return {
            "schema": {
                schema: {
                    "start": "2023-03-28T00:00:00Z",
                    "end": "2026-09-01T00:00:00Z",
                }
                for schema in ("tcbbo", "statistics", "definition")
            }
        }

    def get_cost(self, **kwargs: object):
        self.cost_calls.append(kwargs)
        return self.cost

    def get_billable_size(self, **kwargs: object):
        return {"tcbbo": 230, "statistics": 130, "definition": 25}[
            str(kwargs["schema"])
        ]


class _Timeseries:
    def __init__(self, *, fail_once_schema: str | None = None):
        self.calls: list[dict[str, object]] = []
        self.fail_once_schema = fail_once_schema

    def get_range(self, **kwargs: object):
        self.calls.append(kwargs)
        schema = str(kwargs["schema"])
        if schema == self.fail_once_schema:
            self.fail_once_schema = None
            raise RuntimeError("simulated interruption")
        path = Path(str(kwargs["path"]))
        path.write_bytes(f"test-{schema}".encode())
        return object()


class _Client:
    def __init__(
        self,
        *,
        condition: str = "available",
        cost: float = 0.0,
        fail_once_schema: str | None = None,
    ):
        self.metadata = _Metadata(condition=condition, cost=cost)
        self.timeseries = _Timeseries(fail_once_schema=fail_once_schema)


def _plan() -> HistoricalDayPlan:
    return HistoricalDayPlan(
        trading_date="2026-08-27",
        provider_condition="available",
        provider_condition_last_modified_date="2026-08-27",
        parents=PARENTS,
        schemas=(
            HistoricalSchemaPlan(
                "tcbbo",
                "2026-08-27T13:30:00Z",
                "2026-08-27T20:20:00Z",
                0.0,
                230,
            ),
            HistoricalSchemaPlan(
                "statistics",
                "2026-08-27T00:00:00Z",
                "2026-08-28T00:00:00Z",
                0.0,
                130,
            ),
            HistoricalSchemaPlan(
                "definition",
                "2026-08-27T00:00:00Z",
                "2026-08-28T00:00:00Z",
                0.0,
                25,
            ),
        ),
    )


def _fake_validation(
    path: Path, schema_plan: HistoricalSchemaPlan, parents: tuple[str, ...]
):
    return {
        "dbn_dataset": "OPRA.PILLAR",
        "dbn_schema": schema_plan.schema,
        "dbn_start_utc": schema_plan.start_utc,
        "dbn_end_utc": schema_plan.end_utc,
        "dbn_stype_in": "parent",
        "dbn_stype_out": "instrument_id",
        "dbn_symbols": list(parents),
        "dbn_mapping_count": 9,
        "dbn_mapping_sha256": "same-mapping-hash",
        "record_count": path.stat().st_size,
    }


def _canonical_hash(payload: object) -> str:
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def test_request_windows_match_live_capture_boundary():
    windows = {
        schema: (start, end)
        for schema, start, end in request_windows(date(2026, 8, 27))
    }
    assert windows["tcbbo"][0].isoformat() == "2026-08-27T13:30:00+00:00"
    assert windows["tcbbo"][1].isoformat() == "2026-08-27T20:20:00+00:00"
    assert windows["statistics"][0].isoformat() == "2026-08-27T00:00:00+00:00"
    assert windows["definition"][1].isoformat() == "2026-08-28T00:00:00+00:00"


def test_calendar_fails_closed_outside_verified_range_and_on_holiday():
    with pytest.raises(ValueError, match="verified only"):
        request_windows(date(2023, 11, 24))
    with pytest.raises(ValueError, match="not a configured"):
        request_windows(date(2026, 9, 7))


def test_calendar_handles_early_closes_and_dst():
    summer = {s: (a, b) for s, a, b in request_windows(date(2026, 7, 2))}
    summer_early = {s: (a, b) for s, a, b in request_windows(date(2025, 7, 3))}
    winter = {s: (a, b) for s, a, b in request_windows(date(2025, 11, 28))}
    assert summer["tcbbo"][0].isoformat() == "2026-07-02T13:30:00+00:00"
    assert summer["tcbbo"][1].isoformat() == "2026-07-02T20:20:00+00:00"
    assert summer_early["tcbbo"][1].isoformat() == "2025-07-03T17:20:00+00:00"
    assert winter["tcbbo"][0].isoformat() == "2025-11-28T14:30:00+00:00"
    assert winter["tcbbo"][1].isoformat() == "2025-11-28T18:20:00+00:00"


def test_plan_uses_metadata_only_and_preserves_explicit_parent_scope():
    client = _Client()
    plan = plan_historical_day(client, trading_day=date(2026, 8, 27), parents=PARENTS)
    assert plan.executable
    assert plan.estimated_cost_usd == 0.0
    assert plan.estimated_billable_bytes == 385
    assert not client.timeseries.calls
    assert all(call["symbols"] == list(PARENTS) for call in client.metadata.cost_calls)
    assert plan.request["stype_out"] == "instrument_id"
    assert len(plan.request_sha256) == 64


def test_plan_fails_closed_on_non_available_provider_condition():
    plan = plan_historical_day(
        _Client(condition="degraded"), trading_day=date(2026, 8, 27), parents=PARENTS
    )
    assert not plan.executable
    assert "provider condition is degraded" in plan.blocked_reasons


def test_plan_fails_closed_when_provider_revision_is_missing():
    client = _Client()
    client.metadata.get_dataset_condition = lambda **_: [
        {"date": "2026-08-27", "condition": "available"}
    ]
    plan = plan_historical_day(
        client, trading_day=date(2026, 8, 27), parents=PARENTS
    )
    assert not plan.executable
    assert "provider condition revision date is missing" in plan.blocked_reasons


def test_plan_rejects_noncanonical_parent_scope_before_metadata_calls():
    client = _Client()
    with pytest.raises(ValueError, match="canonical nine"):
        plan_historical_day(
            client,
            trading_day=date(2026, 8, 27),
            parents=PARENTS[:-1],
        )
    assert not client.metadata.cost_calls


@pytest.mark.parametrize("bad_cost", [math.nan, math.inf, -1.0])
def test_plan_fails_closed_on_invalid_provider_estimate(bad_cost: float):
    plan = plan_historical_day(
        _Client(cost=bad_cost), trading_day=date(2026, 8, 27), parents=PARENTS
    )
    assert not plan.executable
    assert any("finite nonnegative" in reason for reason in plan.blocked_reasons)


def test_download_enforces_byte_ceiling_before_any_request(tmp_path: Path):
    client = _Client()
    with pytest.raises(ValueError, match="preflight ceiling"):
        download_historical_day(
            client,
            plan=_plan(),
            output_root=tmp_path,
            max_cost_usd=0.0,
            max_billable_bytes=384,
            validate_component=_fake_validation,
        )
    assert not client.timeseries.calls
    assert not any(tmp_path.iterdir())


def test_download_requires_exact_reviewed_plan_hash(tmp_path: Path):
    client = _Client()
    with pytest.raises(ValueError, match="exact plan_sha256"):
        download_historical_day(
            client,
            plan=_plan(),
            output_root=tmp_path,
            max_cost_usd=0,
            max_billable_bytes=385,
            approved_plan_sha256="wrong",
            validate_component=_fake_validation,
        )
    assert not client.timeseries.calls


@pytest.mark.parametrize("bad_cap", [math.nan, math.inf, -1.0])
def test_download_rejects_invalid_cost_ceiling_before_any_request(
    tmp_path: Path, bad_cap: float
):
    client = _Client()
    with pytest.raises(ValueError, match="finite nonnegative"):
        download_historical_day(
            client,
            plan=_plan(),
            output_root=tmp_path,
            max_cost_usd=bad_cap,
            max_billable_bytes=385,
            validate_component=_fake_validation,
        )
    assert not client.timeseries.calls


def test_download_is_hash_bound_and_idempotent(tmp_path: Path):
    client = _Client()
    first = download_historical_day(
        client,
        plan=_plan(),
        output_root=tmp_path,
        max_cost_usd=0.0,
        max_billable_bytes=385,
        approved_plan_sha256=_plan().plan_sha256,
        validate_component=_fake_validation,
        observed_at=datetime(2026, 8, 28, tzinfo=UTC),
    )
    assert len(client.timeseries.calls) == 3
    assert all(call["stype_out"] == "instrument_id" for call in client.timeseries.calls)
    manifest = tmp_path / "2026-08-27" / "manifest.json"
    assert (
        verify_bundle_manifest(manifest, validate_component=_fake_validation)[
            "bundle_sha256"
        ]
        == first["bundle_sha256"]
    )
    assert (
        discover_existing_bundle(
            tmp_path,
            date(2026, 8, 27),
            expected_plan=_plan(),
            validate_component=_fake_validation,
        )
        == str(manifest.resolve())
    )

    second = download_historical_day(
        client,
        plan=replace(_plan(), existing_bundle=str(manifest)),
        output_root=tmp_path,
        max_cost_usd=0.0,
        max_billable_bytes=385,
        validate_component=_fake_validation,
    )
    assert second["bundle_sha256"] == first["bundle_sha256"]
    assert len(client.timeseries.calls) == 3


def test_existing_manifest_must_match_requested_plan(tmp_path: Path):
    client = _Client()
    download_historical_day(
        client,
        plan=_plan(),
        output_root=tmp_path,
        max_cost_usd=0,
        max_billable_bytes=385,
        approved_plan_sha256=_plan().plan_sha256,
        validate_component=_fake_validation,
    )
    changed = replace(_plan(), parents=PARENTS[:-1])
    with pytest.raises(ValueError, match="does not match"):
        discover_existing_bundle(
            tmp_path,
            date(2026, 8, 27),
            expected_plan=changed,
            validate_component=_fake_validation,
        )


def test_interrupted_download_reuses_verified_orphan_components(tmp_path: Path):
    client = _Client(fail_once_schema="statistics")
    with pytest.raises(RuntimeError, match="simulated interruption"):
        download_historical_day(
            client,
            plan=_plan(),
            output_root=tmp_path,
            max_cost_usd=0,
            max_billable_bytes=385,
            approved_plan_sha256=_plan().plan_sha256,
            validate_component=_fake_validation,
        )
    assert [str(call["schema"]) for call in client.timeseries.calls] == [
        "definition",
        "statistics",
    ]

    download_historical_day(
        client,
        plan=_plan(),
        output_root=tmp_path,
        max_cost_usd=0,
        max_billable_bytes=385,
        approved_plan_sha256=_plan().plan_sha256,
        validate_component=_fake_validation,
    )
    assert [str(call["schema"]) for call in client.timeseries.calls] == [
        "definition",
        "statistics",
        "statistics",
        "tcbbo",
    ]


def test_interrupted_components_are_not_mixed_across_provider_revisions(
    tmp_path: Path,
):
    client = _Client(fail_once_schema="statistics")
    first_plan = replace(
        _plan(), provider_condition_last_modified_date="2026-08-27"
    )
    with pytest.raises(RuntimeError, match="simulated interruption"):
        download_historical_day(
            client,
            plan=first_plan,
            output_root=tmp_path,
            max_cost_usd=0,
            max_billable_bytes=385,
            approved_plan_sha256=first_plan.plan_sha256,
            validate_component=_fake_validation,
        )
    revised_plan = replace(
        _plan(), provider_condition_last_modified_date="2026-08-28"
    )
    client.metadata.get_dataset_condition = lambda **_: [
        {
            "date": "2026-08-27",
            "condition": "available",
            "last_modified_date": "2026-08-28",
        }
    ]
    with pytest.raises(ValueError, match="no acquisition sidecar"):
        download_historical_day(
            client,
            plan=revised_plan,
            output_root=tmp_path,
            max_cost_usd=0,
            max_billable_bytes=385,
            approved_plan_sha256=revised_plan.plan_sha256,
            validate_component=_fake_validation,
        )
    assert [str(call["schema"]) for call in client.timeseries.calls] == [
        "definition",
        "statistics",
    ]


def test_provider_revision_is_rechecked_around_each_download(tmp_path: Path):
    client = _Client()
    condition_calls = 0

    def shifting_condition(**_: object):
        nonlocal condition_calls
        condition_calls += 1
        revision = "2026-08-27" if condition_calls < 3 else "2026-08-28"
        return [
            {
                "date": "2026-08-27",
                "condition": "available",
                "last_modified_date": revision,
            }
        ]

    client.metadata.get_dataset_condition = shifting_condition
    with pytest.raises(ValueError, match="changed after planning"):
        download_historical_day(
            client,
            plan=_plan(),
            output_root=tmp_path,
            max_cost_usd=0,
            max_billable_bytes=385,
            approved_plan_sha256=_plan().plan_sha256,
            validate_component=_fake_validation,
        )
    assert len(client.timeseries.calls) == 1
    assert not (tmp_path / "2026-08-27" / "manifest.json").exists()
    assert list((tmp_path / "2026-08-27" / ".staging").rglob("*.failed.dbn.zst"))


def test_manifest_detects_component_tampering(tmp_path: Path):
    client = _Client()
    download_historical_day(
        client,
        plan=_plan(),
        output_root=tmp_path,
        max_cost_usd=0.0,
        max_billable_bytes=385,
        approved_plan_sha256=_plan().plan_sha256,
        validate_component=_fake_validation,
    )
    component = next((tmp_path / "2026-08-27").glob("*.tcbbo.dbn.zst"))
    component.write_bytes(component.read_bytes() + b"tampered")
    with pytest.raises(ValueError, match="size mismatch"):
        verify_bundle_manifest(
            tmp_path / "2026-08-27" / "manifest.json",
            validate_component=_fake_validation,
        )


def test_manifest_rejects_malformed_component_shape(tmp_path: Path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "version": "marketpin-databento-historical-bundle-v2",
                "dataset": "OPRA.PILLAR",
                "status": "complete",
                "source_kind": "databento_historical",
                "request": {},
                "request_sha256": "bad",
                "components": ["not-an-object"],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError):
        verify_bundle_manifest(manifest, validate_component=_fake_validation)


def test_manifest_rejects_request_component_window_mismatch(tmp_path: Path):
    download_historical_day(
        _Client(),
        plan=_plan(),
        output_root=tmp_path,
        max_cost_usd=0,
        max_billable_bytes=385,
        approved_plan_sha256=_plan().plan_sha256,
        validate_component=_fake_validation,
    )
    manifest = tmp_path / "2026-08-27" / "manifest.json"
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["request"]["components"][0]["end_utc"] = "2026-08-29T00:00:00Z"
    payload["request_sha256"] = _canonical_hash(payload["request"])
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="request windows do not match"):
        verify_bundle_manifest(manifest, validate_component=_fake_validation)


def test_manifest_rejects_self_consistent_noncanonical_session_window(
    tmp_path: Path,
):
    download_historical_day(
        _Client(),
        plan=_plan(),
        output_root=tmp_path,
        max_cost_usd=0,
        max_billable_bytes=385,
        approved_plan_sha256=_plan().plan_sha256,
        validate_component=_fake_validation,
    )
    manifest = tmp_path / "2026-08-27" / "manifest.json"
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    shortened_end = "2026-08-27T20:00:00Z"
    request_component = next(
        item for item in payload["request"]["components"] if item["schema"] == "tcbbo"
    )
    evidence_component = next(
        item for item in payload["components"] if item["schema"] == "tcbbo"
    )
    request_component["end_utc"] = shortened_end
    evidence_component["requested_end_utc"] = shortened_end
    evidence_component["validation"]["dbn_end_utc"] = shortened_end
    payload["request_sha256"] = _canonical_hash(payload["request"])
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="not canonical"):
        verify_bundle_manifest(manifest, validate_component=_fake_validation)


def test_manifest_rejects_plan_estimate_tampering(tmp_path: Path):
    download_historical_day(
        _Client(),
        plan=_plan(),
        output_root=tmp_path,
        max_cost_usd=0,
        max_billable_bytes=385,
        approved_plan_sha256=_plan().plan_sha256,
        validate_component=_fake_validation,
    )
    manifest = tmp_path / "2026-08-27" / "manifest.json"
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["planning"]["component_estimates"][0]["estimated_billable_bytes"] += 1
    payload["provenance_sha256"] = _canonical_hash(
        {
            "source_kind": payload["source_kind"],
            "provider_condition": payload["provider_condition"],
            "provider_condition_last_modified_date": payload[
                "provider_condition_last_modified_date"
            ],
            "planning": payload["planning"],
            "acquisition": payload["acquisition"],
        }
    )
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="planning identity"):
        verify_bundle_manifest(manifest, validate_component=_fake_validation)


def test_native_v2_manifest_requires_reviewed_plan_identity(tmp_path: Path):
    download_historical_day(
        _Client(),
        plan=_plan(),
        output_root=tmp_path,
        max_cost_usd=0,
        max_billable_bytes=385,
        approved_plan_sha256=_plan().plan_sha256,
        validate_component=_fake_validation,
    )
    manifest = tmp_path / "2026-08-27" / "manifest.json"
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["planning"].pop("plan_sha256")
    payload["provenance_sha256"] = _canonical_hash(
        {
            "source_kind": payload["source_kind"],
            "provider_condition": payload["provider_condition"],
            "provider_condition_last_modified_date": payload[
                "provider_condition_last_modified_date"
            ],
            "planning": payload["planning"],
            "acquisition": payload["acquisition"],
        }
    )
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="no reviewed plan identity"):
        verify_bundle_manifest(manifest, validate_component=_fake_validation)


def test_legacy_sidecar_binds_and_preserves_original_manifest(tmp_path: Path):
    day_dir = tmp_path / "2026-08-27"
    day_dir.mkdir(parents=True)
    components = []
    for schema_plan in _plan().schemas:
        filename = f"opra-pillar.2026-08-27.{schema_plan.schema}.dbn.zst"
        source = day_dir / filename
        source.write_bytes(f"legacy-{schema_plan.schema}".encode())
        components.append(
            {
                "schema": schema_plan.schema,
                "start_utc": schema_plan.start_utc,
                "end_utc": schema_plan.end_utc,
                "file": filename,
                "file_bytes": source.stat().st_size,
                "file_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                "estimated_billable_bytes": schema_plan.estimated_billable_bytes,
                "estimated_cost_usd": 0,
            }
        )
    identity = {
        "version": "marketpin-databento-historical-bundle-v1",
        "dataset": "OPRA.PILLAR",
        "trading_date": "2026-08-27",
        "parents": list(PARENTS),
        "components": sorted(
            [
                {
                    "schema": item["schema"],
                    "start_utc": item["start_utc"],
                    "end_utc": item["end_utc"],
                    "file": item["file"],
                    "file_bytes": item["file_bytes"],
                    "file_sha256": item["file_sha256"],
                }
                for item in components
            ],
            key=lambda item: item["schema"],
        ),
    }
    legacy = {
        **identity,
        "components": components,
        "status": "complete",
        "provider_condition": "available",
        "estimated_cost_usd": 0,
        "estimated_billable_bytes": 385,
        "completed_at_utc": "2026-08-28T00:00:00Z",
        "bundle_sha256": _canonical_hash(identity),
    }
    legacy_path = day_dir / "manifest.json"
    legacy_path.write_text(json.dumps(legacy, sort_keys=True), encoding="utf-8")
    before = legacy_path.read_bytes()
    sidecar = write_v2_attestation_for_legacy_bundle(
        legacy_path,
        provider_condition_at_attestation="available",
        provider_condition_last_modified_date_at_attestation="2026-08-27",
        validate_component=_fake_validation,
    )
    assert legacy_path.read_bytes() == before
    assert sidecar["legacy_evidence"]["manifest_sha256"] == hashlib.sha256(
        before
    ).hexdigest()

    legacy_path.write_bytes(before + b"\n")
    with pytest.raises(ValueError, match="legacy manifest hash mismatch"):
        verify_bundle_manifest(
            day_dir / "manifest.v2.json", validate_component=_fake_validation
        )

    wrong_version = json.loads(before.decode("utf-8"))
    wrong_version["version"] = "wrong-legacy-version"
    legacy_path.write_text(json.dumps(wrong_version), encoding="utf-8")
    sidecar_path = day_dir / "manifest.v2.json"
    sidecar_payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    sidecar_payload["legacy_evidence"]["manifest_sha256"] = hashlib.sha256(
        legacy_path.read_bytes()
    ).hexdigest()
    sidecar_identity = {
        "version": sidecar_payload["version"],
        "dataset": sidecar_payload["dataset"],
        "trading_date": sidecar_payload["trading_date"],
        "request_sha256": sidecar_payload["request_sha256"],
        "components": [
            {
                "schema": item["schema"],
                "requested_start_utc": item["requested_start_utc"],
                "requested_end_utc": item["requested_end_utc"],
                "file": item["file"],
                "file_bytes": item["file_bytes"],
                "file_sha256": item["file_sha256"],
                "validation": item["validation"],
            }
            for item in sidecar_payload["components"]
        ],
        "legacy_evidence": sidecar_payload["legacy_evidence"],
    }
    sidecar_payload["bundle_sha256"] = _canonical_hash(sidecar_identity)
    sidecar_payload["attestation_sha256"] = _canonical_hash(
        {
            "bundle_sha256": sidecar_payload["bundle_sha256"],
            "provenance_sha256": sidecar_payload["provenance_sha256"],
        }
    )
    sidecar_path.write_text(json.dumps(sidecar_payload), encoding="utf-8")
    with pytest.raises(ValueError, match="wrong version"):
        verify_bundle_manifest(sidecar_path, validate_component=_fake_validation)


def test_day_lock_rejects_concurrent_writer(tmp_path: Path):
    lock_path = tmp_path / "day" / ".backfill.lock"
    with HistoricalDayLock(lock_path):
        with pytest.raises(RuntimeError, match="already running"):
            with HistoricalDayLock(lock_path):
                pass


def test_staged_verified_file_without_sidecar_is_recovered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    import backend.closing_tape.historical_backfill as module

    client = _Client()
    real_atomic_json = module._atomic_json
    failed_once = False

    def fail_before_first_stage_sidecar(path: Path, payload: object):
        nonlocal failed_once
        if not failed_once and path.name.endswith("verified.dbn.zst.json"):
            failed_once = True
            raise RuntimeError("simulated sidecar crash")
        real_atomic_json(path, payload)

    monkeypatch.setattr(module, "_atomic_json", fail_before_first_stage_sidecar)
    with pytest.raises(RuntimeError, match="simulated sidecar crash"):
        download_historical_day(
            client,
            plan=_plan(),
            output_root=tmp_path,
            max_cost_usd=0,
            max_billable_bytes=385,
            approved_plan_sha256=_plan().plan_sha256,
            validate_component=_fake_validation,
        )
    monkeypatch.setattr(module, "_atomic_json", real_atomic_json)
    download_historical_day(
        client,
        plan=_plan(),
        output_root=tmp_path,
        max_cost_usd=0,
        max_billable_bytes=385,
        approved_plan_sha256=_plan().plan_sha256,
        validate_component=_fake_validation,
    )
    assert [str(call["schema"]) for call in client.timeseries.calls] == [
        "definition",
        "statistics",
        "tcbbo",
    ]


def _fake_store(schema: str, *, include_records: bool = True):
    schema_plan = next(item for item in _plan().schemas if item.schema == schema)
    start = int(datetime.fromisoformat(schema_plan.start_utc.replace("Z", "+00:00")).timestamp()) * 10**9
    end = int(datetime.fromisoformat(schema_plan.end_utc.replace("Z", "+00:00")).timestamp()) * 10**9
    mappings = {}
    records = []
    rtype = {"tcbbo": 194, "statistics": 24, "definition": 19}[schema]
    for instrument_id, parent in enumerate(PARENTS, start=1):
        root = parent.removesuffix(".OPT")
        raw_symbol = f"{root:<6}260827C00100000"
        mappings[raw_symbol] = [
            {
                "start_date": date(2026, 8, 27),
                "end_date": date(2026, 8, 28),
                "symbol": str(instrument_id),
            }
        ]
        if include_records:
            values = {
                "rtype": rtype,
                "ts_recv": start + instrument_id,
                "ts_event": start + instrument_id,
                "instrument_id": instrument_id,
            }
            if schema == "definition":
                values["raw_symbol"] = raw_symbol
                values["instrument_class"] = 67
                values["expiration"] = end + 86_400 * 10**9
                values["strike_price"] = 1_000_000_000
            if schema == "statistics":
                values["stat_type"] = 9
                values["quantity"] = 100
            if schema == "tcbbo":
                values["price"] = 1_000_000_000
                values["levels"] = [
                    SimpleNamespace(
                        bid_px=900_000_000,
                        ask_px=1_100_000_000,
                        bid_sz=10,
                        ask_sz=12,
                    )
                ]
            records.append(SimpleNamespace(**values))
    metadata = SimpleNamespace(
        version=3,
        dataset="OPRA.PILLAR",
        schema=schema,
        stype_in="parent",
        stype_out="instrument_id",
        start=start,
        end=end,
        symbols=list(PARENTS),
        not_found=[],
        partial=[],
        mappings=mappings,
    )

    class Store:
        def __init__(self):
            self.metadata = metadata
            self.compression = "zstd"
            self.records = records

        def __iter__(self):
            return iter(self.records)

    return Store(), schema_plan


@pytest.mark.parametrize("schema", ["tcbbo", "statistics", "definition"])
def test_component_validator_streams_and_attests_expected_content(schema: str):
    store, schema_plan = _fake_store(schema)
    result = validate_dbn_component(
        Path("unused.dbn.zst"),
        schema_plan,
        PARENTS,
        store_factory=lambda _: store,
    )
    assert result["record_count"] == len(PARENTS)
    assert result["record_types"] == {
        str({"tcbbo": 194, "statistics": 24, "definition": 19}[schema]): len(PARENTS)
    }
    assert result["dbn_mapping_count"] == len(PARENTS)


def test_component_validator_rejects_zero_record_file():
    store, schema_plan = _fake_store("tcbbo", include_records=False)
    with pytest.raises(ValueError, match="zero records"):
        validate_dbn_component(
            Path("unused.dbn.zst"),
            schema_plan,
            PARENTS,
            store_factory=lambda _: store,
        )


def test_component_validator_rejects_wrong_compression():
    store, schema_plan = _fake_store("tcbbo")
    store.compression = "none"
    with pytest.raises(ValueError, match="compression"):
        validate_dbn_component(
            Path("unused.dbn"),
            schema_plan,
            PARENTS,
            store_factory=lambda _: store,
        )


def test_component_validator_rejects_only_undefined_open_interest():
    store, schema_plan = _fake_store("statistics")
    for record in store.records:
        record.quantity = 2**63 - 1
    with pytest.raises(ValueError, match="no usable open-interest"):
        validate_dbn_component(
            Path("unused.dbn.zst"),
            schema_plan,
            PARENTS,
            store_factory=lambda _: store,
        )


def test_component_validator_rejects_undefined_definition_strike():
    store, schema_plan = _fake_store("definition")
    store.records[0].strike_price = 2**63 - 1
    with pytest.raises(ValueError, match="undefined strike"):
        validate_dbn_component(
            Path("unused.dbn.zst"),
            schema_plan,
            PARENTS,
            store_factory=lambda _: store,
        )
