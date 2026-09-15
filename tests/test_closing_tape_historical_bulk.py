from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.closing_tape.historical_bulk import (
    execute_historical_bulk,
    plan_historical_bulk,
)
from tools.run_closing_tape_historical_bulk import _requested_dates


UTC = timezone.utc


class _Metadata:
    def __init__(self, *, condition: str = "available", cost: float = 0.0):
        self.condition = condition
        self.cost = cost
        self.range_calls = 0
        self.cost_calls = 0

    def get_dataset_range(self, **_: object):
        self.range_calls += 1
        return {
            "schema": {
                schema: {
                    "start": "2023-03-28T00:00:00Z",
                    "end": "2026-09-02T00:00:00Z",
                }
                for schema in ("tcbbo", "statistics", "definition")
            }
        }

    def get_dataset_condition(self, **kwargs: object):
        requested = str(kwargs["start_date"])
        return [
            {
                "date": requested,
                "condition": self.condition,
                "last_modified_date": requested,
            }
        ]

    def get_cost(self, **_: object):
        self.cost_calls += 1
        return self.cost

    def get_billable_size(self, **kwargs: object):
        return {"tcbbo": 230, "statistics": 130, "definition": 25}[
            str(kwargs["schema"])
        ]


class _Timeseries:
    def __init__(self):
        self.calls: list[dict[str, object]] = []

    def get_range(self, **kwargs: object):
        self.calls.append(dict(kwargs))
        raise AssertionError("tests must not call a real or implicit time-series service")


class _Client:
    def __init__(self, *, condition: str = "available", cost: float = 0.0):
        self.metadata = _Metadata(condition=condition, cost=cost)
        self.timeseries = _Timeseries()


@dataclass(frozen=True)
class _Session:
    trading_date: str
    session_id: str = "session"
    eligible: bool = False
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class _LabelWork:
    trading_date: str
    session_id: str = "session"
    source_sha256: str = "a" * 64
    verified_families: tuple[str, ...] = ()
    missing_families: tuple[str, ...] = ("SPX", "NDX", "RUT", "VIX", "SPY")
    missing_requirements: tuple[object, ...] = ()


@dataclass(frozen=True)
class _Readiness:
    sessions_detail: tuple[_Session, ...] = ()
    label_work_queue: tuple[_LabelWork, ...] = ()
    eligible_sessions: int = 0
    model_evidence_sessions: int = 0
    model_training_ready: bool = False

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class _ReadinessSequence:
    def __init__(self, *values: _Readiness):
        self.values = list(values)
        self.calls = 0

    def __call__(self, _root: str | Path) -> _Readiness:
        index = min(self.calls, len(self.values) - 1)
        self.calls += 1
        return self.values[index]


def _no_bundle(*_args: object, **_kwargs: object) -> None:
    return None


def _fixed_now() -> datetime:
    return datetime(2026, 9, 1, 23, 0, tzinfo=UTC)


def _fake_stages(tmp_path: Path):
    counters = {"downloads": 0, "imports": 0}

    def discover(output_root: str | Path, trading_day: date, **_: object):
        path = Path(output_root) / trading_day.isoformat() / "manifest.json"
        return str(path.resolve()) if path.is_file() else None

    def download(_client: object, *, plan, output_root: str | Path, **_: object):
        path = Path(output_root) / plan.trading_date / "manifest.json"
        if not path.exists():
            counters["downloads"] += 1
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(
                    {
                        "status": "complete",
                        "bundle_sha256": "a" * 64,
                    }
                ),
                encoding="utf-8",
            )
        return json.loads(path.read_text(encoding="utf-8"))

    def verify(path: str | Path):
        return json.loads(Path(path).read_text(encoding="utf-8"))

    def import_bundle(_root: str | Path, manifest: str | Path):
        counters["imports"] += 1
        trading_day = Path(manifest).parent.name
        return {
            "status": "complete" if counters["imports"] == 1 else "already_complete",
            "trading_date": trading_day,
            "session_id": f"{trading_day}-historical-aaaaaaaaaaaa",
            "source_sha256": "a" * 64,
        }

    return counters, discover, download, verify, import_bundle


def test_default_plan_is_metadata_only_and_creates_no_journal(tmp_path: Path):
    client = _Client(cost=0.25)
    journal_root = tmp_path / "data" / "backtest_pipeline" / "historical_bulk"
    plan = plan_historical_bulk(
        client,
        project_root=tmp_path,
        trading_dates=(date(2026, 8, 27),),
        readiness_fn=lambda _root: _Readiness(),
        discover_bundle_fn=_no_bundle,
    )

    assert plan.ready_for_execute
    assert plan.estimated_cost_usd == pytest.approx(0.75)
    assert client.metadata.range_calls == 1
    assert client.metadata.cost_calls == 3
    assert client.timeseries.calls == []
    assert not journal_root.exists()


def test_cli_range_skips_configured_holidays_but_explicit_holiday_is_rejected():
    ranged = _requested_dates(
        SimpleNamespace(
            date=None,
            start=date(2026, 9, 4),
            end=date(2026, 9, 8),
        )
    )
    assert ranged == (date(2026, 9, 4), date(2026, 9, 8))

    with pytest.raises(ValueError, match="not a configured US cash-market session"):
        _requested_dates(
            SimpleNamespace(
                date=[date(2026, 9, 7)],
                start=None,
                end=None,
            )
        )


def test_aggregate_caps_and_batch_hash_fail_before_any_download(tmp_path: Path):
    client = _Client(cost=1.0)
    plan = plan_historical_bulk(
        client,
        project_root=tmp_path,
        trading_dates=(date(2026, 8, 27), date(2026, 8, 28)),
        readiness_fn=lambda _root: _Readiness(),
        discover_bundle_fn=_no_bundle,
    )
    calls: list[str] = []

    with pytest.raises(ValueError, match="exact batch_plan_sha256"):
        execute_historical_bulk(
            client,
            plan=plan,
            approved_batch_plan_sha256="0" * 64,
            max_estimated_cost_usd=6.0,
            max_estimated_billable_bytes=10_000,
            download_day_fn=lambda *_args, **_kwargs: calls.append("download"),
            compute_guard_fn=lambda *_args, **_kwargs: (),
        )
    with pytest.raises(ValueError, match="aggregate estimated cost"):
        execute_historical_bulk(
            client,
            plan=plan,
            approved_batch_plan_sha256=plan.batch_plan_sha256,
            max_estimated_cost_usd=5.99,
            max_estimated_billable_bytes=10_000,
            download_day_fn=lambda *_args, **_kwargs: calls.append("download"),
            compute_guard_fn=lambda *_args, **_kwargs: (),
        )

    assert calls == []
    assert client.timeseries.calls == []
    assert not (tmp_path / "data" / "backtest_pipeline").exists()


def test_active_capture_guard_has_no_override_and_blocks_before_download(tmp_path: Path):
    client = _Client()
    plan = plan_historical_bulk(
        client,
        project_root=tmp_path,
        trading_dates=(date(2026, 8, 27),),
        readiness_fn=lambda _root: _Readiness(),
        discover_bundle_fn=_no_bundle,
    )
    downloads: list[str] = []

    def active_guard(*_args: object, **kwargs: object):
        assert kwargs["override_supported"] is False
        raise RuntimeError("capture is active")

    with pytest.raises(RuntimeError, match="capture is active"):
        execute_historical_bulk(
            client,
            plan=plan,
            approved_batch_plan_sha256=plan.batch_plan_sha256,
            max_estimated_cost_usd=0.0,
            max_estimated_billable_bytes=1_000,
            download_day_fn=lambda *_args, **_kwargs: downloads.append("download"),
            compute_guard_fn=active_guard,
        )

    assert downloads == []
    assert not (tmp_path / "data" / "backtest_pipeline").exists()


def test_execute_resumes_physical_evidence_and_import_is_idempotent(tmp_path: Path):
    client = _Client()
    after = _Readiness(
        sessions_detail=(
            _Session("2026-08-27", eligible=True, reasons=()),
        ),
        label_work_queue=(_LabelWork("2026-08-27"),),
        eligible_sessions=1,
    )
    readiness = _ReadinessSequence(_Readiness(), after, after)
    counters, discover, download, verify, import_bundle = _fake_stages(tmp_path)
    plan = plan_historical_bulk(
        client,
        project_root=tmp_path,
        trading_dates=(date(2026, 8, 27),),
        readiness_fn=readiness,
        discover_bundle_fn=discover,
    )
    kwargs = {
        "client": client,
        "plan": plan,
        "approved_batch_plan_sha256": plan.batch_plan_sha256,
        "max_estimated_cost_usd": 0.0,
        "max_estimated_billable_bytes": 1_000,
        "download_day_fn": download,
        "discover_bundle_fn": discover,
        "verify_bundle_fn": verify,
        "import_bundle_fn": import_bundle,
        "readiness_fn": readiness,
        "compute_guard_fn": lambda *_args, **_kwargs: (),
        "now_fn": _fixed_now,
    }

    first = execute_historical_bulk(**kwargs)
    second = execute_historical_bulk(**kwargs)

    assert first["status"] == "complete"
    assert second["status"] == "complete"
    assert counters["downloads"] == 1
    assert counters["imports"] == 2
    assert first["dates"][0]["labels"]["status"] == "full_five_family_bundle_pending"
    journal = Path(str(first["journal_path"]))
    payload = json.loads(journal.read_text(encoding="utf-8"))
    event_keys = [event["event_key"] for event in payload["events"]]
    assert len(event_keys) == len(set(event_keys))
    assert not list(journal.parent.glob(".*.tmp"))


def test_provider_condition_and_duplicate_date_fail_closed(tmp_path: Path):
    unavailable = plan_historical_bulk(
        _Client(condition="unavailable"),
        project_root=tmp_path,
        trading_dates=(date(2026, 8, 27),),
        readiness_fn=lambda _root: _Readiness(),
        discover_bundle_fn=_no_bundle,
    )
    assert not unavailable.ready_for_execute
    assert "provider condition is unavailable" in unavailable.day_plans[0].blocked_reasons

    duplicate_reason = (
        "multiple eligible captures share this trading date; canonical independent "
        "evidence is ambiguous"
    )
    duplicate = plan_historical_bulk(
        _Client(),
        project_root=tmp_path,
        trading_dates=(date(2026, 8, 27),),
        readiness_fn=lambda _root: _Readiness(
            sessions_detail=(
                _Session("2026-08-27", "one", False, (duplicate_reason,)),
                _Session("2026-08-27", "two", False, (duplicate_reason,)),
            )
        ),
        discover_bundle_fn=_no_bundle,
    )
    assert not duplicate.ready_for_execute
    assert any("increase duplicate-date ambiguity" in reason for reason in duplicate.day_plans[0].blocked_reasons)


def test_journal_path_cannot_escape_project_before_download(tmp_path: Path):
    client = _Client()
    plan = plan_historical_bulk(
        client,
        project_root=tmp_path,
        trading_dates=(date(2026, 8, 27),),
        readiness_fn=lambda _root: _Readiness(),
        discover_bundle_fn=_no_bundle,
    )
    calls: list[str] = []
    with pytest.raises(ValueError, match="journal root must remain inside"):
        execute_historical_bulk(
            client,
            plan=plan,
            approved_batch_plan_sha256=plan.batch_plan_sha256,
            max_estimated_cost_usd=0.0,
            max_estimated_billable_bytes=1_000,
            journal_root=tmp_path.parent / "escaped-journal",
            download_day_fn=lambda *_args, **_kwargs: calls.append("download"),
            compute_guard_fn=lambda *_args, **_kwargs: (),
        )
    assert calls == []


def test_discovered_manifest_cannot_escape_trading_day_root(tmp_path: Path):
    client = _Client()
    plan = plan_historical_bulk(
        client,
        project_root=tmp_path,
        trading_dates=(date(2026, 8, 27),),
        readiness_fn=lambda _root: _Readiness(),
        discover_bundle_fn=_no_bundle,
    )
    escaped = tmp_path / "data" / "elsewhere" / "manifest.json"
    escaped.parent.mkdir(parents=True)
    escaped.write_text(
        json.dumps({"status": "complete", "bundle_sha256": "a" * 64}),
        encoding="utf-8",
    )
    imports: list[str] = []
    result = execute_historical_bulk(
        client,
        plan=plan,
        approved_batch_plan_sha256=plan.batch_plan_sha256,
        max_estimated_cost_usd=0.0,
        max_estimated_billable_bytes=1_000,
        download_day_fn=lambda *_args, **_kwargs: {
            "status": "complete",
            "bundle_sha256": "a" * 64,
        },
        discover_bundle_fn=lambda *_args, **_kwargs: str(escaped),
        verify_bundle_fn=lambda _path: {
            "status": "complete",
            "bundle_sha256": "a" * 64,
        },
        import_bundle_fn=lambda *_args, **_kwargs: imports.append("import"),
        readiness_fn=lambda _root: _Readiness(),
        compute_guard_fn=lambda *_args, **_kwargs: (),
        now_fn=_fixed_now,
    )
    assert result["status"] == "partial"
    assert "escapes its configured trading-day directory" in result["failures"][0]["error"]
    assert imports == []


def test_full_five_family_label_state_is_reported_without_surface_claim(tmp_path: Path):
    client = _Client()
    after = _Readiness(
        sessions_detail=(_Session("2026-08-27", eligible=True),),
        eligible_sessions=1,
        model_evidence_sessions=1,
    )
    readiness = _ReadinessSequence(_Readiness(), after)
    counters, discover, download, verify, import_bundle = _fake_stages(tmp_path)
    plan = plan_historical_bulk(
        client,
        project_root=tmp_path,
        trading_dates=(date(2026, 8, 27),),
        readiness_fn=readiness,
        discover_bundle_fn=discover,
    )
    result = execute_historical_bulk(
        client,
        plan=plan,
        approved_batch_plan_sha256=plan.batch_plan_sha256,
        max_estimated_cost_usd=0.0,
        max_estimated_billable_bytes=1_000,
        download_day_fn=download,
        discover_bundle_fn=discover,
        verify_bundle_fn=verify,
        import_bundle_fn=import_bundle,
        readiness_fn=readiness,
        compute_guard_fn=lambda *_args, **_kwargs: (),
        now_fn=_fixed_now,
    )

    assert counters == {"downloads": 1, "imports": 1}
    assert result["dates"][0]["labels"]["status"] == "full_five_family_bundle_verified"
    assert result["surface"]["status"] == "not_requested"
    assert "separately gated" in result["surface"]["reason"]
