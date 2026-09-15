from __future__ import annotations

import json
import subprocess
import sys
import re
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

import pytest

from backend.closing_analysis import HORIZON_ID, run_closing_analysis
from backend.closing_tape.catalog import TapeCatalog
from backend.closing_tape.config import SessionConfig, build_session_config


UTC = timezone.utc


def test_closing_analysis_supports_direct_script_execution():
    script = Path(__file__).parents[1] / "backend" / "closing_analysis.py"
    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        cwd=Path(__file__).parents[1], capture_output=True, text=True, timeout=15,
        check=False,
    )

    assert result.returncode == 0
    assert "Build an audited close-minus-15" in result.stdout
TRADING_DAY = date(2026, 8, 25)
ASOF_UTC = datetime(2026, 8, 25, 19, 45, tzinfo=UTC)
ROOTS = ("SPX", "NDX", "RUT", "VIX", "SPY")


def _new_catalog(project_root: Path, session_id: str) -> tuple[TapeCatalog, SessionConfig]:
    config = build_session_config(
        project_root,
        trading_day=TRADING_DAY,
        now=ASOF_UTC,
        session_id=session_id,
    )
    catalog = TapeCatalog(config.catalog_path)
    catalog.start_session(config)
    return catalog, config


def _populate_healthy_tape(catalog: TapeCatalog, config: SessionConfig) -> None:
    feed = config.feeds[0]
    catalog.register_feed(config.session_id, feed, config.output_dir / "opra_options.dbn")
    catalog.update_feed_status(
        config.session_id,
        feed.name,
        {
            "status": "complete",
            "records_seen": 1_000,
            "trade_records": 500,
            "mapping_records": 100,
            "statistics_records": 100,
            "definition_records": 100,
            "unmapped_trade_records": 0,
            "last_event_ns": int((ASOF_UTC - timedelta(seconds=15)).timestamp() * 1_000_000_000),
            "reconnect_count": 0,
            "slow_reader_warnings": 0,
            "complete": 1,
        },
    )

    minute = (ASOF_UTC - timedelta(minutes=1)).replace(second=0, microsecond=0).isoformat()
    updated = ASOF_UTC.isoformat()
    catalog.upsert_observed_minutes(
        [
            {
                "session_id": config.session_id,
                "feed_name": feed.name,
                "family_root": root,
                "minute_utc": minute,
                "asset_class": "options",
                "trade_count": 20,
                "volume": 40.0,
                "notional": 40_000.0,
                "call_count": 12,
                "put_count": 8,
                "call_volume": 25.0,
                "put_volume": 15.0,
                "call_premium": 25_000.0,
                "put_premium": 15_000.0,
                "first_price": 10.0,
                "high_price": 12.0,
                "low_price": 9.0,
                "last_price": 11.0,
                "price_volume_sum": 440.0,
                "largest_trade_size": 10.0,
                "largest_trade_notional": 10_000.0,
                "updated_at_utc": updated,
            }
            for root in ROOTS
        ]
    )
    catalog.upsert_inferred_minute_flow(
        [
            {
                "session_id": config.session_id,
                "feed_name": feed.name,
                "family_root": root,
                "minute_utc": minute,
                "inference_method": "trade_price_vs_pretrade_nbbo",
                "inference_version": "1.0",
                "source_sha256": f"source-{root}",
                "at_ask_count": 10,
                "at_bid_count": 6,
                "inside_count": 2,
                "unknown_count": 2,
                "at_ask_volume": 20.0,
                "at_bid_volume": 12.0,
                "inside_volume": 4.0,
                "unknown_volume": 4.0,
                "at_ask_notional": 20_000.0,
                "at_bid_notional": 12_000.0,
                "inside_notional": 4_000.0,
                "unknown_notional": 4_000.0,
                "call_at_ask_notional": 15_000.0,
                "call_at_bid_notional": 5_000.0,
                "put_at_ask_notional": 6_000.0,
                "put_at_bid_notional": 8_000.0,
                "updated_at_utc": updated,
            }
            for root in ROOTS
        ]
    )
    catalog.upsert_open_interest(
        [
            {
                "session_id": config.session_id,
                "feed_name": feed.name,
                "instrument_id": instrument_id,
                "raw_symbol": f"{root} TEST",
                "family_root": root,
                "asof_utc": ASOF_UTC.isoformat(),
                "open_interest": 1_000.0,
            }
            for instrument_id, root in enumerate(ROOTS, start=1)
        ]
    )


def _create_market_database(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    prediction_time = (ASOF_UTC - timedelta(seconds=30)).replace(tzinfo=None).isoformat(sep=" ")
    quote_time = (ASOF_UTC - timedelta(seconds=31)).replace(tzinfo=None).isoformat(sep=" ")
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE prediction_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timestamp_utc TEXT NOT NULL,
                trading_date TEXT NOT NULL,
                quote_timestamp_utc TEXT,
                is_valid INTEGER NOT NULL,
                validation_status TEXT,
                provider TEXT,
                model_version TEXT,
                current_price REAL,
                predicted_close REAL,
                confidence REAL,
                gamma_pin REAL,
                max_pain REAL,
                zero_gamma REAL,
                gross_gex REAL,
                net_gex REAL,
                quote_age_seconds REAL,
                data_age_seconds REAL,
                subscription_epoch_id TEXT,
                subscription_generation INTEGER,
                active_contract_count INTEGER,
                fresh_quote_count INTEGER
            )
            """
        )
        connection.executemany(
            """
            INSERT INTO prediction_snapshots (
                symbol, timestamp_utc, trading_date, quote_timestamp_utc,
                is_valid, validation_status, provider, model_version,
                current_price, predicted_close, confidence, gamma_pin,
                max_pain, zero_gamma, gross_gex, net_gex,
                quote_age_seconds, data_age_seconds, subscription_epoch_id,
                subscription_generation,
                active_contract_count, fresh_quote_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "SPX", prediction_time, TRADING_DAY.isoformat(), quote_time,
                    1, "valid", "databento", "test-model", 5_100.0, 5_105.0,
                    0.60, 5_100.0, 5_125.0, 5_103.25, 1_000_000.0, 100_000.0,
                    1.0, 1.0, "e" * 64, 7, 2_000, 1_900,
                ),
                (
                    "NDX", prediction_time, TRADING_DAY.isoformat(), quote_time,
                    1, "valid", "databento", "test-model", 22_300.0, 22_320.0,
                    0.55, 22_300.0, 22_350.0, 22_312.75, 2_000_000.0, -250_000.0,
                    1.0, 1.0, "e" * 64, 7, 3_000, 2_850,
                ),
            ],
        )


@pytest.fixture
def healthy_case(tmp_path: Path) -> dict[str, Any]:
    project_root = tmp_path / "project"
    catalog, config = _new_catalog(project_root, "healthy-session")
    _populate_healthy_tape(catalog, config)
    market_path = project_root / "data" / "market_data.db"
    _create_market_database(market_path)
    return {
        "catalog": catalog,
        "config": config,
        "market_path": market_path,
        "output_root": project_root / "exports" / "decision_support",
    }


def _run(case: dict[str, Any]) -> dict[str, object]:
    return run_closing_analysis(
        tape_catalog_path=case["catalog"].path,
        market_db_path=case["market_path"],
        output_root=case["output_root"],
        session_id=case["config"].session_id,
        trading_day=TRADING_DAY,
        asof_utc=ASOF_UTC,
    )


def _walk_keys(value: object) -> Iterator[str]:
    if isinstance(value, dict):
        for key, child in value.items():
            yield str(key)
            yield from _walk_keys(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_keys(child)


def _walk_strings(value: object) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for child in value.values():
            yield from _walk_strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_strings(child)


def test_current_catalog_missing_inputs_fail_closed_instead_of_crashing(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    catalog, config = _new_catalog(project_root, "schema-contract-session")

    report = run_closing_analysis(
        tape_catalog_path=catalog.path,
        market_db_path=project_root / "data" / "missing-market.db",
        output_root=project_root / "exports" / "decision_support",
        session_id=config.session_id,
        trading_day=TRADING_DAY,
        asof_utc=ASOF_UTC,
    )

    assert report["decision_state"] == "ABSTAIN"
    reasons = report["abstention_reasons"]
    assert "required OPRA options tape feed is missing" in reasons
    assert "SPX option trade coverage is missing" in reasons
    assert "canonical MarketPin database is missing" in reasons


def test_unhealthy_required_feed_forces_abstention(healthy_case: dict[str, Any]) -> None:
    catalog = healthy_case["catalog"]
    config = healthy_case["config"]
    catalog.update_feed_status(
        config.session_id,
        "opra_options",
        {"status": "error", "reconnect_count": 1, "slow_reader_warnings": 1},
    )

    report = _run(healthy_case)

    assert report["decision_state"] == "ABSTAIN"
    assert report["use_gate"] == "NOT_VALIDATED_FOR_LIVE_TRADE_DECISIONS"
    assert "OPRA tape status is error" in report["abstention_reasons"]
    assert "OPRA tape has a recorded reconnect gap" in report["abstention_reasons"]
    assert "OPRA tape reported slow-reader or skipped-record risk" in report["abstention_reasons"]


def test_post_horizon_trade_in_analysis_prefix_forces_abstention(
    healthy_case: dict[str, Any],
) -> None:
    catalog = healthy_case["catalog"]
    config = healthy_case["config"]
    cutoff_ns = int(ASOF_UTC.timestamp() * 1_000_000_000)
    catalog.record_analysis_cutoff(
        session_id=config.session_id,
        feed_name="opra_options",
        horizon_id=HORIZON_ID,
        captured_at_utc=(ASOF_UTC + timedelta(seconds=2)).isoformat(),
        event_cutoff_utc=ASOF_UTC.isoformat(),
        cutoff_bytes=1_024,
        record_sequence=100,
        processed_sequence=100,
        prefix_sha256="a" * 64,
        last_trade_event_ns=cutoff_ns + 1,
    )

    report = _run(healthy_case)

    assert report["decision_state"] == "ABSTAIN"
    assert (
        "OPRA analysis prefix contains a post-horizon trade event"
        in report["abstention_reasons"]
    )


def test_report_has_no_actionable_order_buy_or_sell_fields(healthy_case: dict[str, Any]) -> None:
    report = _run(healthy_case)

    assert report["decision_state"] == "RESEARCH_ONLY"
    assert report["marketpin_context_alignment"] == {
        "aligned": True,
        "subscription_epoch_id": "e" * 64,
        "subscription_generation": 7,
        "identities": {
            "SPX": {
                "subscription_epoch_id": "e" * 64,
                "subscription_generation": 7,
            },
            "NDX": {
                "subscription_epoch_id": "e" * 64,
                "subscription_generation": 7,
            },
        },
    }
    forbidden_tokens = {"order", "orders", "buy", "sell"}
    key_tokens = {
        token
        for key in _walk_keys(report)
        for token in re.split(r"[^a-z0-9]+", key.casefold())
        if token
    }
    assert key_tokens.isdisjoint(forbidden_tokens)
    assert not any(
        re.search(r"\b(?:buy|sell)\b", text, flags=re.IGNORECASE)
        for text in _walk_strings(report)
    )


def test_cross_process_marketpin_context_forces_abstention(
    healthy_case: dict[str, Any],
) -> None:
    with sqlite3.connect(healthy_case["market_path"]) as connection:
        connection.execute(
            "UPDATE prediction_snapshots SET subscription_epoch_id=? WHERE symbol='NDX'",
            ("f" * 64,),
        )

    report = _run(healthy_case)

    assert report["decision_state"] == "ABSTAIN"
    assert report["marketpin_context_alignment"]["aligned"] is False
    assert "SPX/NDX MarketPin process identities are not aligned" in report[
        "abstention_reasons"
    ]


def test_zero_gamma_is_exposed_as_a_modeled_level_not_a_strike_or_target(
    healthy_case: dict[str, Any],
) -> None:
    report = _run(healthy_case)

    spx_context = report["marketpin_context"]["SPX"]
    assert spx_context["zero_gamma_modeled_level"] == pytest.approx(5_103.25)
    assert "zero_gamma" not in spx_context
    semantics = report["level_semantics"]["zero_gamma_modeled_level"]
    assert "continuous linear-interpolation regime crossing" in semantics
    assert "not necessarily a listed strike" in semantics
    assert "not used as a close target" in semantics

    markdown_path = (
        healthy_case["output_root"]
        / TRADING_DAY.isoformat()
        / f"closing_analysis_{healthy_case['config'].session_id}_{HORIZON_ID}.md"
    )
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "interpolated modeled regime level" in markdown
    assert "not a listed strike" in markdown
    assert "not a close target" in markdown


def test_repeated_run_is_idempotent_for_session_and_horizon(healthy_case: dict[str, Any]) -> None:
    first = _run(healthy_case)
    stem = f"closing_analysis_{healthy_case['config'].session_id}_{HORIZON_ID}"
    day_dir = healthy_case["output_root"] / TRADING_DAY.isoformat()
    json_path = day_dir / f"{stem}.json"
    markdown_path = day_dir / f"{stem}.md"
    first_artifacts = (
        json_path.read_text(encoding="utf-8"),
        markdown_path.read_text(encoding="utf-8"),
    )
    second = _run(healthy_case)

    assert second == first
    assert second["created_at_utc"] == first["created_at_utc"]
    assert (
        json_path.read_text(encoding="utf-8"),
        markdown_path.read_text(encoding="utf-8"),
    ) == first_artifacts
    with sqlite3.connect(healthy_case["catalog"].path) as connection:
        count = connection.execute(
            """
            SELECT COUNT(*) FROM closing_analysis_runs
            WHERE session_id=? AND horizon_id=?
            """,
            (healthy_case["config"].session_id, HORIZON_ID),
        ).fetchone()[0]
        persisted = json.loads(
            connection.execute(
                """
                SELECT report_json FROM closing_analysis_runs
                WHERE session_id=? AND horizon_id=?
                """,
                (healthy_case["config"].session_id, HORIZON_ID),
            ).fetchone()[0]
        )

    assert count == 1
    assert persisted == first
