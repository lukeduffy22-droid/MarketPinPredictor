"""Bounded, query-only adapters from retained evidence into research models.

No schema creation, ORM sessions, backfill or source-database writes occur here.
"""
from __future__ import annotations

from datetime import datetime, timezone
from contextlib import closing
from pathlib import Path
import sqlite3
import time
from zoneinfo import ZoneInfo

from app.utils.time_et import close_time_et


def utc(value):
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def load_research_history(database_path, symbols, *, as_of_utc, runtime_context=None):
    """Read one coherent WAL snapshot; preserve event AND availability cutoffs.

    OPRA parity prices remain derived references, never official cash quotes.
    Legacy rows lacking process identity cannot acquire it from current state.
    """
    as_of = utc(as_of_utc)
    day = as_of.astimezone(ZoneInfo("America/New_York")).date()
    cutoff = as_of.replace(tzinfo=None).isoformat(" ", timespec="microseconds")
    histories = {symbol: [] for symbol in dict.fromkeys(symbols)}
    diagnostics = []
    path = Path(database_path).resolve()
    if not path.is_file():
        return histories, ["RETAINED_DATABASE_MISSING"]
    started = time.monotonic()
    context = runtime_context or {}
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=0.25)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.set_progress_handler(lambda: int(time.monotonic() - started > 2.0), 2000)
        conn.execute("BEGIN")
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        for symbol in histories:
            if "market_structure_observations" in tables:
                rows = conn.execute(
                    "SELECT * FROM market_structure_observations WHERE symbol=? AND trading_date=? "
                    "AND source_timestamp_utc<=? AND captured_at_utc<=? "
                    "ORDER BY source_timestamp_utc DESC, observation_id DESC LIMIT 2000",
                    (symbol, day.isoformat(), cutoff, cutoff),
                ).fetchall()
                for raw in reversed(rows):
                    row = dict(raw)
                    identity_ok = (
                        context.get("handoff_status") == "active"
                        and context.get("runtime_context_stable") is True
                        and bool(context.get("subscription_epoch_id"))
                        and row.get("subscription_epoch_id") == context.get("subscription_epoch_id")
                        and row.get("subscription_generation") == context.get("subscription_generation")
                    )
                    symbol_status = (context.get("symbol_status") or {}).get(symbol, {})
                    live_ok = symbol_status.get("usable_for_prediction") is True
                    row.update({
                        "timestamp_utc": utc(row["source_timestamp_utc"]).isoformat(),
                        "available_at_utc": utc(row["captured_at_utc"]).isoformat(),
                        "spot": row["reference_price"], "price": row["reference_price"],
                        "valid": identity_ok and live_ok and row["validation_status"] == "valid",
                        "source": row.get("spot_source") or row.get("provider"),
                        "source_verified": row["validation_status"] == "valid",
                        "source_sha256": row.get("universe_sha256"),
                        "same_day_profile_available": row.get("same_day_profile_available") == 1,
                        "active_generation": context.get("subscription_generation"),
                        "active_subscription_epoch_id": context.get("subscription_epoch_id"),
                        "options_context_valid": identity_ok and live_ok and row.get("same_day_profile_available") == 1,
                        "options_expiration_date": row.get("primary_expiration"),
                        "options_source_timestamp_utc": utc(row["source_timestamp_utc"]).isoformat(),
                        "options_available_at_utc": utc(row["captured_at_utc"]).isoformat(),
                    })
                    # Direction engine accepts validation_status too; invalidate both.
                    if not row["valid"]:
                        row["validation_status"] = "runtime_unverified"
                    histories[symbol].append(row)
            if "eod_close_observations" in tables:
                rows = conn.execute(
                    "SELECT * FROM eod_close_observations WHERE symbol=? "
                    "AND observed_at_utc<=? AND ingested_at_utc<=? "
                    "ORDER BY trading_date DESC, id DESC LIMIT 500",
                    (symbol, cutoff, cutoff),
                ).fetchall()
                grouped = {}
                for raw in rows:
                    row = dict(raw)
                    grouped.setdefault(row["trading_date"], []).append(row)
                for session_day, revisions in sorted(grouped.items()):
                    # An unverified latest correction invalidates the date. Never
                    # silently fall back to the older, now superseded close.
                    row = revisions[0]
                    if not row["source_verified"] or not row.get("source_artifact_sha256"):
                        continue
                    prices = {item["official_close"] for item in revisions}
                    if len(prices) > 1 and not row.get("correction_of_id"):
                        diagnostics.append(f"{symbol}:{session_day}:CONFLICTING_CLOSES")
                        continue
                    close = close_time_et(datetime.fromisoformat(session_day).replace(tzinfo=ZoneInfo("America/New_York")))
                    histories[symbol].append({
                        "kind": "daily_close", "symbol": symbol, "session_date": session_day,
                        "timestamp_utc": close.astimezone(timezone.utc).isoformat(),
                        "available_at_utc": max(utc(row["observed_at_utc"]), utc(row["ingested_at_utc"])).isoformat(),
                        "official_close": row["official_close"], "spot": row["official_close"],
                        "valid": True, "source_verified": True, "source": row["source"],
                        "source_sha256": row["source_artifact_sha256"], "close_observation_id": row["id"],
                    })
    return histories, diagnostics
