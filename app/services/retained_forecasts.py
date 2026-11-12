"""Read-only historical forecast display, separate from live eligibility."""

from __future__ import annotations

from datetime import datetime, timezone
from contextlib import closing
import json
import math
from pathlib import Path
import sqlite3
import time
from zoneinfo import ZoneInfo

from backend.database_target import configured_market_database_path
from backend.workstation import payload_has_fallback_provenance


def read_retained_forecasts(
    database_path: Path, symbols: list[str], now: datetime,
) -> dict:
    """Return today's last accepted periodic estimates, never live authority."""
    now = now.astimezone(timezone.utc)
    day = now.astimezone(ZoneInfo("America/New_York")).date().isoformat()
    rows = []
    deadline = time.monotonic() + 0.5
    try:
        with closing(sqlite3.connect(Path(database_path).resolve().as_uri() + "?mode=ro", uri=True, timeout=0.1)) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=ON")
            conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
            for symbol in dict.fromkeys(str(s).strip().upper() for s in symbols):
                candidates = conn.execute(
                    "SELECT id,symbol,timestamp_utc,predicted_close,subscription_epoch_id,"
                    "subscription_generation,source_payload_json FROM prediction_snapshots "
                    "WHERE symbol=? AND trading_date=? AND provider='databento' "
                    "AND prediction_mode='backend_periodic' AND is_valid=1 "
                    "AND validation_status='valid' ORDER BY timestamp_utc DESC,id DESC LIMIT 20",
                    (symbol, day),
                ).fetchall()
                for row in candidates:
                    try:
                        stamp = datetime.fromisoformat(row["timestamp_utc"].replace("Z", "+00:00"))
                        stamp = stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp.astimezone(timezone.utc)
                        price = float(row["predicted_close"])
                        epoch, generation = row["subscription_epoch_id"], row["subscription_generation"]
                        source = json.loads(row["source_payload_json"])
                        if (stamp > now or stamp.astimezone(ZoneInfo("America/New_York")).date().isoformat() != day
                            or not math.isfinite(price) or price <= 0
                            or not isinstance(epoch, str) or len(epoch) != 64
                            or any(c not in "0123456789abcdef" for c in epoch)
                            or not isinstance(generation, int) or isinstance(generation, bool) or generation <= 0
                            or not isinstance(source, dict)
                            or source.get("validation_is_valid") is not True
                            or source.get("gamma_excluded_from_model") is not False
                            or source.get("symbol", symbol) != symbol
                            or source.get("subscription_epoch_id", epoch) != epoch
                            or source.get("subscription_generation", generation) != generation
                            or payload_has_fallback_provenance(source)):
                            continue
                    except (ValueError, TypeError, AttributeError, OverflowError):
                        continue
                    rows.append({
                        "symbol": symbol, "predicted_close": price,
                        "timestamp_utc": row["timestamp_utc"],
                        "age_seconds": (now - stamp).total_seconds(),
                        "subscription_epoch_id": epoch, "subscription_generation": generation,
                        "snapshot_id": row["id"],
                    })
                    break
    except (sqlite3.Error, OSError, ValueError):
        return {"rows": [], "error": "Saved forecast history could not be read; refresh to retry."}
    return {"rows": rows, "error": None}


def render_retained_forecasts(
    pipeline: dict | None = None, *, symbols: tuple = ("SPX", "NDX"),
    database_path: Path | None = None, now: datetime | None = None,
) -> None:
    """Show retained research estimates without changing any live forecast."""
    import streamlit as st
    from app.utils.display_time import format_display_timestamp, resolve_context_display_timezone
    from app.services.live_data_client import live_pipeline_symbol_is_current

    now = now or datetime.now(timezone.utc)
    st.markdown("**Last accepted EOD forecasts (saved research)**")
    st.caption("Historical estimates only. Refresh updates their age; the original issue time stays unchanged. These are not current validated forecasts.")
    try:
        path = database_path if database_path is not None else configured_market_database_path(Path(__file__).resolve().parents[2])
        result = read_retained_forecasts(path, list(symbols), now)
    except (RuntimeError, ValueError):
        result = {"rows": [], "error": "Saved forecast database is not configured for this view."}
    if result["error"]:
        st.warning(result["error"])
        return
    by_symbol = {row["symbol"]: row for row in result["rows"]}
    display_timezone = resolve_context_display_timezone(st.context)
    for symbol in symbols:
        row = by_symbol.get(symbol)
        if row is None:
            st.caption(f"{symbol}: no accepted forecast saved for today's session.")
            continue
        detail = (pipeline or {}).get("symbol_status", {}).get(symbol) or {}
        ready = live_pipeline_symbol_is_current(pipeline or {}, symbol)
        reasons = detail.get("validation_failure_reasons") or []
        if not ready:
            reason = "; ".join(str(x) for x in reasons) or "current live eligibility is unavailable"
            st.warning(f"{symbol}: live updates paused or unverified — {reason}")
        else:
            st.caption(f"{symbol}: live inputs were eligible at the last status check; the value below is saved history.")
        st.metric(f"{symbol} last accepted research close", f"{row['predicted_close']:,.2f}")
        issued = format_display_timestamp(row["timestamp_utc"], display_timezone, format_string="%Y-%m-%d %H:%M:%S %Z")
        st.caption(f"Issued {issued} | Age {row['age_seconds'] / 60:.1f} min | Saved generation {row['subscription_generation']} | Main GEX estimate")
