from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

from backend.database_target import configured_market_database_path


ET = ZoneInfo("America/New_York")
CT = ZoneInfo("America/Chicago")
UTC = timezone.utc

DEFAULT_OPTION_PARENTS = (
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
DEFAULT_EQUITY_SYMBOLS = ("SPY", "QQQ", "IWM")


@dataclass(frozen=True)
class SubscriptionSpec:
    schema: str
    symbols: tuple[str, ...]
    stype_in: str
    start_utc: datetime

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "symbols": list(self.symbols),
            "stype_in": self.stype_in,
            "start_utc": self.start_utc.astimezone(UTC).isoformat(),
        }


@dataclass(frozen=True)
class FeedSpec:
    name: str
    dataset: str
    subscriptions: tuple[SubscriptionSpec, ...]
    required: bool = True
    asset_class: str = "options"
    contract_multiplier: float = 100.0

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "dataset": self.dataset,
            "subscriptions": [item.to_dict() for item in self.subscriptions],
            "required": self.required,
            "asset_class": self.asset_class,
            "contract_multiplier": self.contract_multiplier,
        }


@dataclass(frozen=True)
class SessionConfig:
    project_root: Path
    trading_date: date
    session_id: str
    feeds: tuple[FeedSpec, ...]
    cash_open_utc: datetime
    analysis_due_utc: datetime
    cash_close_utc: datetime
    stop_due_utc: datetime
    output_dir: Path
    catalog_path: Path
    market_db_path: Path
    status_path: Path
    lock_path: Path
    flush_seconds: float = 10.0
    min_free_bytes: int = 8 * 1024**3
    max_feed_file_bytes: int = 5 * 1024**3

    def to_dict(self) -> dict[str, object]:
        return {
            "project_root": str(self.project_root),
            "trading_date": self.trading_date.isoformat(),
            "session_id": self.session_id,
            "feeds": [feed.to_dict() for feed in self.feeds],
            "cash_open_utc": self.cash_open_utc.isoformat(),
            "analysis_due_utc": self.analysis_due_utc.isoformat(),
            "cash_close_utc": self.cash_close_utc.isoformat(),
            "stop_due_utc": self.stop_due_utc.isoformat(),
            "output_dir": str(self.output_dir),
            "catalog_path": str(self.catalog_path),
            "market_db_path": str(self.market_db_path),
            "status_path": str(self.status_path),
            "lock_path": str(self.lock_path),
            "flush_seconds": self.flush_seconds,
            "min_free_bytes": self.min_free_bytes,
            "max_feed_file_bytes": self.max_feed_file_bytes,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))


def _parse_csv(value: str | None, default: Iterable[str]) -> tuple[str, ...]:
    if not value:
        return tuple(default)
    return tuple(item.strip().upper() for item in value.split(",") if item.strip())


def _market_times(trading_day: date) -> tuple[datetime, datetime, datetime, datetime]:
    # The existing repository calendar is maintained through 2026.  Import it
    # lazily so this capture package stays usable in small standalone tests.
    try:
        from app.utils.time_et import EARLY_CLOSE_ET_DATES, US_MARKET_HOLIDAYS
    except Exception:
        EARLY_CLOSE_ET_DATES = set()
        US_MARKET_HOLIDAYS = set()

    iso_day = trading_day.isoformat()
    if trading_day.weekday() >= 5 or iso_day in US_MARKET_HOLIDAYS:
        raise ValueError(f"{iso_day} is not a configured US cash-market session")

    open_et = datetime.combine(trading_day, time(9, 30), tzinfo=ET)
    close_hour = 13 if iso_day in EARLY_CLOSE_ET_DATES else 16
    close_et = datetime.combine(trading_day, time(close_hour, 0), tzinfo=ET)
    analysis_et = close_et - timedelta(minutes=15)
    # Cboe options commonly continue through 16:15 ET.  Five extra minutes
    # allow final control messages and a graceful DBN close.
    stop_et = close_et + (timedelta(minutes=20) if close_hour == 16 else timedelta(minutes=20))
    return tuple(item.astimezone(UTC) for item in (open_et, analysis_et, close_et, stop_et))


def build_session_config(
    project_root: str | Path,
    *,
    trading_day: date | None = None,
    now: datetime | None = None,
    include_equities: bool = False,
    option_parents: Iterable[str] | None = None,
    equity_symbols: Iterable[str] | None = None,
    session_id: str | None = None,
    flush_seconds: float | None = None,
    stop_due_utc: datetime | None = None,
) -> SessionConfig:
    root = Path(project_root).resolve()
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    market_day = trading_day or current.astimezone(ET).date()
    cash_open, analysis_due, cash_close, configured_stop = _market_times(market_day)
    if stop_due_utc is not None:
        configured_stop = stop_due_utc.astimezone(UTC)

    parents = tuple(option_parents or _parse_csv(os.getenv("CLOSING_TAPE_OPTION_PARENTS"), DEFAULT_OPTION_PARENTS))
    equities = tuple(equity_symbols or _parse_csv(os.getenv("CLOSING_TAPE_EQUITY_SYMBOLS"), DEFAULT_EQUITY_SYMBOLS))
    midnight_utc = datetime.combine(market_day, time.min, tzinfo=UTC)
    # Live replay timestamps cannot be in the future. A pre-open launcher starts
    # capture immediately; analysis still filters the cash-session window.
    trade_replay_start = min(cash_open, current.astimezone(UTC) - timedelta(seconds=5))

    feeds: list[FeedSpec] = [
        FeedSpec(
            name="opra_options",
            dataset="OPRA.PILLAR",
            asset_class="options",
            contract_multiplier=100.0,
            required=True,
            subscriptions=(
                SubscriptionSpec("tcbbo", parents, "parent", trade_replay_start),
                SubscriptionSpec("statistics", parents, "parent", midnight_utc),
                SubscriptionSpec("definition", parents, "parent", midnight_utc),
            ),
        )
    ]
    if include_equities:
        feeds.append(
            FeedSpec(
                name="equity_proxies",
                dataset=os.getenv("CLOSING_TAPE_EQUITY_DATASET", "EQUS.MINI"),
                asset_class="equity",
                contract_multiplier=1.0,
                required=False,
                subscriptions=(SubscriptionSpec("tbbo", equities, "raw_symbol", trade_replay_start),),
            )
        )

    sid = session_id or f"{market_day.isoformat()}-{current.astimezone(UTC).strftime('%H%M%SZ')}"
    output_dir = root / "data" / "closing_tape" / market_day.isoformat()
    return SessionConfig(
        project_root=root,
        trading_date=market_day,
        session_id=sid,
        feeds=tuple(feeds),
        cash_open_utc=cash_open,
        analysis_due_utc=analysis_due,
        cash_close_utc=cash_close,
        stop_due_utc=configured_stop,
        output_dir=output_dir,
        catalog_path=output_dir / "closing_tape.sqlite",
        market_db_path=configured_market_database_path(root),
        status_path=output_dir / "status.json",
        lock_path=output_dir / "recorder.lock",
        flush_seconds=float(flush_seconds or os.getenv("CLOSING_TAPE_FLUSH_SECONDS", "10")),
        min_free_bytes=int(os.getenv("CLOSING_TAPE_MIN_FREE_BYTES", str(8 * 1024**3))),
        max_feed_file_bytes=int(os.getenv("CLOSING_TAPE_MAX_FEED_BYTES", str(5 * 1024**3))),
    )
