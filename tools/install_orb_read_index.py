"""Install the ORB covering index with bounded SQLite writer-lock exposure.

Defaults to inspection only. Never updates retained evidence or changes gates.
"""
from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import sqlite3
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def install(database: Path, *, apply: bool = False, budget_seconds: float = 1.0,
            trading_date: date | None = None) -> dict:
    if not database.is_file():
        raise ValueError("database must already exist")
    if not 0 < budget_seconds <= 30:
        raise ValueError("budget_seconds must be between 0 and 30")
    from sqlalchemy.schema import CreateIndex
    from sqlalchemy import Index, MetaData
    from sqlalchemy.dialects import sqlite
    from backend.database import OrbReferenceSample

    name = "idx_orb_reference_snapshot_metadata"
    index = next(i for i in OrbReferenceSample.__table__.indexes if i.name == name)
    if trading_date is not None:
        name += "_" + trading_date.strftime("%Y%m%d")
        # Do not attach temporary per-date indexes to the global ORM metadata.
        table = OrbReferenceSample.__table__.to_metadata(MetaData())
        index = Index(name, *(table.c[c.name] for c in index.columns),
                      sqlite_where=table.c.trading_date == trading_date)
    sql = str(CreateIndex(index).compile(dialect=sqlite.dialect()))
    mode = "rw" if apply else "ro"
    connection = sqlite3.connect(database.resolve().as_uri() + f"?mode={mode}",
                                 uri=True, timeout=0.1)
    try:
        existing = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name=?", (name,)
        ).fetchone()
        expected_columns = [c.name for c in index.columns]
        if existing:
            actual_columns = [r[2] for r in connection.execute(f'PRAGMA index_info("{name}")')]
            if actual_columns != expected_columns:
                raise ValueError("existing index has incompatible columns")
            return {"status": "already_present", "index": name}
        if not apply:
            return {"status": "planned", "index": name, "sql": sql}
        started = time.perf_counter()
        deadline = started + budget_seconds
        connection.set_progress_handler(lambda: int(time.perf_counter() >= deadline), 1000)
        try:
            # Explicit transaction ensures an interrupted build cannot leave a
            # partially installed index. Busy writers cause an immediate deferral.
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(sql)
            connection.commit()
        except sqlite3.OperationalError as exc:
            connection.set_progress_handler(None, 0)
            connection.rollback()
            if getattr(exc, "sqlite_errorcode", None) not in (
                sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED, sqlite3.SQLITE_INTERRUPT
            ):
                raise
            return {"status": "deferred", "index": name, "reason": str(exc),
                    "elapsed_seconds": time.perf_counter() - started}
        finally:
            connection.set_progress_handler(None, 0)
        return {"status": "installed", "index": name,
                "elapsed_seconds": time.perf_counter() - started}
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--budget-seconds", type=float, default=1.0)
    parser.add_argument("--trading-date", type=date.fromisoformat,
                        help="Index only this date for bounded live deployment")
    args = parser.parse_args()
    result = install(args.database, apply=args.apply, budget_seconds=args.budget_seconds,
                     trading_date=args.trading_date)
    print(json.dumps(result))
    if result["status"] == "deferred":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
