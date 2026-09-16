"""Profile current ORB projections against SQLite using enforced read-only I/O."""
from __future__ import annotations

import argparse
import cProfile
from contextlib import closing
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import pstats
import sqlite3
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--symbols", default="SPX,NDX")
    parser.add_argument("--as-of", default=None)
    args = parser.parse_args()
    if not args.database.is_file():
        parser.error("database must exist")
    if args.output.exists():
        parser.error("output must not already exist")
    from sqlalchemy import create_engine, event
    from sqlalchemy.orm import sessionmaker
    import backend.database as database
    from backend.market_structure import MarketStructureJournal

    def connect():
        connection = sqlite3.connect(args.database.resolve().as_uri() + "?mode=ro", uri=True,
                                     timeout=1, check_same_thread=False)
        connection.execute("PRAGMA query_only=ON")
        return connection

    engine = create_engine("sqlite://", creator=connect)
    database.engine = engine
    database.SessionLocal = sessionmaker(bind=engine)
    queries = []

    @event.listens_for(engine, "before_cursor_execute")
    def before(_connection, _cursor, statement, parameters, context, _many):
        context.orb_profile_started = time.perf_counter()

    @event.listens_for(engine, "after_cursor_execute")
    def after(_connection, _cursor, statement, parameters, context, _many):
        queries.append({"sql": statement, "parameters": parameters,
                        "execute_seconds": time.perf_counter() - context.orb_profile_started})

    as_of = datetime.fromisoformat(args.as_of) if args.as_of else datetime.now(timezone.utc)
    if as_of.tzinfo is None:
        parser.error("as-of must have a timezone")
    report = {"as_of_utc": as_of.isoformat(), "read_only": True, "symbols": {}}
    for symbol in args.symbols.split(","):
        timings = {}

        def timed(name, loader):
            def load(*a, **kw):
                started = time.perf_counter()
                rows = loader(*a, **kw)
                timings[name] = {"seconds": time.perf_counter() - started, "rows": len(rows)}
                return rows
            return load

        journal = MarketStructureJournal(
            reference_loader=timed("references", database.load_orb_reference_snapshot_samples),
            loader=timed("structure", database.load_market_structure_observations))
        queries.clear()
        profile = cProfile.Profile()
        started = time.perf_counter()
        profile.enable()
        snapshot = journal.snapshot(symbol.strip(), as_of_utc=as_of)
        profile.disable()
        elapsed = time.perf_counter() - started
        output = io.StringIO()
        pstats.Stats(profile, stream=output).sort_stats("cumulative").print_stats(22)
        plans = []
        with closing(connect()) as connection:
            for query in queries:
                plan = connection.execute("EXPLAIN QUERY PLAN " + query["sql"], query["parameters"]).fetchall()
                plans.append({"execute_seconds": query["execute_seconds"], "plan": plan})
        report["symbols"][symbol] = {
            "elapsed_seconds": elapsed, "loaders": timings, "queries": plans,
            "profile": output.getvalue(), "snapshot": snapshot,
        }
        print(json.dumps({"symbol": symbol, "seconds": elapsed, "loaders": timings, "queries": plans}))
        print(output.getvalue())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as output_file:
        json.dump(report, output_file, indent=2, default=str)
    engine.dispose()


if __name__ == "__main__":
    main()
