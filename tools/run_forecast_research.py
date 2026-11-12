"""Run the independent research backend: python tools/run_forecast_research.py."""
from pathlib import Path
import argparse
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def build_service(source_database, journal_database, *, interval_seconds=60, indicator_config=None, comparison_journal=None):
    """Construct without I/O; the optional display collector starts at lifespan."""
    from backend.market_research_service import MarketResearchService
    from backend.market_universe import DEFAULT_TRACKED_SYMBOLS
    # Preflight all worker dependencies without running collection/lifespans.
    from backend import directional_shift, forecast_horizons  # noqa: F401

    source = Path(source_database).resolve()
    if not source.is_file():
        raise ValueError("RETAINED_DATABASE_MISSING")
    tracker = None
    if indicator_config and indicator_config["enabled"]:
        from backend.indicator_capture import IndicatorCapture
        tracker = IndicatorCapture(symbols=indicator_config["symbols"], enabled=True,
                                   db_path=source.parent / "equity_context.db")
    return MarketResearchService(source, journal_database, symbols=DEFAULT_TRACKED_SYMBOLS,
                                 interval_seconds=interval_seconds, indicator_tracker=tracker,
                                 comparison_journal=comparison_journal)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--source-database", type=Path, default=ROOT / "data" / "market_data.db",
                        help="Source used by the canonical full-stack launcher; override for an alternate capture database")
    parser.add_argument("--journal-database", type=Path, default=ROOT / "data" / "forecast_research.db")
    parser.add_argument("--comparison-journal", type=Path, help="Optional existing journal, opened query-only for comparisons")
    parser.add_argument("--interval-seconds", type=float, default=60)
    parser.add_argument("--check", action="store_true", help="Validate imports/paths without starting anything")
    parser.add_argument("--runtime-config", type=Path, default=ROOT / "config" / "forecast_research_runtime.json")
    args = parser.parse_args()
    from backend.research_api import create_research_app
    import uvicorn
    from backend.indicator_capture import load_capture_config
    try:
        indicator_config = load_capture_config(args.runtime_config)
        service = build_service(args.source_database, args.journal_database,
                                interval_seconds=args.interval_seconds, indicator_config=indicator_config,
                                comparison_journal=args.comparison_journal)
    except ValueError as exc:
        parser.error(str(exc))
    if args.check:
        print(json.dumps({"loaded_source_sha256": service.code_fingerprint,
                          "source_identity_sha256": service.source_identity}))
        return
    if indicator_config["enabled"]:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env", override=False)
    uvicorn.run(create_research_app(service), host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
