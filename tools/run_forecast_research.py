"""Run the independent research backend: python tools/run_forecast_research.py."""
from pathlib import Path
import argparse
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--source-database", type=Path, default=ROOT / "data" / "market_data.db",
                        help="Source used by the canonical full-stack launcher; override for an alternate capture database")
    args = parser.parse_args()
    from backend.market_research_service import MarketResearchService
    from backend.research_api import create_research_app
    from backend.market_universe import DEFAULT_TRACKED_SYMBOLS
    from backend.equity_context import get_equity_context_tracker
    import uvicorn
    source = args.source_database.resolve()
    if not source.is_file():
        parser.error("Retained source database does not exist")
    service = MarketResearchService(source, ROOT / "data" / "forecast_research.db",
                                   symbols=DEFAULT_TRACKED_SYMBOLS,
                                   equity_tracker=get_equity_context_tracker())
    uvicorn.run(create_research_app(service), host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
