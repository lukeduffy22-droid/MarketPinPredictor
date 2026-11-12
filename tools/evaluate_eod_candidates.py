"""Append an immutable, current EOD comparison report; read-only provider/source access."""
from datetime import datetime, timezone
import argparse
import hashlib
import json
from pathlib import Path
import sys
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    from backend.eod_forecast_learning import read_frozen_eod, analyze_eod_forecast
    from backend.forecast_repository import load_research_history
    from backend.market_research_service import research_code_fingerprint
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--journal", type=Path, default=ROOT / "data/forecast_research.db")
    parser.add_argument("--completed-sessions-only", action="store_true",
                        help="Score frozen past sessions against official closes without a live API or current forecast.")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("invalid loopback port")
    if args.output.exists():
        parser.error("output already exists; choose a new receipt path")
    started = datetime.now(timezone.utc)
    payload = None
    if not args.completed_sessions_only:
        with urllib.request.urlopen(f"http://127.0.0.1:{args.port}/v1/research/forecasts?symbols=SPX,NDX&horizon_sessions=0", timeout=4) as response:
            payload = json.load(response)
    retrieved = datetime.now(timezone.utc)
    frozen, audit = read_frozen_eod(args.journal, ["SPX", "NDX"], as_of_utc=retrieved,
                                  include_current_session=args.completed_sessions_only)
    history, reasons = load_research_history(ROOT / "data/market_data.db", ["SPX", "NDX"],
        as_of_utc=retrieved, include_intraday=False)
    report = {"started_at_utc": started.isoformat(), "retrieved_at_utc": retrieved.isoformat(),
        "evaluation_code_sha256": research_code_fingerprint(), "source_database_access": "query_only",
        "journal_read": audit, "source_diagnostics": reasons, "retained_api_payload": payload,
        "symbols": {symbol: analyze_eod_forecast(payload["symbols"][symbol]["forecast"], frozen,
            history[symbol], as_of_utc=retrieved) for symbol in history} if payload else {}}
    if args.completed_sessions_only:
        from backend.eod_retrospective import score_completed_eod
        report["completed_session_scores"] = score_completed_eod(frozen, history,
            symbols=["SPX", "NDX"], as_of_utc=retrieved)
        report["retrospective_code_sha256"] = hashlib.sha256(
            (ROOT / "backend/eod_retrospective.py").read_bytes()).hexdigest()
    canonical = json.dumps(report, sort_keys=True, allow_nan=False, separators=(",", ":"))
    receipt = {"report_sha256": hashlib.sha256(canonical.encode()).hexdigest(), "report": report}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(receipt, handle, indent=2, allow_nan=False)
    print(json.dumps({"receipt": str(args.output.resolve()), "sha256": receipt["report_sha256"],
                      "independent_sessions": report["completed_session_scores"]["independent_sessions"] if args.completed_sessions_only
                          else {s: r.get("independent_sessions", 0) for s, r in report["symbols"].items()},
                      "accuracy_improvement_verified": False}))


if __name__ == "__main__":
    main()
