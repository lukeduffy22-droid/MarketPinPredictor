"""Bounded EQUS.MINI access check; never activates a subscription or writes bars."""
from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    from backend.forecast_calendar import ET, session_bounds
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=False)
    now = datetime.now(timezone.utc)
    day = now.astimezone(ET).date()
    for _ in range(10):
        try:
            opened, closed = session_bounds(day)
            end = min(now.replace(second=0, microsecond=0), closed)
            if end > opened:
                break
        except ValueError:
            pass
        day -= timedelta(days=1)
    else:
        raise RuntimeError("NO_RECENT_SESSION")
    request = dict(dataset="EQUS.MINI", schema="ohlcv-1m", symbols=["SPY", "QQQ"],
                   start=(end-timedelta(minutes=1)).isoformat(), end=end.isoformat(), limit=4)
    result = {"schema_version": "marketpin-indicator-access-check.v1",
              "retrieval_started_at_utc": now.isoformat(), "request": request,
              "maximum_estimated_cost_usd": 0.01, "status": "NOT_YET_VERIFIED"}
    logging.getLogger("databento").setLevel(logging.ERROR)
    try:
        import databento as db
        client = db.Historical()
        client.metadata.TIMEOUT = client.timeseries.TIMEOUT = 10
        cost = float(client.metadata.get_cost(**request))
        result["estimated_cost_usd"] = cost
        if not 0 <= cost <= 0.01:
            result["reason"] = "COST_CAP_EXCEEDED_OR_INVALID"
        else:
            records = client.timeseries.get_range(**request).to_df()
            result["record_count"] = len(records)
            result["status"] = "OBSERVED" if len(records) else "NOT_YET_VERIFIED"
            result["reason"] = "RECENT_BAR_RETRIEVAL_PASSED" if len(records) else "NO_RECENT_BARS"
    except Exception as exc:
        msg = str(exc).casefold()
        result["reason"] = ("ENTITLEMENT_UNAVAILABLE" if any(s in msg for s in
            ("403", "license_not_found", "live data license")) else "PROVIDER_CHECK_FAILED")
        result["error_type"] = type(exc).__name__
    result["retrieved_at_utc"] = datetime.now(timezone.utc).isoformat()
    folder = ROOT / "output" / "indicator_capture_20261002"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + ".access-check.json")
    with path.open("x", encoding="utf-8") as handle:
        json.dump(result, handle, indent=2)
    path.chmod(0o444)
    print(json.dumps({**result, "receipt": str(path)}))


if __name__ == "__main__":
    main()
