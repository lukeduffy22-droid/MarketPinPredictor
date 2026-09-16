"""Sampled, diagnostic-only post-close quotes; never ORB or forecast inputs."""
from datetime import datetime
import json
from pathlib import Path


def append_quote_sample(directory: Path, window: dict, quotes: dict, *, epoch: str,
                        generation: int) -> dict:
    if window.get("state") != "post_close_research":
        raise ValueError("post-close research window required")
    start = datetime.fromisoformat(window["cash_close_utc"])
    end = datetime.fromisoformat(window["collection_close_utc"])
    observed = datetime.fromisoformat(window["observed_at_utc"])
    if not start <= observed < end:
        raise ValueError("observation outside collection window")
    lower, upper, cutoff = (int(t.timestamp() * 1e9) for t in (start, end, observed))
    retained = []
    for symbol, quote in quotes.items():
        event, received = quote.get("ts_event_ns"), quote.get("ts_recv_ns")
        if (quote.get("generation") != generation
                or not isinstance(event, int) or not isinstance(received, int)
                or not lower <= event <= received <= cutoff or received >= upper
                or cutoff - received > 30_000_000_000):
            continue
        retained.append({"symbol": symbol, **quote})
    record = {
        "schema_version": "marketpin-post-close-quotes.v1",
        "scope": "diagnostic_research_only", "usable_for_prediction": False,
        "sampling": "latest accepted quote per symbol, at most once per 5 seconds",
        "is_complete_trade_or_quote_tape": False,
        "contract_trading_eligibility": "not_asserted",
        "subscription_epoch_id": epoch, "subscription_generation": generation,
        "session": dict(window), "quote_count": len(retained),
        "excluded_quote_count": len(quotes) - len(retained), "quotes": retained,
    }
    directory.mkdir(parents=True, exist_ok=True)
    # Partition by the existing session date, not the machine's local timezone.
    path = directory / (window["trading_date"] + ".ndjson")
    with path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(record, allow_nan=False, separators=(",", ":")) + "\n")
    return record
