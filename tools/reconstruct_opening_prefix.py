"""Offline, append-only research recovery; never writes or promotes live ORB rows.

Input is an already acquired, hash-bound Databento CBBO receipt. No provider
connection, live clock emulation, interpolation, or live-generation admission.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import sqlite3
import statistics
from datetime import datetime, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

UTC = timezone.utc
NY = ZoneInfo("America/New_York")
CONTRACT = re.compile(r"^(?P<root>[A-Z]+)\s+(?P<date>\d{6})(?P<side>[CP])(?P<strike>\d{8})$")
SCHEMA = "marketpin-retrospective-opening-prefix.v1"


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def utc(value):
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    # SQLite's documented timestamps are UTC without a suffix.
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def evaluate(book, contracts, *, as_of_ns, opening_ns, expiration, policy):
    """Parity from quotes known by as_of; later or stale quotes cannot repair it."""
    legs = {}
    for symbol, quote in book.items():
        spec = contracts.get(symbol)
        if spec is None or spec[1] != expiration:
            continue
        root, _, side, strike = spec
        recv, bid, ask, event, instrument_id = quote
        if not opening_ns < recv <= as_of_ns:
            continue
        if (as_of_ns - recv) / 1e9 > policy["freshness_seconds"]:
            continue
        if event is not None and event > recv:
            continue
        if not all(math.isfinite(v) for v in (bid, ask)) or bid < 0 or ask < bid:
            continue
        if ask >= (2**63 - 1) / 1e9 or (bid + ask) / 2 <= 0:
            continue
        key = (root, strike)
        legs.setdefault(key, {}).setdefault(side, []).append({
            "symbol": symbol, "mid": (bid + ask) / 2,
            "ts_recv_ns": recv, "ts_event_ns": event,
            "instrument_id": instrument_id,
        })
    pairs = []
    for (root, strike), sides in legs.items():
        if len(sides.get("C", [])) == len(sides.get("P", [])) == 1:
            call, put = sides["C"][0], sides["P"][0]
            pairs.append({"root": root, "strike": strike, "call": call, "put": put})
    if len(pairs) < policy["minimum_pairs"]:
        return {"status": "unavailable", "reason": "COMPLETE_PAIR_MINIMUM_NOT_MET", "paired_quotes": len(pairs)}
    pairs.sort(key=lambda p: (abs(p["call"]["mid"] - p["put"]["mid"]), p["root"], p["strike"]))
    selected = pairs[:policy["pair_limit"]]
    cutoff = datetime.combine(datetime.fromisoformat(expiration).date(), time(policy["expiry_hour_et"], policy["expiry_minute_et"]), NY)
    years = max(policy["minimum_tte_years"], (cutoff.timestamp() - as_of_ns / 1e9) / (365 * 86400))
    discount = math.exp(-policy["risk_free_rate"] * years)
    price = statistics.median(p["call"]["mid"] - p["put"]["mid"] + p["strike"] * discount for p in selected)
    return {"status": "reconstructed", "classification": "INFERRED", "reference_price": price,
            "as_of_utc": datetime.fromtimestamp(as_of_ns / 1e9, UTC).isoformat(),
            "paired_quotes": len(pairs), "time_to_expiration_years": years, "pairs": selected}


def coverage(live, reconstructed, opening, minutes, observed_at):
    expected = minutes * 12
    buckets = {int((utc(r["sample_timestamp_utc"]) - opening).total_seconds()) // 5: r["reference_price"] for r in live}
    added = 0
    for bucket, row in reconstructed.items():
        if row["status"] == "reconstructed" and bucket not in buckets:
            buckets[bucket] = row["reference_price"]
            if bucket < expected:
                added += 1
    relevant = {k: v for k, v in buckets.items() if 0 <= k < expected}
    missing = [k for k in range(expected) if k not in relevant]
    elapsed_buckets = min(expected, max(0, int((observed_at-opening).total_seconds())//5))
    finished = (observed_at - opening).total_seconds() >= minutes * 60
    status = "forming" if not finished else ("reconstructed_complete" if not missing else "partial")
    return {"status": status, "classification": "INFERRED", "expected_buckets": expected,
            "covered_buckets": len(relevant), "recovered_buckets": added,
            "first_bucket_available": 0 in relevant, "missing_bucket_offsets_seconds": [5*k for k in missing],
            "missing_elapsed_bucket_offsets_seconds": [5*k for k in missing if k < elapsed_buckets],
            "not_yet_elapsed_buckets": expected-elapsed_buckets,
            "sampled_high": max(relevant.values(), default=None), "sampled_low": min(relevant.values(), default=None),
            "live_capture_complete": False, "usable_for_prediction": False}


def run(receipt_path, database, universe, destination):
    import databento as db
    import pandas as pd
    from dotenv import dotenv_values

    receipt_path, database, universe, destination = map(Path, (receipt_path, database, universe, destination))
    receipt = json.loads(receipt_path.read_text(encoding="utf8"))
    raw = Path(receipt["path"])
    if receipt.get("status") != "downloaded" or digest(raw) != receipt["sha256"]:
        raise ValueError("Unverified download or DBN hash mismatch")
    request = receipt["request"]
    if (request["schema"], request["dataset"], request["stype_in"]) != ("cbbo-1s", "OPRA.PILLAR", "raw_symbol"):
        raise ValueError("Only exact-contract OPRA CBBO-1s receipts are supported")
    start, end = utc(request["start"]), utc(request["end"])
    opening = datetime.combine(start.astimezone(NY).date(), time(9, 30), NY).astimezone(UTC)
    if start != opening or not 0 < (end-start).total_seconds() <= 60:
        raise ValueError("Recovery must start at the exact open and span at most 60 seconds")
    requested = set(request["symbols"])
    store = db.DBNStore.from_file(raw)
    meta = store.metadata
    if (str(meta.dataset), str(meta.schema), int(meta.start), int(meta.end)) != ("OPRA.PILLAR", "cbbo-1s", int(start.timestamp()*1e9), int(end.timestamp()*1e9)):
        raise ValueError("DBN metadata does not match receipt")
    if set(meta.symbols) != requested or meta.not_found:
        raise ValueError("Missing or mismatching contract resolution")
    frame = store.to_df().reset_index()
    if len(frame) != receipt["parsed_rows"] or frame.duplicated(["ts_recv", "symbol"]).any():
        raise ValueError("Record count mismatch or ambiguous duplicate quote")
    definitions = {}
    with universe.open(encoding="utf8", newline="") as f:
        for row in csv.DictReader(f):
            symbol = row["symbol"]
            if symbol in requested:
                match = CONTRACT.fullmatch(symbol)
                if not match or match["root"] != row["_root"] or match["side"] != row["option_type"] or int(match["strike"])/1000 != float(row["strike"]):
                    raise ValueError("Universe contract identity mismatch")
                expiration = datetime.strptime(match["date"], "%y%m%d").date().isoformat()
                if expiration != row["expiration_date"] or symbol in definitions:
                    raise ValueError("Ambiguous universe expiration/contract")
                definitions[symbol] = (match["root"], expiration, match["side"], float(row["strike"]), row["market"])
    if set(definitions) != requested:
        raise ValueError("Universe does not cover exact requested contracts")
    # Read-only connection: no ORM import, migrations, inserts or updates.
    with sqlite3.connect(database.resolve().as_uri()+"?mode=ro", uri=True, timeout=2) as conn:
        conn.row_factory = sqlite3.Row
        live = [dict(r) for r in conn.execute("SELECT * FROM orb_cbbo_reference_samples WHERE trading_date=? AND sample_timestamp_utc>=? AND sample_timestamp_utc<? ORDER BY sample_timestamp_utc,symbol", (start.date().isoformat(), start.strftime("%Y-%m-%d %H:%M:%S"), datetime.fromtimestamp(opening.timestamp()+3600, UTC).strftime("%Y-%m-%d %H:%M:%S")))]
    if not live:
        raise ValueError("No live policy/source evidence to compare")
    observed_at = datetime.now(UTC)
    settings = dotenv_values(Path(__file__).resolve().parents[1]/".env")
    policies, contracts = {}, {}
    for market in sorted({r["symbol"] for r in live}):
        rows = [r for r in live if r["symbol"] == market]
        first = rows[0]
        if len({(r["subscription_epoch_id"], r["subscription_generation"], r["universe_sha256"]) for r in rows}) != 1:
            raise ValueError("Live identity changed; refuse automatic mixed-identity reconstruction")
        formula = json.loads(first["formula_inputs_json"])
        if formula["formula_version"] != "put-call-parity-v2-discounted-strike":
            raise ValueError("Unsupported parity formula")
        # Independently reproduce the persisted arithmetic before using its policy.
        replay = statistics.median(p["call_mid"]-p["put_mid"]+p["strike"]*math.exp(-first["risk_free_rate"]*first["time_to_expiration_years"]) for p in formula["pairs"])
        if abs(replay-first["reference_price"]) > 1e-7:
            raise ValueError("Live arithmetic receipt does not reproduce")
        policies[market] = {"expiration": first["primary_expiration"], "risk_free_rate": first["risk_free_rate"],
            "freshness_seconds": first["quote_freshness_limit_seconds"], "minimum_pairs": first["minimum_paired_quote_count"],
            "pair_limit": formula["pair_limit"], "minimum_tte_years": float(settings.get("DATABENTO_MIN_TTE_DAYS", 1/365)),
            "expiry_hour_et": int(settings.get("DATABENTO_EXPIRY_HOUR_ET", 16)), "expiry_minute_et": int(settings.get("DATABENTO_EXPIRY_MINUTE_ET", 0)),
            "live_policy_sample_id": first["sample_id"]}
        contracts[market] = {s: d[:4] for s,d in definitions.items() if d[4] == market}
    mapping = {}
    for symbol, entries in meta.mappings.items():
        matches = [e for e in entries if e["start_date"] <= start.date() < e["end_date"]]
        if len(matches) != 1:
            raise ValueError("Ambiguous historical instrument mapping")
        mapping[symbol] = int(matches[0]["symbol"])
    book, recovered, first_raw, attempts = {}, {m:{} for m in policies}, {}, []
    for timestamp, group in frame.groupby("ts_recv", sort=True):
        ns = int(timestamp.value)
        if not int(start.timestamp()*1e9) < ns < int(end.timestamp()*1e9) or ns % 1_000_000_000:
            raise ValueError("Interval end outside bounded session or not whole seconds")
        for q in group.itertuples(index=False):
            if q.symbol not in requested or mapping.get(q.symbol) != q.instrument_id:
                raise ValueError("Unbound historical instrument")
            event = None if pd.isna(q.ts_event) else int(q.ts_event.value)
            book[q.symbol] = (ns, float(q.bid_px_00), float(q.ask_px_00), event, int(q.instrument_id))
            first_raw.setdefault(definitions[q.symbol][4], timestamp.isoformat())
        bucket = int((timestamp.to_pydatetime()-opening).total_seconds())//5
        for market, policy in policies.items():
            result = evaluate(book, contracts[market], as_of_ns=ns, opening_ns=int(opening.timestamp()*1e9), expiration=policy["expiration"], policy=policy)
            attempts.append({"symbol": market, "as_of_utc": timestamp.isoformat(), "paired_quotes": result["paired_quotes"], "status": result["status"]})
            if result["status"] == "reconstructed" and bucket not in recovered[market]:
                recovered[market][bucket] = result
    results = {}
    for market in policies:
        rows = [r for r in live if r["symbol"] == market]
        results[market] = {"first_historical_quote_utc": first_raw.get(market), "policy": policies[market],
            "reference_semantics": "forward-like context only" if market == "VIX" else "inferred option parity; not official index OHLC",
            "recovered_prefix": recovered[market], "original_live_first_sample_utc": rows[0]["sample_timestamp_utc"],
            "windows": {str(m): coverage(rows, recovered[market], opening, m, observed_at) for m in (5,15,30,60)}}
    report = {"schema_version": SCHEMA, "trading_date": start.date().isoformat(), "created_at_utc": observed_at.isoformat(),
        "authority": "RETROSPECTIVE_RESEARCH_ONLY", "usable_for_prediction": False, "original_live_records_modified": False,
        "sampling": "Earliest valid historical parity per five-second bucket; original live samples win overlap. No interpolation. Historical quotes never satisfy original live clock, transport or capture gates.",
        "source": {"download_receipt": str(receipt_path.resolve()), "receipt_sha256": digest(receipt_path), "dbn_sha256": digest(raw), "universe_sha256": digest(universe), "parsed_rows": len(frame)},
        "attempts": attempts, "symbols": results}
    destination.mkdir(parents=True, exist_ok=False)
    for name, content in (("live-samples.json", live), ("report.json", report)):
        (destination/name).open("x", encoding="utf8").write(json.dumps(content, indent=2, allow_nan=False))
    lines = [f"# Opening-range recovery — {start.date()}", "",
        f"Checked: {observed_at.astimezone(ZoneInfo('America/Chicago')).strftime('%I:%M:%S %p %Z')}", "",
        "**Historical reconstruction, for research only. The original live capture remains partial.**",
        "This report recovers missing buckets only from quotes timestamped inside those buckets. It does not insert rows into the live database, alter opening gates, or enable breakout signals.", "",
        "| Symbol | First provider quote (Central) | 5 minutes | 15 minutes | 30 minutes | 60 minutes |",
        "|---|---|---|---|---|---|"]
    for market, value in results.items():
        first = value['first_historical_quote_utc']
        values = [market, utc(first).astimezone(ZoneInfo('America/Chicago')).strftime('%I:%M:%S %p') if first else 'Unavailable']
        for minutes in ('5','15','30','60'):
            window = value['windows'][minutes]
            values.append(f"{window['status'].replace('_',' ')} ({window['covered_buckets']}/{window['expected_buckets']})")
        lines.append('| '+' | '.join(values)+' |')
    lines += ['', 'The first quote is not necessarily a valid parity pair or the official index open. SPY was not subscribed for live ORB capture.', '',
        '**OBSERVED:** provider records, instrument mappings, original saved samples, and the dates/times of retrieval. The receipt and raw DBN have SHA-256 hashes.', '',
        '**INFERRED:** reconstructed option-parity values and combined sampled ranges. VIX is forward-like option context, not a spot-index breakout range.', '',
        '**NOT YET VERIFIED:** complete original live capture, official index OHLC, predictive usefulness of reconstruction, and any missing elapsed buckets listed in report.json. A forming 60-minute range is not final.', '',
        'CBBO interval end timestamps are used for availability. Later quotes are never used to populate earlier buckets. Existing live samples win overlapping buckets. No interpolation is performed.']
    (destination/'REPORT.md').open('x', encoding='utf8').write('\n'.join(lines)+'\n')
    manifest = {"created_at_utc": datetime.now(UTC).isoformat(), "files": {p.name: digest(p) for p in destination.iterdir() if p.is_file()}, "tool_sha256": digest(__file__)}
    (destination/"manifest.json").open("x", encoding="utf8").write(json.dumps(manifest, indent=2))
    print(json.dumps({m:{"first_quote":r["first_historical_quote_utc"], "windows":{k:{field:v[field] for field in ('status','covered_buckets','expected_buckets','recovered_buckets')} for k,v in r['windows'].items()}} for m,r in results.items()}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("receipt", "database", "universe", "output"):
        parser.add_argument("--"+key, required=True)
    args = parser.parse_args()
    run(args.receipt, args.database, args.universe, args.output)
