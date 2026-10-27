"""Versioned provider timing contracts, independent of price/forecast policy.

CBBO ts_recv denotes an interval end; ts_event denotes the optional last sale.
See https://databento.com/docs/schemas-and-data-formats . No trade timestamps
are synthesized. Legacy evidence keeps its original, stricter interpretation.
"""
from __future__ import annotations

from datetime import datetime, time
import json
from typing import Any, Mapping
from zoneinfo import ZoneInfo

LEGACY_TIMING = "provider-event.v1"
CBBO_TIMING = "databento-cbbo-interval.v1"
# The interval layout is unchanged. Validation policy is versioned separately
# and retained in immutable formula inputs, including each contributing leg.
SAME_DAY_LAST_SALE = "cbbo-last-sale-same-day.v1"
OPTIONAL_LAST_SALE = "cbbo-last-sale-metadata.v2"
CBBO_SCHEMAS = {"cbbo-1s": 1_000_000_000, "cbbo-1m": 60_000_000_000}
CBBO_RTYPES = {"cbbo-1s": 192, "cbbo-1m": 193}
UNDEF_TIMESTAMP = (1 << 64) - 1
EASTERN = ZoneInfo("America/New_York")


def timestamp_ns(value: Any) -> int | None:
    """Accept actual integral timestamps; undefined is represented as None."""
    if value is None or isinstance(value, bool):
        return None
    try:
        result = int(value)
        if result != value or not 0 < result < (1 << 63):
            return None
        return result
    except (TypeError, ValueError, OverflowError):
        return None


def timing_contract(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("timestamp_contract_version", LEGACY_TIMING)
    if value == LEGACY_TIMING and not payload.get("quote_schema"):
        return LEGACY_TIMING
    if value == CBBO_TIMING and payload.get("quote_schema") in CBBO_SCHEMAS:
        return CBBO_TIMING
    return None


def last_sale_policy(payload: Mapping[str, Any]) -> str | None:
    """Read live or persisted policy; old evidence keeps its original rules."""
    formula = payload.get("formula_inputs", payload.get("formula_inputs_json", {}))
    if isinstance(formula, str):
        try:
            formula = json.loads(formula)
        except (ValueError, TypeError):
            return None
    if not isinstance(formula, Mapping):
        return None
    direct = payload.get("last_sale_validation_version")
    retained = formula.get("last_sale_validation_version")
    if direct is not None and retained is not None and direct != retained:
        return None
    value = direct if direct is not None else retained
    if value is None:
        return SAME_DAY_LAST_SALE
    return value if value in (SAME_DAY_LAST_SALE, OPTIONAL_LAST_SALE) else None


def last_sale_identity(payload: Mapping[str, Any]) -> dict[str, str]:
    """New policy gets a distinct sample/window identity; legacy IDs are stable."""
    policy = last_sale_policy(payload)
    return ({"last_sale_validation_version": policy}
            if timing_contract(payload) == CBBO_TIMING and policy == OPTIONAL_LAST_SALE else {})


def cbbo_timestamps_valid(schema: str, event: Any, recv: Any, index: Any,
                          *, policy: str = SAME_DAY_LAST_SALE) -> bool:
    if policy not in (SAME_DAY_LAST_SALE, OPTIONAL_LAST_SALE):
        return False
    interval = CBBO_SCHEMAS.get(schema)
    receive = timestamp_ns(recv)
    observation = timestamp_ns(index)
    trade = timestamp_ns(event)
    if interval is None or receive is None or observation != receive or receive % interval:
        return False
    if event is not None and event != UNDEF_TIMESTAMP and trade is None:
        return False
    if trade is None:
        return True
    if policy == OPTIONAL_LAST_SALE:
        # A last-sale timestamp is metadata, not the age of the bid/ask interval.
        # Keep it verbatim. Future or malformed trades remain invalid; callers
        # still enforce current-session interval freshness and mapping identity.
        return trade <= receive
    # The provider defines an undefined last sale when none exists this session.
    # Pre-cash-open trades are permitted, but previous-day trade evidence is not.
    return (trade <= receive and
            datetime.fromtimestamp(trade / 1e9, EASTERN).date() ==
            datetime.fromtimestamp(receive / 1e9, EASTERN).date())


def cbbo_record_timing_valid(schema: str, record: Any,
                             *, policy: str = SAME_DAY_LAST_SALE) -> bool:
    """Bind the schema contract to the actual SDK record type."""
    return (schema in CBBO_RTYPES and getattr(record, "rtype", None) == CBBO_RTYPES[schema]
            and cbbo_timestamps_valid(schema, getattr(record, "ts_event", None),
                                      getattr(record, "ts_recv", None),
                                      getattr(record, "ts_index", None), policy=policy))


def timestamp_bounds_valid(payload: Mapping[str, Any]) -> bool:
    contract = timing_contract(payload)
    first_recv = timestamp_ns(payload.get("earliest_ts_recv_ns"))
    last_recv = timestamp_ns(payload.get("latest_ts_recv_ns"))
    index = timestamp_ns(payload.get("observation_index_ns"))
    first_event = timestamp_ns(payload.get("earliest_ts_event_ns"))
    last_event = timestamp_ns(payload.get("latest_ts_event_ns"))
    if contract is None or first_recv is None or last_recv is None or index is None:
        return False
    if first_recv > last_recv:
        return False
    if contract == LEGACY_TIMING:
        return (first_event is not None and last_event is not None and
                first_event <= last_event <= last_recv)
    count = payload.get("trade_timestamp_count")
    total = payload.get("contributing_quote_count")
    policy = last_sale_policy(payload)
    if policy is None:
        return False
    if (type(count) is not int or type(total) is not int or not 0 <= count <= total):
        return False
    if index != last_recv or not all(
        cbbo_timestamps_valid(payload["quote_schema"], None, value, value)
        for value in (first_recv, last_recv)
    ):
        return False
    if count == 0:
        return (payload.get("earliest_ts_event_ns") is None and
                payload.get("latest_ts_event_ns") is None)
    return (first_event is not None and last_event is not None and
            first_event <= last_event <= last_recv and
            cbbo_timestamps_valid(payload["quote_schema"], first_event, last_recv, last_recv, policy=policy) and
            cbbo_timestamps_valid(payload["quote_schema"], last_event, last_recv, last_recv, policy=policy))


def cbbo_formula_timing_valid(payload: Mapping[str, Any], pairs: Any) -> bool:
    """Recheck every contributing interval and its optional trade, not just flags."""
    if timing_contract(payload) != CBBO_TIMING or not isinstance(pairs, list) or not pairs:
        return False
    policy = last_sale_policy(payload)
    if policy is None:
        return False
    events, receives, indexes = [], [], []
    for pair in pairs:
        if not isinstance(pair, Mapping):
            return False
        for side in ("call", "put"):
            if pair.get(f"{side}_last_sale_validation_version", SAME_DAY_LAST_SALE) != policy:
                return False
            event = pair.get(f"{side}_ts_event_ns")
            raw_key = f"{side}_raw_ts_event_ns"
            if raw_key not in pair:
                return False
            raw_event = pair[raw_key]
            if raw_event not in (None, UNDEF_TIMESTAMP) and timestamp_ns(raw_event) is None:
                return False
            if timestamp_ns(raw_event) != event:
                return False
            recv = pair.get(f"{side}_ts_recv_ns")
            index = pair.get(f"{side}_ts_index_ns")
            if not cbbo_timestamps_valid(payload["quote_schema"], event, recv, index, policy=policy):
                return False
            observed = datetime.fromtimestamp(recv / 1e9, EASTERN)
            if not time(9, 30) <= observed.time() < time(16):
                return False
            receives.append(recv)
            indexes.append(index)
            if event is not None:
                # Serialized normalized evidence must not retain a sentinel as a trade.
                if timestamp_ns(event) is None:
                    return False
                events.append(event)
    return all(payload.get(key) == value for key, value in {
        "earliest_ts_event_ns": min(events) if events else None,
        "latest_ts_event_ns": max(events) if events else None,
        "earliest_ts_recv_ns": min(receives),
        "latest_ts_recv_ns": max(receives),
        "observation_index_ns": max(indexes),
        "trade_timestamp_count": len(events),
        "contributing_quote_count": len(receives),
    }.items()) and timestamp_bounds_valid(payload)
