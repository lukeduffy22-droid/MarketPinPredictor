"""Backend-authoritative Databento symbols for the Streamlit selector."""

from __future__ import annotations

from typing import Any, Mapping

import requests


STABLE_LIVE_SYMBOLS = ("SPX", "NDX", "VIX")
CANARY_CANDIDATE_SYMBOLS = (
    "SPY",
    "QQQ",
    "DIA",
    "IWM",
    "XSP",
    "XND",
    "RUT",
    "MRUT",
    "OEX",
    "DJX",
    "RUI",
    "XAU",
    "HGX",
    "OSX",
    "UTY",
)


def fetch_databento_universe(timeout_seconds: float = 1.0) -> dict[str, Any]:
    """Fetch the existing stream's universe contract without mutating it."""
    try:
        response = requests.get(
            "http://localhost:8000/databento/universe",
            timeout=timeout_seconds,
        )
        if response.status_code == 200:
            payload = response.json()
            return payload if isinstance(payload, dict) else {}
    except (requests.RequestException, ValueError):
        pass
    return {}


def selectable_databento_symbols(
    universe_payload: Mapping[str, Any] | None,
) -> tuple[str, ...]:
    """Expose optional symbols only when the backend selected live contracts.

    The stable set remains visible while the backend is warming or unreachable.
    This function never changes the subscription; it only mirrors evidence from
    the existing Databento stream.
    """
    selected = set(STABLE_LIVE_SYMBOLS)
    payload = universe_payload or {}
    statuses = payload.get("market_subscription_status") or {}
    if isinstance(statuses, Mapping):
        for raw_symbol, raw_status in statuses.items():
            symbol = str(raw_symbol or "").strip().upper()
            status = raw_status if isinstance(raw_status, Mapping) else {}
            try:
                contract_count = int(status.get("selected_contract_count") or 0)
            except (TypeError, ValueError):
                contract_count = 0
            if symbol in CANARY_CANDIDATE_SYMBOLS and contract_count > 0:
                selected.add(symbol)
    ordered = (*STABLE_LIVE_SYMBOLS, *CANARY_CANDIDATE_SYMBOLS)
    return tuple(symbol for symbol in ordered if symbol in selected)


def active_canary_symbols(
    universe_payload: Mapping[str, Any] | None,
) -> tuple[str, ...]:
    """Return backend-selected optional symbols in deterministic order."""
    selectable = set(selectable_databento_symbols(universe_payload))
    return tuple(symbol for symbol in CANARY_CANDIDATE_SYMBOLS if symbol in selectable)
