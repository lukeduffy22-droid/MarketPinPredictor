"""Read-only sampled opening-range and pin-behavior endpoints."""

from __future__ import annotations

from datetime import date, datetime

from fastapi import APIRouter, Query

from ..bounded_runtime_read import bounded_runtime_endpoint
from backend.config import DATABENTO_SUPPORTED_SYMBOLS
from backend.market_structure import get_market_structure_journal
from backend.streamer import get_streamer

router = APIRouter(tags=["market-data"])

DEFAULT_ORB_INDEX_SYMBOLS = ("SPX", "NDX", "VIX", "RUT")


def _configured_symbols(streamer=None) -> tuple[str, ...]:
    streamer = streamer or get_streamer()
    return tuple(
        dict.fromkeys(
            str(symbol).upper()
            for symbol in (getattr(streamer, "symbols", None) or [])
            if str(symbol).strip()
        )
    )


def _requested_symbols(raw: str | None, configured: tuple[str, ...]) -> tuple[str, ...]:
    if raw:
        candidates = [item.strip().upper() for item in raw.split(",") if item.strip()]
    else:
        candidates = [*DEFAULT_ORB_INDEX_SYMBOLS, *configured]
    supported = set(DATABENTO_SUPPORTED_SYMBOLS)
    return tuple(dict.fromkeys(symbol for symbol in candidates if symbol in supported))


def _runtime_context(streamer) -> dict[str, object | None]:
    getter = getattr(streamer, "get_subscription_context", None)
    if callable(getter):
        raw = dict(getter())
    else:
        raw = {
            "subscription_epoch_id": getattr(streamer, "subscription_epoch_id", None),
            "subscription_generation": getattr(streamer, "active_generation", None),
            "handoff_status": getattr(streamer, "handoff_status", None),
        }
    try:
        generation = int(raw.get("subscription_generation"))
    except (TypeError, ValueError, OverflowError):
        generation = None
    return {
        "subscription_epoch_id": raw.get("subscription_epoch_id"),
        "subscription_generation": generation,
        "handoff_status": raw.get("handoff_status"),
    }


def _live_snapshot_kwargs(context: dict[str, object | None]) -> dict[str, object | None]:
    return {
        "active_subscription_epoch_id": context["subscription_epoch_id"],
        "active_subscription_generation": context["subscription_generation"],
        "active_handoff_status": context["handoff_status"],
    }


def _live_snapshots(journal, streamer, symbols, *, configured):
    """Read against one runtime identity and fail closed if it keeps changing."""

    context = _runtime_context(streamer)

    def build(binding):
        kwargs = _live_snapshot_kwargs(binding)
        return {
            symbol: journal.snapshot(
                symbol,
                configured=symbol in configured,
                **kwargs,
            )
            for symbol in symbols
        }

    snapshots = build(context)
    context_after = _runtime_context(streamer)
    if context_after == context:
        return snapshots, context, True

    snapshots = build(context_after)
    final_context = _runtime_context(streamer)
    if final_context == context_after:
        return snapshots, final_context, True

    unstable_context = dict(final_context)
    unstable_context["handoff_status"] = "context_changed"
    return build(unstable_context), final_context, False


@router.get(
    "/orb",
    summary="Get sampled opening ranges for configured markets",
    description=(
        "Returns fail-closed MarketPin reference ORB and gamma/max-pain drift evidence. "
        "Prices are sampled from Databento OPRA put/call-parity calculations, not official OHLC."
    ),
)
@router.get(
    "/v1/orb",
    summary="Get sampled opening ranges for configured markets (v1)",
)
@bounded_runtime_endpoint
def get_all_orb(
    trading_date: date | None = None,
    as_of_utc: datetime | None = None,
    symbols: str | None = Query(
        default=None,
        description="Optional comma-separated Databento market symbols.",
    ),
):
    streamer = get_streamer()
    configured = _configured_symbols(streamer)
    requested = _requested_symbols(symbols, configured)
    journal = get_market_structure_journal()
    live_request = trading_date is None and as_of_utc is None
    runtime_context = None
    runtime_context_stable = None
    if live_request:
        snapshots, runtime_context, runtime_context_stable = _live_snapshots(
            journal,
            streamer,
            requested,
            configured=configured,
        )
    else:
        snapshots = {
            symbol: journal.snapshot(
                symbol,
                trading_date=trading_date,
                as_of_utc=as_of_utc,
                configured=symbol in configured,
            )
            for symbol in requested
        }
    return {
        "schema_version": "marketpin-reference-orb.collection.v2",
        "configured_symbols": list(configured),
        "requested_symbols": list(requested),
        "runtime_binding_applied": live_request,
        "runtime_context_stable": runtime_context_stable,
        "active_runtime_context": runtime_context,
        "symbols": snapshots,
    }


@router.get(
    "/orb/{symbol}",
    summary="Get one sampled opening range",
    description=(
        "Returns retained ORB, capture quality, provenance, and pin/max-pain movement for one symbol."
    ),
)
@router.get(
    "/v1/orb/{symbol}",
    summary="Get one sampled opening range (v1)",
)
@bounded_runtime_endpoint
def get_orb(
    symbol: str,
    trading_date: date | None = None,
    as_of_utc: datetime | None = None,
):
    normalized = symbol.upper().strip()
    streamer = get_streamer()
    configured = _configured_symbols(streamer)
    journal = get_market_structure_journal()
    if trading_date is None and as_of_utc is None:
        snapshots, _runtime, _stable = _live_snapshots(
            journal,
            streamer,
            (normalized,),
            configured=configured,
        )
        return snapshots[normalized]
    return journal.snapshot(
        normalized,
        trading_date=trading_date,
        as_of_utc=as_of_utc,
        configured=normalized in configured,
    )
