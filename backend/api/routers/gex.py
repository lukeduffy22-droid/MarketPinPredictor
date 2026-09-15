"""Market data and GEX endpoints."""

from datetime import datetime

from fastapi import APIRouter, HTTPException
from backend.api.bounded_runtime_read import bounded_runtime_endpoint

from backend.api.helpers import (
    current_buffered_payload,
    normalize_symbol,
    payload_data_age_seconds,
    runtime_bound_workstation_state,
    streamer_health,
    with_gex_level_semantics,
)
from backend.api.schemas import MarketDataResponse, WorkstationStateV1
from backend.config import LIVE_DATA_STALE_AFTER_SECONDS
from backend.database import save_prediction_snapshot  # legacy monkeypatch compatibility
from backend.streamer import get_streamer
from backend.workstation import (
    PREDICTIVE_PIN_PAYLOAD_FIELDS,
    build_workstation_state,
    legacy_dashboard_payload,
    sanitize_pin_payload,
    unavailable_workstation_state,
    workstation_state_store,
)

router = APIRouter(tags=["market-data"])

_PREDICTIVE_NUMERIC_FIELDS = PREDICTIVE_PIN_PAYLOAD_FIELDS


def _unpublished_gex_payload(streamer: object, symbol: str) -> tuple[dict | None, dict]:
    """Return diagnostic market evidence without manufacturing a GET-time forecast."""

    payload, binding = current_buffered_payload(streamer, symbol)
    if payload is None:
        return None, binding
    diagnostic = sanitize_pin_payload(payload) or {}
    diagnostic["usable"] = False
    diagnostic["usable_for_prediction"] = False
    diagnostic["decision_grade"] = False
    diagnostic["forecast_state"] = "UNAVAILABLE"
    diagnostic["validation_failure_reasons"] = list(
        dict.fromkeys(
            [
                *list(diagnostic.get("validation_failure_reasons") or []),
                "WAITING_FOR_LIFECYCLE_PUBLICATION",
            ]
        )
    )
    return diagnostic, binding


@router.get(
    "/market/{symbol}",
    response_model=MarketDataResponse,
    summary="Get latest market quote",
    description=(
        "Returns the most recent buffered quote only when it matches the active "
        "streamer process epoch, generation, and handoff and remains within the "
        "configured live-data freshness window."
    ),
)
@bounded_runtime_endpoint
def get_market_data(symbol: str):
    """Get latest market data for a symbol."""
    symbol = symbol.upper()

    streamer = get_streamer()
    data, runtime_binding = current_buffered_payload(streamer, symbol)

    if data is None and runtime_binding.get("payload_present"):
        reasons = runtime_binding.get("failure_reasons") or [
            "ACTIVE_RUNTIME_BINDING_UNAVAILABLE"
        ]
        raise HTTPException(
            status_code=503,
            detail=f"No current-runtime market quote for {symbol}: {reasons[0]}",
        )
    if data is None:
        raise HTTPException(status_code=404, detail=f"No data available for {symbol}")

    if (
        data.get("validation_is_valid") is not True
        or data.get("gamma_excluded_from_model") is not False
    ):
        reasons = data.get("validation_failure_reasons") or [
            "latest buffered observation is not prediction-eligible"
        ]
        raise HTTPException(
            status_code=409,
            detail=f"No validated market quote for {symbol}: {reasons[0]}",
        )

    timestamp_value = data.get("timestamp") or data.get("timestamp_utc")
    if isinstance(timestamp_value, str):
        try:
            timestamp = datetime.fromisoformat(timestamp_value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise HTTPException(
                status_code=503,
                detail=f"Latest validated quote timestamp is malformed for {symbol}",
            ) from exc
    elif isinstance(timestamp_value, datetime):
        timestamp = timestamp_value
    else:
        raise HTTPException(
            status_code=503,
            detail=f"Latest validated quote timestamp is missing for {symbol}",
        )

    age = payload_data_age_seconds(data)
    if age is None:
        raise HTTPException(
            status_code=503,
            detail=f"Latest validated quote freshness is unavailable for {symbol}",
        )
    if age > LIVE_DATA_STALE_AFTER_SECONDS:
        raise HTTPException(
            status_code=503,
            detail=(
                f"Latest validated quote is stale for {symbol} "
                f"(age={age:.1f}s, limit={LIVE_DATA_STALE_AFTER_SECONDS:.1f}s)"
            ),
        )

    return MarketDataResponse(
        symbol=symbol,
        price=data["price"],
        timestamp=timestamp.isoformat(),
        data_age_seconds=age,
    )


@router.get(
    "/buffer/latest/{symbol}",
    summary="Get latest buffered payload",
    description=(
        "Returns the latest active-runtime cached payload without opening a live socket; "
        "retained cross-runtime buffers are never presented as current."
    ),
)
@bounded_runtime_endpoint
def get_buffer_latest(symbol: str):
    """Return latest cached market/pin data for Streamlit without opening sockets."""
    symbol = symbol.upper()
    streamer = get_streamer()
    data, runtime_binding = current_buffered_payload(streamer, symbol)
    if data is None and runtime_binding.get("payload_present"):
        reasons = runtime_binding.get("failure_reasons") or [
            "ACTIVE_RUNTIME_BINDING_UNAVAILABLE"
        ]
        raise HTTPException(
            status_code=503,
            detail=f"No current-runtime cached data for {symbol}: {reasons[0]}",
        )
    if data is None:
        raise HTTPException(status_code=404, detail=f"No cached data available for {symbol}")
    data = sanitize_pin_payload(data) or {}
    timestamp = data.get("timestamp")
    if hasattr(timestamp, "isoformat"):
        data["timestamp"] = timestamp.isoformat()
    return with_gex_level_semantics(data)


@router.get(
    "/gex/{symbol}",
    summary="Get latest gamma exposure snapshot",
    description=(
        "Returns the latest gamma pin and GEX payload only under exact active-runtime "
        "process provenance."
    ),
)
@router.get(
    "/api/gex/{symbol}",
    summary="Get latest gamma exposure snapshot (legacy alias)",
    description="Legacy alias for /gex/{symbol}.",
    deprecated=True,
)
@bounded_runtime_endpoint
def get_gex(symbol: str):
    """Return latest gamma pin/GEX data when the active provider supports it."""
    normalized = normalize_symbol(symbol)
    streamer = get_streamer()
    published = workstation_state_store.get(normalized)
    if published is not None:
        bound = runtime_bound_workstation_state(streamer, published, symbol=normalized)
        retained_payload = bound.get("pin_payload")
        if isinstance(retained_payload, dict) and retained_payload:
            return with_gex_level_semantics(sanitize_pin_payload(retained_payload) or {})

    payload, binding = _unpublished_gex_payload(streamer, normalized)
    if payload is not None:
        decorated = with_gex_level_semantics(sanitize_pin_payload(payload) or {})
        decorated.update(
            forecast_state="UNAVAILABLE",
            usable_for_prediction=False,
            decision_grade=False,
        )
        return decorated
    if binding.get("payload_present"):
        reasons = binding.get("failure_reasons") or ["ACTIVE_RUNTIME_BINDING_UNAVAILABLE"]
        raise HTTPException(
            status_code=503,
            detail=f"No current-runtime GEX data for {normalized}: {reasons[0]}",
        )
    raise HTTPException(status_code=404, detail=f"No GEX data available for {normalized}")


def _symbol_dashboard_payload(symbol: str) -> dict:
    normalized = normalize_symbol(symbol)
    streamer = get_streamer()
    published = workstation_state_store.get(normalized)
    if published is not None:
        return legacy_dashboard_payload(
            runtime_bound_workstation_state(
                streamer,
                published,
                symbol=normalized,
            )
        )

    # A GET may project current buffered evidence while it waits for the
    # lifecycle producer, but it must never synthesize history/model forecasts.
    payload, _binding = _unpublished_gex_payload(streamer, normalized)
    if payload:
        unpublished = build_workstation_state(
            symbol=normalized,
            payload=payload,
            prediction=None,
            forecast_id=None,
        )
        result = legacy_dashboard_payload(
            runtime_bound_workstation_state(
                streamer,
                unpublished,
                symbol=normalized,
            )
        )
        pin_payload = result.get("pin_payload")
        if isinstance(pin_payload, dict):
            for field in _PREDICTIVE_NUMERIC_FIELDS:
                pin_payload.pop(field, None)
        prediction = result.get("prediction")
        if isinstance(prediction, dict):
            for field in _PREDICTIVE_NUMERIC_FIELDS:
                prediction.pop(field, None)
        return result

    return legacy_dashboard_payload(unavailable_workstation_state(normalized))


@router.get(
    "/dashboard/symbol/{symbol}",
    summary="Get unified symbol dashboard payload",
    description="Returns prediction, pin payload, and freshness diagnostics in one response for frontend dashboards.",
)
@router.get(
    "/v1/dashboard/symbol/{symbol}",
    summary="Get unified symbol dashboard payload (v1)",
    description="Versioned alias for /dashboard/symbol/{symbol}.",
)
@bounded_runtime_endpoint
def get_symbol_dashboard(symbol: str):
    return _symbol_dashboard_payload(symbol)


@router.get(
    "/v1/workstation/state/{symbol}",
    response_model=WorkstationStateV1,
    summary="Get lifecycle-published workstation state",
    description=(
        "Returns versioned read-only state. Reading never triggers inference or persistence; "
        "forecast_id is present only when the lifecycle has retained a verified immutable passport."
    ),
)
@bounded_runtime_endpoint
def get_workstation_state(symbol: str):
    normalized = normalize_symbol(symbol)
    return runtime_bound_workstation_state(
        get_streamer(),
        workstation_state_store.get(normalized),
        symbol=normalized,
    )


@router.get(
    "/databento/universe",
    summary="Get Databento universe summary",
    description="Returns subscribed contracts, root counts, and core symbol coverage for Streamlit diagnostics.",
)
@router.get(
    "/v1/databento/universe",
    summary="Get Databento universe summary (v1)",
    description="Versioned alias for /databento/universe.",
)
async def get_databento_universe():
    from backend.api.bounded_runtime_read import read_runtime_or_503

    health = await read_runtime_or_503(
        "databento-universe", lambda: streamer_health(get_streamer())
    )
    return {
        "provider": health.get("provider", "unknown"),
        "schema": health.get("schema"),
        "symbols_requested": health.get("symbols_requested") or [],
        "symbols_subscribed": int(health.get("symbols_subscribed") or 0),
        "subscribed_symbols": health.get("subscribed_symbols") or [],
        "root_contract_counts": health.get("root_contract_counts") or {},
        "core_symbol_status": health.get("core_symbol_status") or {},
        "market_subscription_status": health.get("market_subscription_status") or {},
        "last_update_utc": health.get("last_update_utc"),
        "websocket": health.get("websocket"),
        "buffer_health": health.get("buffer_health"),
    }
