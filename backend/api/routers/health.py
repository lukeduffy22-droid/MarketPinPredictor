"""Health and system endpoints."""

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from backend.api.helpers import payload_health, streamer_health
from backend.api.schemas import HealthResponse
from backend.config import LIVE_DATA_STALE_AFTER_SECONDS, MARKET_DATA_PROVIDER, SYMBOLS
from backend.inference import get_inference_engine
from backend.streamer import get_streamer

router = APIRouter(tags=["health"])


@router.get(
    "/",
    response_model=HealthResponse,
    summary="Get backend health overview",
    description="Returns high-level API health, CUDA availability, loaded models, and stream status.",
)
async def health_check():
    """Health check endpoint."""
    import torch

    engine = get_inference_engine()
    streamer = get_streamer()
    stats = engine.get_stats()

    return HealthResponse(
        status="healthy",
        version="2.0.0",
        cuda_available=torch.cuda.is_available(),
        models_loaded=stats.get("models_loaded", []),
        streaming_active=streamer.is_running,
        uptime_seconds=0.0,  # TODO: track uptime
    )


@router.get(
    "/health",
    summary="Get dashboard health payload",
    description="Compatibility health payload used by the Streamlit dashboard and service checks.",
)
async def health_status():
    """Compatibility health endpoint used by the Streamlit dashboard."""
    streamer = get_streamer()
    health = streamer_health(streamer)
    stream_active = bool(streamer.is_running)
    has_recent_payload = bool(streamer.get_all_latest())
    status = "healthy" if stream_active and has_recent_payload else "degraded"
    health.update(
        {
            "status": status,
            "version": "2.0.0",
            "market_data_provider": health.get("provider", MARKET_DATA_PROVIDER),
            "streaming_active": stream_active,
            "uptime": "active" if stream_active else "stopped",
        }
    )
    return health


@router.get(
    "/health/live",
    summary="Get institutional live-data health",
    description="Returns provider connectivity, payload freshness, and prediction readiness signals.",
)
async def health_live_status():
    streamer = get_streamer()
    health = streamer_health(streamer)
    latest_by_symbol = streamer.get_all_latest()

    most_recent_symbol = None
    most_recent_age = None
    symbol_status: dict[str, dict] = {}
    stale_symbols: list[str] = []

    configured_symbols = health.get("symbols_requested") or SYMBOLS
    for symbol in sorted(set(configured_symbols) | set(latest_by_symbol.keys())):
        payload = latest_by_symbol.get(symbol)
        detail = payload_health(payload, LIVE_DATA_STALE_AFTER_SECONDS)
        symbol_status[symbol] = detail
        age = detail.get("data_age_seconds")
        if isinstance(age, (float, int)) and (most_recent_age is None or float(age) < most_recent_age):
            most_recent_age = float(age)
            most_recent_symbol = symbol
        if detail.get("is_stale"):
            stale_symbols.append(symbol)

    websocket_state = str(health.get("websocket", "unknown")).lower()
    stream_connected = websocket_state == "active"
    prediction_pipeline_ok = stream_connected and not stale_symbols

    return {
        "status": "healthy" if prediction_pipeline_ok else "degraded",
        "provider": health.get("provider", MARKET_DATA_PROVIDER),
        "stream_connected": stream_connected,
        "prediction_pipeline_ok": prediction_pipeline_ok,
        "messages_received": health.get("messages_received", 0),
        "last_tick_age_ms": None if most_recent_age is None else int(most_recent_age * 1000),
        "most_recent_symbol": most_recent_symbol,
        "stale_after_seconds": LIVE_DATA_STALE_AFTER_SECONDS,
        "stale_symbols": stale_symbols,
        "symbol_status": symbol_status,
        "last_update_utc": health.get("last_update_utc"),
        "last_error": health.get("last_error"),
    }


@router.get(
    "/performance",
    summary="Get inference performance statistics",
    description="Returns current inference engine performance metrics and runtime stats.",
)
async def get_performance_stats():
    """Get performance statistics."""
    engine = get_inference_engine()
    stats = engine.get_stats()
    return JSONResponse(content=stats)


@router.get(
    "/symbols",
    summary="List tracked symbols",
    description="Returns configured market symbols tracked by the backend.",
)
async def get_available_symbols():
    """Get list of available symbols."""
    return {"symbols": SYMBOLS}
