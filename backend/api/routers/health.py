"""Health and system endpoints."""

import asyncio
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Header, Query
from fastapi.responses import JSONResponse, StreamingResponse

from backend.api.helpers import (
    payload_health,
    runtime_bound_workstation_event,
    streamer_runtime_context,
    streamer_health,
)
from backend.api.lifecycle import runtime_control_health
from backend.api.bounded_runtime_read import (
    RuntimeReadUnavailable,
    read_runtime_or_503 as _read_or_503,
    runtime_read_unavailable_payload as _unavailable_read_payload,
    runtime_reads as _runtime_reads,
)
from backend.ai_predictor import inference_device
from backend.api.schemas import HealthResponse, WorkstationEventV1
from backend.config import (
    DATABENTO_REQUIRED_SYMBOLS,
    LIVE_DATA_STALE_AFTER_SECONDS,
    MARKET_DATA_PROVIDER,
    SYMBOLS,
)
from backend.inference import get_inference_engine
from backend.opening_code_fingerprint import (
    LOADED_CODE_CAPTURE_SEMANTICS,
    LOADED_CODE_FINGERPRINT_SCHEMA,
    LOADED_CODE_HASH_ALGORITHM,
    OPENING_CRITICAL_SOURCE_PATHS,
)
from backend.streamer import get_streamer
from backend.workstation import workstation_state_store

router = APIRouter(tags=["health"])

SUBSCRIBED_SYMBOLS_DIAGNOSTICS_URL = "/databento/universe"
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_LOADED_CODE_SOURCE_PATHS = OPENING_CRITICAL_SOURCE_PATHS


def _capture_loaded_code_sha256(
    project_root: Path,
    relative_paths: tuple[str, ...],
) -> tuple[tuple[str, str], ...]:
    """Capture immutable, deterministic source hashes exactly once per call."""
    root = project_root.resolve()
    return tuple(
        (
            relative_path,
            hashlib.sha256((root / relative_path).read_bytes()).hexdigest(),
        )
        for relative_path in relative_paths
    )


_LOADED_CODE_CAPTURED_AT_UTC = datetime.now(timezone.utc).isoformat()
_LOADED_CODE_SHA256 = _capture_loaded_code_sha256(
    _PROJECT_ROOT,
    _LOADED_CODE_SOURCE_PATHS,
)


def _loaded_code_fingerprint_payload() -> dict:
    """Serialize the import-time snapshot without rereading source files."""
    return {
        "schema_version": LOADED_CODE_FINGERPRINT_SCHEMA,
        "capture_semantics": LOADED_CODE_CAPTURE_SEMANTICS,
        "captured_at_utc": _LOADED_CODE_CAPTURED_AT_UTC,
        "hash_algorithm": LOADED_CODE_HASH_ALGORITHM,
        "current_on_disk_recomputed": False,
        "current_on_disk_comparison": "not_performed_by_health_endpoint",
        "files": {
            relative_path: {"loaded_at_startup_sha256": digest}
            for relative_path, digest in _LOADED_CODE_SHA256
        },
    }


def _compact_stream_health(health: dict, *, include_symbols: bool) -> dict:
    """Keep high-frequency health polling bounded unless symbols are requested."""
    payload = dict(health)
    subscribed_symbols = payload.pop("subscribed_symbols", None)
    payload["subscribed_symbols_included"] = bool(include_symbols)
    payload["subscribed_symbols_diagnostics_url"] = SUBSCRIBED_SYMBOLS_DIAGNOSTICS_URL
    if include_symbols:
        payload["subscribed_symbols"] = list(subscribed_symbols or [])
    return payload


def _universe_snapshot(health: dict, *, include_symbols: bool) -> dict:
    """Build a compact SSE universe summary with an explicit detail opt-in."""
    payload = {
        "symbols_requested": health.get("symbols_requested") or [],
        "symbols_subscribed": int(health.get("symbols_subscribed") or 0),
        "root_contract_counts": health.get("root_contract_counts") or {},
        "core_symbol_status": health.get("core_symbol_status") or {},
        "market_subscription_status": health.get("market_subscription_status") or {},
        "subscribed_symbols_included": bool(include_symbols),
        "subscribed_symbols_diagnostics_url": SUBSCRIBED_SYMBOLS_DIAGNOSTICS_URL,
    }
    if include_symbols:
        payload["subscribed_symbols"] = health.get("subscribed_symbols") or []
    return payload


def _live_pipeline_snapshot(streamer, health: dict) -> dict:
    """Separate transport, collection, calculation, and prediction readiness."""
    runtime_context_before = streamer_runtime_context(streamer)
    latest_by_symbol = streamer.get_all_latest()
    runtime_context_after = streamer_runtime_context(streamer)
    configured_symbols = sorted(set(health.get("symbols_requested") or SYMBOLS))
    required_symbols = sorted(set(configured_symbols).intersection(DATABENTO_REQUIRED_SYMBOLS))
    if not required_symbols:
        # A deliberately different deployment (for example a one-symbol canary)
        # still needs an explicit readiness set rather than vacuous success.
        required_symbols = list(configured_symbols)
    optional_symbols = sorted(set(configured_symbols).difference(required_symbols))
    symbols_to_check = sorted(set(configured_symbols) | set(latest_by_symbol.keys()))
    raw_active_generation = health.get("active_generation")
    try:
        active_generation = (
            int(raw_active_generation)
            if not isinstance(raw_active_generation, bool)
            else 0
        )
    except (TypeError, ValueError, OverflowError):
        active_generation = 0
    active_generation_valid = active_generation > 0
    active_epoch_id = str(health.get("subscription_epoch_id") or "").strip()
    active_epoch_valid = bool(
        len(active_epoch_id) == 64
        and all(character in "0123456789abcdef" for character in active_epoch_id)
    )
    fresh_quote_counts = health.get("fresh_quote_counts") or {}

    most_recent_symbol = None
    most_recent_age = None
    symbol_status: dict[str, dict] = {}
    stale_symbols: list[str] = []
    invalid_symbols: list[str] = []
    missing_symbols: list[str] = []
    epoch_mismatch_symbols: list[str] = []
    generation_mismatch_symbols: list[str] = []
    zero_fresh_quote_symbols: list[str] = []

    for symbol in symbols_to_check:
        payload = latest_by_symbol.get(symbol)
        detail = payload_health(payload, LIVE_DATA_STALE_AFTER_SECONDS)
        payload_epoch_id = str(payload.get("subscription_epoch_id") or "") if payload else ""
        epoch_is_current = bool(
            active_epoch_valid and payload_epoch_id == active_epoch_id
        )
        raw_payload_generation = (
            payload.get("subscription_generation") if payload else None
        )
        try:
            payload_generation = (
                int(raw_payload_generation)
                if not isinstance(raw_payload_generation, bool)
                else 0
            )
        except (TypeError, ValueError, OverflowError):
            payload_generation = 0
        generation_is_current = bool(
            active_generation_valid
            and payload_generation > 0
            and payload_generation == active_generation
        )
        detail.update(
            {
                "subscription_epoch_id": payload_epoch_id or None,
                "active_subscription_epoch_id": active_epoch_id or None,
                "epoch_is_current": epoch_is_current,
                "subscription_generation": payload_generation or None,
                "active_generation": active_generation or None,
                "generation_is_current": generation_is_current,
                "fresh_quote_count": int(fresh_quote_counts.get(symbol) or 0),
            }
        )
        symbol_status[symbol] = detail
        age = detail.get("data_age_seconds")
        if isinstance(age, (float, int)) and (most_recent_age is None or float(age) < most_recent_age):
            most_recent_age = float(age)
            most_recent_symbol = symbol
        if symbol in configured_symbols and payload is None:
            missing_symbols.append(symbol)
        if symbol in configured_symbols and detail.get("is_stale"):
            stale_symbols.append(symbol)
        if symbol in configured_symbols and not detail.get("usable_for_prediction"):
            invalid_symbols.append(symbol)
        if symbol in configured_symbols and payload is not None and not generation_is_current:
            generation_mismatch_symbols.append(symbol)
        if symbol in configured_symbols and payload is not None and not epoch_is_current:
            epoch_mismatch_symbols.append(symbol)
        if symbol in configured_symbols and symbol in fresh_quote_counts and not fresh_quote_counts.get(symbol):
            zero_fresh_quote_symbols.append(symbol)

    websocket_state = str(health.get("websocket", "unknown")).lower()
    stream_connected = websocket_state == "active"
    stream_progressing = bool(health.get("stream_progressing", False))
    handoff_status = str(health.get("handoff_status") or "unknown").lower()
    handoff_ready = handoff_status == "active"
    runtime_context_epoch = str(
        runtime_context_before.get("subscription_epoch_id") or ""
    ).strip()
    try:
        runtime_context_generation = (
            int(runtime_context_before.get("subscription_generation"))
            if not isinstance(
                runtime_context_before.get("subscription_generation"), bool
            )
            else 0
        )
    except (TypeError, ValueError, OverflowError):
        runtime_context_generation = 0
    runtime_context_handoff = str(
        runtime_context_before.get("handoff_status") or "unknown"
    ).lower()
    runtime_context_stable = bool(
        runtime_context_before == runtime_context_after
        and runtime_context_epoch == active_epoch_id
        and runtime_context_generation == active_generation
        and runtime_context_handoff == handoff_status
    )
    transport_ready = stream_connected and stream_progressing
    required_missing = sorted(set(missing_symbols).intersection(required_symbols))
    required_invalid = sorted(set(invalid_symbols).intersection(required_symbols))
    required_epoch_mismatch = sorted(
        set(epoch_mismatch_symbols).intersection(required_symbols)
    )
    required_generation_mismatch = sorted(
        set(generation_mismatch_symbols).intersection(required_symbols)
    )
    required_zero_fresh = sorted(set(zero_fresh_quote_symbols).intersection(required_symbols))
    required_stale = sorted(set(stale_symbols).intersection(required_symbols))
    optional_degraded_symbols = sorted(
        set(optional_symbols).intersection(
            set(missing_symbols)
            | set(invalid_symbols)
            | set(epoch_mismatch_symbols)
            | set(generation_mismatch_symbols)
            | set(zero_fresh_quote_symbols)
            | set(stale_symbols)
        )
    )

    collection_ready = (
        transport_ready
        and handoff_ready
        and runtime_context_stable
        and not required_zero_fresh
    )
    calculation_ready = (
        active_epoch_valid
        and active_generation_valid
        and runtime_context_stable
        and not required_missing
        and not required_invalid
        and not required_epoch_mismatch
        and not required_generation_mismatch
    )
    prediction_pipeline_ok = collection_ready and calculation_ready and not required_stale
    all_configured_collection_ready = (
        transport_ready
        and handoff_ready
        and runtime_context_stable
        and not zero_fresh_quote_symbols
    )
    all_configured_calculation_ready = (
        active_epoch_valid
        and active_generation_valid
        and runtime_context_stable
        and not missing_symbols
        and not invalid_symbols
        and not epoch_mismatch_symbols
        and not generation_mismatch_symbols
    )
    all_configured_prediction_pipeline_ok = (
        all_configured_collection_ready
        and all_configured_calculation_ready
        and not stale_symbols
    )

    return {
        "status": "healthy" if prediction_pipeline_ok else "degraded",
        "stream_connected": stream_connected,
        "stream_progressing": stream_progressing,
        "handoff_status": handoff_status,
        "subscription_epoch_id": active_epoch_id or None,
        "subscription_epoch_valid": active_epoch_valid,
        "subscription_generation": active_generation or None,
        "active_generation": active_generation or None,
        "subscription_generation_valid": active_generation_valid,
        "runtime_context_stable": runtime_context_stable,
        "transport_ready": transport_ready,
        "collection_ready": collection_ready,
        "calculation_ready": calculation_ready,
        "live_gex_research_pipeline_ok": prediction_pipeline_ok,
        "prediction_pipeline_ok": prediction_pipeline_ok,
        "prediction_pipeline_semantics": "legacy_live_gex_research_not_tcbbo_promotion",
        "required_symbols": required_symbols,
        "optional_symbols": optional_symbols,
        "optional_context_ready": not optional_degraded_symbols,
        "optional_degraded_symbols": optional_degraded_symbols,
        "required_missing_symbols": required_missing,
        "required_invalid_symbols": required_invalid,
        "required_epoch_mismatch_symbols": required_epoch_mismatch,
        "required_generation_mismatch_symbols": required_generation_mismatch,
        "required_zero_fresh_quote_symbols": required_zero_fresh,
        "required_stale_symbols": required_stale,
        "all_configured_collection_ready": all_configured_collection_ready,
        "all_configured_calculation_ready": all_configured_calculation_ready,
        "all_configured_prediction_pipeline_ok": all_configured_prediction_pipeline_ok,
        "last_tick_age_ms": None if most_recent_age is None else int(most_recent_age * 1000),
        "most_recent_symbol": most_recent_symbol,
        "stale_symbols": sorted(set(stale_symbols)),
        "invalid_symbols": sorted(set(invalid_symbols)),
        "missing_symbols": sorted(set(missing_symbols)),
        "epoch_mismatch_symbols": sorted(set(epoch_mismatch_symbols)),
        "generation_mismatch_symbols": sorted(set(generation_mismatch_symbols)),
        "zero_fresh_quote_symbols": sorted(set(zero_fresh_quote_symbols)),
        "symbol_status": symbol_status,
    }


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


def _read_live_health_sample() -> dict:
    """Read health and both identity checks together on a bounded worker."""
    streamer = get_streamer()
    health = dict(streamer_health(streamer))
    return {
        "health": health,
        "pipeline": _live_pipeline_snapshot(streamer, health),
        "stream_active": bool(getattr(streamer, "is_running", False)),
        "quant_inference_device": str(inference_device()),
        "runtime_controls": runtime_control_health(),
        "sampled_at_utc": datetime.now(timezone.utc).isoformat(),
    }


@router.get(
    "/health",
    summary="Get dashboard health payload",
    description=(
        "Compatibility health payload used by the Streamlit dashboard and service checks. "
        "The full contract-symbol list is omitted by default; set include_symbols=true or "
        "use /databento/universe for explicit diagnostics."
    ),
)
async def health_status(include_symbols: bool = False):
    """Compatibility health endpoint used by the Streamlit dashboard."""
    sample = await _read_or_503("live-health", _read_live_health_sample)
    health = dict(sample["health"])
    stream_active = sample["stream_active"]
    pipeline = sample["pipeline"]
    stream_progressing = bool(health.get("stream_progressing", stream_active))
    status = "healthy" if stream_active and pipeline["prediction_pipeline_ok"] else "degraded"
    health.update(
        {
            "status": status,
            "version": "2.0.0",
            "market_data_provider": health.get("provider", MARKET_DATA_PROVIDER),
            "formula_health": health.get("formula_health"),
            "streaming_active": stream_active,
            "stream_progressing": stream_progressing,
            "uptime": "active" if stream_active else "stopped",
            "quant_inference_device": sample["quant_inference_device"],
            "quant_model": "Databento live GEX research estimator",
            "quant_model_authority": "research_only_not_tcbbo_promoted",
            "tcbbo_promotion_status_endpoint": "/closing-tape/readiness",
            "transport_ready": pipeline["transport_ready"],
            "collection_ready": pipeline["collection_ready"],
            "calculation_ready": pipeline["calculation_ready"],
            "prediction_pipeline_ok": pipeline["prediction_pipeline_ok"],
            "runtime_context_stable": pipeline["runtime_context_stable"],
            "runtime_controls": sample["runtime_controls"],
            "sampled_at_utc": sample["sampled_at_utc"],
            "loaded_code_fingerprint": _loaded_code_fingerprint_payload(),
        }
    )
    return _compact_stream_health(health, include_symbols=include_symbols)


@router.get(
    "/health/live",
    summary="Get institutional live-data health",
    description=(
        "Returns provider connectivity, payload freshness, and legacy live-GEX research "
        "readiness. TCBBO promotion readiness is reported separately."
    ),
)
async def health_live_status():
    sample = await _read_or_503("live-health", _read_live_health_sample)
    health = sample["health"]
    pipeline = sample["pipeline"]

    return {
        **pipeline,
        "provider": health.get("provider", MARKET_DATA_PROVIDER),
        "formula_health": health.get("formula_health"),
        "stream_stalled_seconds": health.get("stream_stalled_seconds"),
        "reconnect_attempts": health.get("reconnect_attempts", 0),
        "last_reconnect_utc": health.get("last_reconnect_utc"),
        "last_reconnect_reason": health.get("last_reconnect_reason"),
        "quant_inference_device": sample["quant_inference_device"],
        "quant_model": "Databento live GEX research estimator",
        "quant_model_authority": "research_only_not_tcbbo_promoted",
        "tcbbo_promotion_status_endpoint": "/closing-tape/readiness",
        "messages_received": health.get("messages_received", 0),
        "stale_after_seconds": LIVE_DATA_STALE_AFTER_SECONDS,
        "last_update_utc": health.get("last_update_utc"),
        "last_error": health.get("last_error"),
        "subscription_session_state": health.get("subscription_session_state"),
        "subscription_allowed": health.get("subscription_allowed"),
        "subscription_window": health.get("subscription_window"),
        "runtime_controls": sample["runtime_controls"],
        "sampled_at_utc": sample["sampled_at_utc"],
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


@router.get(
    "/events/live",
    summary="Stream live health and universe updates",
    description=(
        "SSE stream that emits compact health and Databento universe snapshots for lightweight "
        "live dashboards. Set include_symbols=true only for explicit contract diagnostics."
    ),
)
@router.get(
    "/v1/events/live",
    summary="Stream live health and universe updates (v1)",
    description="Versioned alias for /events/live SSE stream.",
)
async def events_live(include_symbols: bool = False):
    async def _event_generator():
        while True:
            try:
                sample = await _runtime_reads.run("live-health", _read_live_health_sample)
            except RuntimeReadUnavailable as exc:
                yield f"event: unavailable\ndata: {json.dumps(_unavailable_read_payload(exc))}\n\n"
                await asyncio.sleep(2)
                continue
            health = sample["health"]
            pipeline = sample["pipeline"]

            event_payload = {
                "timestamp_utc": sample["sampled_at_utc"],
                "health": {
                    "provider": health.get("provider", MARKET_DATA_PROVIDER),
                    "websocket": health.get("websocket"),
                    "buffer_health": health.get("buffer_health"),
                    "messages_received": health.get("messages_received", 0),
                    "quotes_cached": health.get("quotes_cached", 0),
                    "symbols_subscribed": int(health.get("symbols_subscribed") or 0),
                    "schema": health.get("schema"),
                    "data_age_seconds": health.get("data_age_seconds"),
                    "valid_symbols": health.get("valid_symbols") or [],
                    "invalid_symbols": health.get("invalid_symbols") or [],
                    "invalid_reasons": health.get("invalid_reasons") or {},
                    "last_update_utc": health.get("last_update_utc"),
                    "last_error": health.get("last_error"),
                    "formula_health": health.get("formula_health"),
                },
                "universe": _universe_snapshot(health, include_symbols=include_symbols),
                "pipeline": {
                    **pipeline,
                    "stale_after_seconds": LIVE_DATA_STALE_AFTER_SECONDS,
                    "last_reconnect_utc": health.get("last_reconnect_utc"),
                    "last_reconnect_reason": health.get("last_reconnect_reason"),
                },
            }
            yield f"event: snapshot\ndata: {json.dumps(event_payload)}\n\n"
            await asyncio.sleep(2)

    return StreamingResponse(
        _event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get(
    "/v1/workstation/events/snapshot",
    response_model=WorkstationEventV1,
    summary="Get a versioned workstation event snapshot",
    description="Pure-read snapshot used to initialize or explicitly resynchronize a client.",
)
async def workstation_event_snapshot():
    return await _read_or_503(
        "workstation-snapshot",
        lambda: runtime_bound_workstation_event(
            get_streamer(), workstation_state_store.snapshot_event()
        ),
    )


@router.get(
    "/v1/workstation/events",
    summary="Stream lifecycle-published workstation state",
    description=(
        "SSE stream with monotonic sequence IDs. Supply after_sequence or Last-Event-ID to "
        "resume. A cursor outside retained history produces resync_required with a full snapshot."
    ),
)
async def workstation_events(
    after_sequence: int | None = Query(default=None, ge=0),
    last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
):
    cursor = after_sequence
    if cursor is None and last_event_id:
        try:
            cursor = int(str(last_event_id).split(":", 1)[0])
        except ValueError:
            cursor = -1

    async def _event_generator():
        nonlocal cursor
        next_heartbeat_at = asyncio.get_running_loop().time() + 10.0
        while True:
            heartbeat_due = asyncio.get_running_loop().time() >= next_heartbeat_at

            def read_events():
                streamer = get_streamer()
                events = workstation_state_store.events_after(cursor)
                if not events and heartbeat_due:
                    events = [workstation_state_store.heartbeat_event()]
                return [runtime_bound_workstation_event(streamer, event) for event in events]

            try:
                events = await _runtime_reads.run(
                    ("workstation-events", cursor, heartbeat_due), read_events
                )
            except RuntimeReadUnavailable as exc:
                # No ID or cursor advancement: a runtime-unverified lifecycle
                # event must remain available for retry and resynchronization.
                yield f"event: unavailable\ndata: {json.dumps(_unavailable_read_payload(exc))}\n\n"
                await asyncio.sleep(1)
                continue
            for event in events:
                event_sequence = int(event.get("sequence") or 0)
                event_type = str(event.get("event_type") or "update")
                yield (
                    f"id: {event_sequence}\n"
                    f"event: {event_type}\n"
                    f"data: {json.dumps(event)}\n\n"
                )
                cursor = event_sequence
                next_heartbeat_at = asyncio.get_running_loop().time() + 10.0
            # Avoid one blocking ten-second worker per SSE connection. Replay
            # identity still comes exclusively from the lifecycle event store.
            await asyncio.sleep(0.5)

    return StreamingResponse(
        _event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
