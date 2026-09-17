"""Admin and scoring endpoints."""

import os
from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, HTTPException

from backend.api.helpers import normalize_symbol
from backend.api.schemas import DatabentoCacheRefreshRequest, EODCloseRequest
from backend.database import (
    get_prediction_score_backlog,
    get_prediction_score_summary,
    score_prediction_snapshots,
    upsert_eod_close,
)
from backend.streamer import get_streamer

router = APIRouter(tags=["admin"])


@router.post(
    "/api/eod-close/{symbol}",
    summary="Ingest official close and score predictions (legacy alias)",
    description="Legacy alias for /eod-close/{symbol}.",
    deprecated=True,
)
@router.post(
    "/eod-close/{symbol}",
    summary="Ingest manual close and optionally request explicit prediction scoring",
    description=(
        "Records an unverified manual close. Scoring is attempted only for explicitly "
        "selected prediction IDs and still requires separately verified close evidence."
    ),
)
async def ingest_eod_close(symbol: str, request: EODCloseRequest):
    """Record an unverified close without broad intraday scoring."""
    normalized = normalize_symbol(symbol)
    if request.close <= 0:
        raise HTTPException(status_code=400, detail="close must be > 0")
    if request.trading_date:
        try:
            trading_day = date.fromisoformat(request.trading_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="trading_date must be YYYY-MM-DD")
    else:
        trading_day = datetime.utcnow().date()

    observed_at = None
    if request.observed_at_utc:
        try:
            observed_at = datetime.fromisoformat(request.observed_at_utc.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(status_code=400, detail="observed_at_utc must be ISO-8601")
    try:
        close_row = upsert_eod_close(
            normalized,
            trading_day,
            request.close,
            request.source,
            source_reference=request.source_reference,
            observed_at_utc=observed_at,
            correction_of_id=request.correction_of_id,
            source_verified=False,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    score = score_prediction_snapshots(
        normalized,
        trading_day,
        prediction_ids=request.prediction_ids,
    )
    return {
        "symbol": normalized,
        "trading_date": trading_day.isoformat(),
        "official_close": close_row.official_close if close_row else request.close,
        "source": request.source,
        "source_verified": False,
        "source_reference": request.source_reference,
        "score": score,
    }


@router.get(
    "/api/prediction-scores",
    summary="Get prediction score summary (legacy alias)",
    description="Legacy alias for /prediction-scores.",
    deprecated=True,
)
@router.get(
    "/prediction-scores",
    summary="Get claim-safe legacy prediction score summary",
    description=(
        "Returns no headline model-accuracy metric from legacy snapshot scores. "
        "Verified-close diagnostics remain available in a separately labeled, "
        "non-training-eligible section; promotion-grade results live in the "
        "closing-tape scorecard."
    ),
)
async def prediction_scores(symbol: Optional[str] = None, trading_date: Optional[str] = None):
    """Return claim-safe metrics with diagnostic scores explicitly separated."""
    parsed_date = None
    if trading_date:
        try:
            parsed_date = date.fromisoformat(trading_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="trading_date must be YYYY-MM-DD")
    return get_prediction_score_summary(normalize_symbol(symbol) if symbol else None, parsed_date)


@router.get(
    "/prediction-score-backlog",
    summary="List legacy predictions ready for explicit verified-close scoring",
    description=(
        "Read-only diagnostic work queue. Returns scoreable prediction IDs and blocked "
        "reasons after verified close evidence exists. It never writes scores, never "
        "changes training eligibility, and never creates a model-accuracy claim."
    ),
)
async def prediction_score_backlog(
    symbol: Optional[str] = None,
    trading_date: Optional[str] = None,
    limit: int = 500,
):
    """Return explicit score requests for verified-close diagnostic feedback."""
    parsed_date = None
    if trading_date:
        try:
            parsed_date = date.fromisoformat(trading_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="trading_date must be YYYY-MM-DD")
    try:
        return get_prediction_score_backlog(
            normalize_symbol(symbol) if symbol else None,
            parsed_date,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/databento/refresh-cache",
    summary="Clear Databento universe cache",
    description=(
        "Destructive maintenance action. Requires explicit confirmation and is refused "
        "while the Databento streamer is running unless force_maintenance is explicitly set."
    ),
)
async def refresh_databento_cache(request: DatabentoCacheRefreshRequest):
    """Clear the universe cache only after explicit, state-aware authorization."""
    if request.confirm is not True:
        raise HTTPException(
            status_code=400,
            detail="Explicit confirmation is required to delete the Databento universe cache.",
        )

    streamer = get_streamer()
    cache_path_getter = getattr(streamer, "_cache_path", None)
    if not callable(cache_path_getter):
        raise HTTPException(status_code=400, detail="Active provider is not Databento")

    stream_state = getattr(streamer, "is_running", None)
    if stream_state is None and not request.force_maintenance:
        raise HTTPException(
            status_code=409,
            detail=(
                "Databento streamer state is unknown; cache deletion is blocked. "
                "Use an explicit force-maintenance request only during controlled maintenance."
            ),
        )
    if bool(stream_state) and not request.force_maintenance:
        raise HTTPException(
            status_code=409,
            detail=(
                "Databento streamer is running; cache deletion is blocked to protect the live universe. "
                "Stop the stream first or use an explicit force-maintenance request outside the dashboard."
            ),
        )

    cache_path = str(cache_path_getter())
    deleted = False
    try:
        if os.path.exists(cache_path):
            os.remove(cache_path)
            deleted = True
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to clear cache: {exc}")
    return {
        "deleted": deleted,
        "cache_file": str(cache_path),
        "restart_required": True,
        "forced_while_running": bool(stream_state) and request.force_maintenance,
        "message": "Databento cache cleared. Restart backend and verify readiness before relying on predictions.",
    }
