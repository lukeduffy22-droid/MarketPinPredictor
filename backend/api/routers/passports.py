"""Read-only Decision Passport and grounded analyst endpoints."""
import asyncio
import ipaddress
import os
import secrets
import threading
import time
from collections import defaultdict, deque

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, ConfigDict, Field, field_validator

from backend.api.schemas import (
    ForecastPassportDetailV1,
    ForecastPassportListV1,
    PassportAnalystResponseV1,
    PassportReplayResponseV1,
)
from backend.passport_analyst import (
    AnalystEvidenceError,
    AnalystServiceError,
    ask_passport,
)
from backend.prediction_passport import (
    PassportIntegrityError,
    list_prediction_passport_summaries,
    passport_detail_envelope,
    read_prediction_passport,
    replay_prediction_passport,
)

_ACCESS_HEADER = APIKeyHeader(name="X-MarketPin-Governance-Token", auto_error=False)
_ANALYST_RATE_LOCK = threading.Lock()
_ANALYST_REQUESTS: dict[str, deque[float]] = defaultdict(deque)


def _governance_access(
    request: Request,
    supplied_token: str | None = Depends(_ACCESS_HEADER),
) -> None:
    expected = str(os.getenv("MARKETPIN_GOVERNANCE_ACCESS_TOKEN") or "").strip()
    if len(expected) < 16:
        raise HTTPException(
            status_code=503,
            detail="Prediction-governance API access is not configured",
        )
    if not supplied_token or not secrets.compare_digest(supplied_token, expected):
        raise HTTPException(status_code=401, detail="Invalid governance access token")
    if os.getenv("MARKETPIN_GOVERNANCE_ALLOW_REMOTE", "0") == "1":
        return
    host = request.client.host if request.client is not None else ""
    try:
        address = ipaddress.ip_address(host)
        if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
            address = address.ipv4_mapped
        is_loopback = address.is_loopback
    except ValueError:
        is_loopback = host.lower() == "localhost"
    if not is_loopback:
        raise HTTPException(
            status_code=403,
            detail="Prediction-governance API is restricted to the local workstation",
        )


def _enforce_analyst_rate_limit(request: Request) -> None:
    try:
        limit = int(os.getenv("MARKETPIN_ANALYST_REQUESTS_PER_MINUTE", "10"))
    except ValueError:
        limit = 10
    limit = max(1, min(limit, 120))
    client_key = request.client.host if request.client is not None else "local"
    now = time.monotonic()
    with _ANALYST_RATE_LOCK:
        events = _ANALYST_REQUESTS[client_key]
        while events and now - events[0] >= 60.0:
            events.popleft()
        if len(events) >= limit:
            raise HTTPException(
                status_code=429,
                detail="Ask MarketPin rate limit exceeded",
            )
        events.append(now)


router = APIRouter(
    prefix="/v1/workstation",
    tags=["prediction-governance"],
    dependencies=[Depends(_governance_access)],
)


class PassportQuestion(BaseModel):
    model_config = ConfigDict(extra="forbid")
    question: str = Field(min_length=1, max_length=1000)

    @field_validator("question")
    @classmethod
    def normalize_question(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("question cannot be blank")
        return normalized


@router.get("/passports", response_model=ForecastPassportListV1)
async def get_passports(symbol: str | None = None, state: str | None = None, limit: int = Query(50, ge=1, le=500)):
    try:
        items = await asyncio.to_thread(
            list_prediction_passport_summaries,
            symbol=symbol,
            state=state,
            limit=limit,
        )
        return {"items": items}
    except PassportIntegrityError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get(
    "/passports/{forecast_id}",
    response_model=ForecastPassportDetailV1,
    response_model_exclude_unset=True,
)
async def get_passport(forecast_id: str):
    try:
        passport = await asyncio.to_thread(read_prediction_passport, forecast_id)
        return passport_detail_envelope(passport)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Forecast passport not found") from exc
    except PassportIntegrityError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post(
    "/passports/{forecast_id}/replay",
    response_model=PassportReplayResponseV1,
)
async def replay_passport(forecast_id: str):
    try:
        return await asyncio.to_thread(replay_prediction_passport, forecast_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Forecast passport not found") from exc
    except PassportIntegrityError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post(
    "/passports/{forecast_id}/ask",
    response_model=PassportAnalystResponseV1,
)
async def ask_marketpin(
    forecast_id: str,
    request: Request,
    question: PassportQuestion,
):
    _enforce_analyst_rate_limit(request)
    try:
        passport = await asyncio.to_thread(read_prediction_passport, forecast_id)
        return await asyncio.to_thread(ask_passport, passport, question.question)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Forecast passport not found") from exc
    except PassportIntegrityError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except AnalystEvidenceError as exc:
        raise HTTPException(
            status_code=502,
            detail="Ask MarketPin rejected an ungrounded model response",
        ) from exc
    except AnalystServiceError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
