import asyncio
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import threading
import time
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query

from backend.closing_tape.catalog_discovery import (
    catalog_configuration_resolution_id,
)
from backend.closing_tape.status import closing_tape_status
from backend.closing_tape.readiness import audit_training_readiness
from backend.closing_tape.governance import (
    build_closing_tape_scorecard,
    load_governed_promoted_close_predictions,
)
from backend.closing_tape.dataset import PRODUCTION_FAMILIES
from backend.prediction_authority import build_prediction_authority
from backend.database import engine as database_engine
from backend.database_target import sqlite_database_path_from_url


router = APIRouter(prefix="/closing-tape", tags=["closing-tape"])
PROJECT_ROOT = Path(__file__).resolve().parents[3]
READINESS_CACHE_TTL_SECONDS = 300.0
READINESS_CACHE_CONTRACT_VERSION = "closing-tape-readiness-cache.v6"
READINESS_SNAPSHOT_PATH = PROJECT_ROOT / "data" / "closing_tape" / "readiness_snapshot.json"
_readiness_cache_lock = threading.Lock()
_readiness_refresh_in_progress = False
_readiness_refresh_error: str | None = None
_readiness_cache_created_monotonic = 0.0
_NEW_YORK_TIMEZONE = ZoneInfo("America/New_York")


def _configured_api_market_database_path() -> Path:
    """Bind API reads to the same immutable engine target initialized at startup."""
    return sqlite_database_path_from_url(database_engine.url)


def _validate_governed_prediction_response(
    rows: object,
    *,
    requested_date: str,
) -> list[dict[str, object]]:
    if not isinstance(rows, list):
        raise ValueError("governed prediction loader returned a non-list batch")
    if not rows:
        return []
    if len(rows) != len(PRODUCTION_FAMILIES) or any(
        not isinstance(row, dict) for row in rows
    ):
        raise ValueError("governed prediction batch must contain exactly five records")
    typed_rows = rows
    if {str(row.get("family_root") or "") for row in typed_rows} != set(
        PRODUCTION_FAMILIES
    ):
        raise ValueError("governed prediction batch must contain every production family")
    if {str(row.get("trading_date") or "") for row in typed_rows} != {
        requested_date
    }:
        raise ValueError(
            "promoted prediction batch does not match the requested trading date"
        )
    forecast_ids: set[str] = set()
    replay_states: set[tuple[bool, str]] = set()
    for row in typed_rows:
        forecast_id = str(row.get("forecast_id") or "").strip()
        prediction_key = str(row.get("prediction_key") or "").strip()
        if not forecast_id or forecast_id != prediction_key:
            raise ValueError("governed prediction forecast identity is invalid")
        if row.get("validation_state") != "VALID" or row.get("decision_grade") is not True:
            raise ValueError("prediction is not registered as decision-grade governance evidence")
        replay_verified = row.get("deterministic_replay_verified")
        replay_status = str(row.get("replay_status") or "")
        if (
            replay_verified is not True
            or replay_status != "DETERMINISTIC_REPLAY_VERIFIED"
        ):
            raise ValueError(
                "decision-grade prediction lacks deterministic replay verification"
            )
        forecast_ids.add(forecast_id)
        replay_states.add((replay_verified, replay_status))
    if len(forecast_ids) != len(PRODUCTION_FAMILIES):
        raise ValueError("governed prediction forecast identities are not unique")
    if len(replay_states) != 1:
        raise ValueError("governed prediction batch has mixed replay states")
    return typed_rows


def _unavailable_scorecard_payload(reason: str) -> dict[str, object]:
    return {
        "available": False,
        "reason": reason,
        "prediction_mode": "tcbbo_promoted",
        "attempts": {
            "opportunities": 0,
            "predicted": 0,
            "abstained": 0,
            "unresolved": 0,
            "invalid_predicted": 0,
            "availability_rate": None,
            "abstention_rate": None,
            "abstention_reasons": {},
            "scope": "governance evidence unavailable",
        },
        "overall": {
            "metrics_available": False,
            "reason": reason,
            "scored_rows": 0,
            "scored_sessions": 0,
            "candidate_mae": None,
            "candidate_rmse": None,
            "candidate_direction_hit_rate": None,
            "persistence_mae": None,
            "persistence_rmse": None,
            "persistence_direction_hit_rate": None,
            "candidate_mae_improvement_pct": None,
            "candidate_mae_improvement_ci_low_pct": None,
            "candidate_mae_improvement_ci_high_pct": None,
            "interval_target_coverage": None,
            "interval_empirical_coverage": None,
            "interval_coverage_error": None,
            "mean_interval_width_points": None,
            "mean_interval_width_pct": None,
        },
        "by_family": [],
        "by_symbol": [],
        "by_horizon": [],
        "by_regime": [],
        "by_model": [],
        "by_slice": [],
        "outcome_resolution": {
            "eligible_forecasts": 0,
            "resolved_forecasts": 0,
            "unresolved_forecasts": 0,
            "resolution_rate": None,
        },
    }


def _current_trading_date() -> date:
    """Return the New York calendar date without asserting exchange openness."""

    return datetime.now(_NEW_YORK_TIMEZONE).date()


def _load_readiness_snapshot() -> dict[str, object] | None:
    try:
        payload = json.loads(READINESS_SNAPSHOT_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if (
        not isinstance(payload, dict)
        or payload.get("cache_contract_version") != READINESS_CACHE_CONTRACT_VERSION
        or not payload.get("generated_at_utc")
        or payload.get("catalog_resolution_id")
        != catalog_configuration_resolution_id(PROJECT_ROOT)
    ):
        return None
    return payload


_readiness_cache_payload: dict[str, object] | None = _load_readiness_snapshot()


def _persist_readiness_snapshot(payload: dict[str, object]) -> None:
    destination = READINESS_SNAPSHOT_PATH
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(
        f".{destination.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            temporary.unlink()


def _evidence_age_seconds(payload: dict[str, object]) -> float | None:
    raw = payload.get("generated_at_utc")
    if not isinstance(raw, str):
        return None
    try:
        generated = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if generated.tzinfo is None:
        generated = generated.replace(tzinfo=timezone.utc)
    age = (
        datetime.now(timezone.utc) - generated.astimezone(timezone.utc)
    ).total_seconds()
    if age < -1.0:
        return None
    return max(0.0, age)


def _reset_readiness_cache() -> None:
    """Clear only the in-process read cache; retained evidence is never touched."""
    global _readiness_cache_payload, _readiness_cache_created_monotonic
    global _readiness_refresh_in_progress, _readiness_refresh_error
    with _readiness_cache_lock:
        _readiness_cache_payload = None
        _readiness_cache_created_monotonic = 0.0
        _readiness_refresh_in_progress = False
        _readiness_refresh_error = None


def _refresh_training_readiness() -> None:
    global _readiness_cache_payload, _readiness_cache_created_monotonic
    global _readiness_refresh_in_progress, _readiness_refresh_error
    try:
        payload = audit_training_readiness(
            PROJECT_ROOT,
            market_db_path=_configured_api_market_database_path(),
        ).to_dict()
        payload["cache_contract_version"] = READINESS_CACHE_CONTRACT_VERSION
        evidence_age = _evidence_age_seconds(payload)
        if evidence_age is None or evidence_age >= READINESS_CACHE_TTL_SECONDS:
            raise ValueError(
                "readiness evidence timestamp is missing, invalid, future, or already stale"
            )
        _persist_readiness_snapshot(payload)
    except Exception as exc:
        with _readiness_cache_lock:
            _readiness_refresh_error = type(exc).__name__
            _readiness_refresh_in_progress = False
        return
    with _readiness_cache_lock:
        _readiness_cache_payload = payload
        _readiness_cache_created_monotonic = time.monotonic()
        _readiness_refresh_error = None
        _readiness_refresh_in_progress = False


def _spawn_readiness_refresh() -> None:
    threading.Thread(
        target=_refresh_training_readiness,
        name="closing-tape-readiness-refresh",
        daemon=True,
    ).start()


def request_training_readiness_refresh() -> bool:
    """Schedule the producer-owned readiness scan without involving a GET request."""
    global _readiness_refresh_in_progress
    with _readiness_cache_lock:
        if _readiness_refresh_in_progress:
            return False
        _readiness_refresh_in_progress = True
    try:
        _spawn_readiness_refresh()
    except Exception:
        with _readiness_cache_lock:
            _readiness_refresh_in_progress = False
        raise
    return True


def _cached_training_readiness() -> dict[str, object]:
    """Return retained readiness evidence without starting scans or writes."""
    now = time.monotonic()
    current_resolution_id = catalog_configuration_resolution_id(PROJECT_ROOT)
    with _readiness_cache_lock:
        cached_payload = (
            _readiness_cache_payload
            if _readiness_cache_payload is not None
            and _readiness_cache_payload.get("catalog_resolution_id")
            == current_resolution_id
            else None
        )
        process_cache_age = now - _readiness_cache_created_monotonic
        cache_is_fresh = (
            cached_payload is not None
            and _readiness_cache_created_monotonic > 0
            and 0 <= process_cache_age < READINESS_CACHE_TTL_SECONDS
        )
        if cached_payload is None:
            response: dict[str, object] = {
                "generated_at_utc": None,
                "catalogs": None,
                "catalog_roots": [],
                "catalog_paths": [],
                "catalog_resolution_id": current_resolution_id,
                "catalog_issues": [],
                "sessions": None,
                "eligible_sessions": None,
                "unique_verified_sources": None,
                "verified_close_sessions": None,
                "model_evidence_sessions": None,
                "model_evidence_sources": None,
                "paper_forecast_sessions": None,
                "capture_gate_sessions_required": 10,
                "model_sessions_required": 60,
                "paper_sessions_required": 20,
                "capture_gate_ready": False,
                "model_training_ready": False,
                "paper_evidence_ready": False,
                "verified_close_artifact_issues": [],
                "label_work_queue": [],
                "sessions_detail": [],
                "unavailable_reason": (
                    "readiness scan failed; a producer refresh is required"
                    if _readiness_refresh_error
                    else (
                        "readiness scan is warming; no cached evidence is available yet"
                        if _readiness_refresh_in_progress
                        else "no retained readiness evidence is available"
                    )
                ),
                "cache_age_seconds": None,
                "readiness_cache_state": (
                    "warming" if _readiness_refresh_in_progress else "unavailable"
                ),
            }
        else:
            evidence_age = _evidence_age_seconds(cached_payload)
            response = {
                **cached_payload,
                "cache_age_seconds": (
                    round(evidence_age, 3) if evidence_age is not None else None
                ),
                "readiness_cache_state": (
                    "fresh"
                    if cache_is_fresh
                    else (
                        "stale_refreshing"
                        if _readiness_refresh_in_progress
                        else "stale"
                    )
                ),
            }
            if not cache_is_fresh:
                response.update(
                    {
                        "capture_gate_ready": False,
                        "model_training_ready": False,
                        "paper_evidence_ready": False,
                        "unavailable_reason": (
                            "cached readiness is stale; gates are disabled until "
                            "a producer-owned catalog inventory scan completes"
                        ),
                    }
                )
            if _readiness_refresh_error:
                response.update(
                    {
                        "capture_gate_ready": False,
                        "model_training_ready": False,
                        "paper_evidence_ready": False,
                        "readiness_cache_state": "stale_error",
                        "unavailable_reason": (
                            "readiness refresh failed; cached gate claims are disabled"
                        ),
                    }
                )
        response.update({
            "cache_ttl_seconds": READINESS_CACHE_TTL_SECONDS,
            "refresh_in_progress": _readiness_refresh_in_progress,
            "refresh_error_type": _readiness_refresh_error,
        })
    return response


@router.get(
    "/status",
    summary="Get observed and inferred closing-tape status",
    description=(
        "Reports raw capture integrity, observed aggregate coverage, explicitly inferred "
        "feature coverage, and the frozen-model promotion gate. Missing evidence is unavailable, not zero."
    ),
)
def get_closing_tape_status(
    trading_date: str | None = Query(default=None, description="Trading date in YYYY-MM-DD format"),
):
    # A normal def endpoint runs in FastAPI's worker threadpool. Status may
    # inspect a large SQLite catalog, so it must not block the async event loop
    # that serves health checks and live-market requests.
    try:
        day = date.fromisoformat(trading_date) if trading_date else _current_trading_date()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="trading_date must use YYYY-MM-DD") from exc
    return closing_tape_status(
        PROJECT_ROOT,
        trading_day=day,
        market_db_path=_configured_api_market_database_path(),
    )


@router.get(
    "/readiness",
    summary="Audit retained TCBBO model-evidence readiness",
    description=(
        "Returns the latest producer-retained catalog audit with exact session exclusions "
        "and progress toward capture, model, and paper-evidence gates. The GET never "
        "starts a scan or writes a snapshot."
    ),
)
def get_closing_tape_readiness():
    # Scans and snapshot writes belong to startup/producers, never the read path.
    return _cached_training_readiness()


@router.get(
    "/production-predictions",
    summary="Read immutable promoted TCBBO close estimates",
    description=(
        "Returns only evidence-gated promoted estimates with model, artifact, source-tape, "
        "feature-time, execution provenance, and persisted issuance-time deterministic replay. "
        "Missing output is an empty list, never zero."
    ),
)
async def get_promoted_closing_tape_predictions(
    trading_date: str | None = Query(default=None, description="Trading date in YYYY-MM-DD format"),
):
    current_day = _current_trading_date()
    try:
        day = date.fromisoformat(trading_date) if trading_date else current_day
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="trading_date must use YYYY-MM-DD") from exc
    if trading_date is not None and day.isoformat() != trading_date:
        raise HTTPException(status_code=400, detail="trading_date must use YYYY-MM-DD")
    requested_date = day.isoformat()
    current_date = current_day.isoformat()
    is_current_session = day == current_day
    try:
        rows = await asyncio.to_thread(
            load_governed_promoted_close_predictions,
            _configured_api_market_database_path(),
            project_root=PROJECT_ROOT,
            trading_day=day,
        )
        rows = _validate_governed_prediction_response(
            rows,
            requested_date=requested_date,
        )
    except (KeyError, OSError, RuntimeError, ValueError, sqlite3.DatabaseError) as exc:
        prediction_authority = build_prediction_authority(
            prediction_mode="tcbbo_promoted",
            forecast_state="UNAVAILABLE",
            numeric_available=False,
        )
        return {
            "available": False,
            "read_succeeded": False,
            "reason": f"promoted prediction evidence is unavailable: {exc}",
            "prediction_mode": "tcbbo_promoted",
            "is_estimate": False,
            "prediction_authority": prediction_authority,
            "deterministic_replay_verified": False,
            "replay_status": "UNAVAILABLE",
            "requested_trading_date": requested_date,
            "current_trading_date": current_date,
            "is_current_session": is_current_session,
            "rows": [],
        }
    prediction_authority = build_prediction_authority(
        prediction_mode="tcbbo_promoted",
        forecast_state="VALID" if rows else "UNAVAILABLE",
        numeric_available=bool(rows),
        decision_grade=bool(rows),
        promotion_verified=bool(rows),
    )
    deterministic_replay_verified = bool(rows) and all(
        row["deterministic_replay_verified"] is True for row in rows
    )
    return {
        "available": bool(rows),
        "read_succeeded": True,
        "reason": None if rows else "no promoted TCBBO estimate is available",
        "prediction_mode": "tcbbo_promoted",
        "is_estimate": bool(prediction_authority["is_estimate"]),
        "prediction_authority": prediction_authority,
        "deterministic_replay_verified": deterministic_replay_verified,
        "replay_status": (
            str(rows[0]["replay_status"]) if rows else "UNAVAILABLE"
        ),
        "requested_trading_date": requested_date,
        "current_trading_date": current_date,
        "is_current_session": is_current_session,
        "rows": rows,
    }


@router.get(
    "/scorecard",
    summary="Read the promoted closing-tape governance scorecard",
    description=(
        "Reports prediction availability, explicit abstentions, verified-close error versus "
        "persistence, and calibrated-interval coverage. Insufficient evidence is unavailable, "
        "never zero. This endpoint does not score or mutate predictions."
    ),
)
async def get_closing_tape_scorecard(
    model_version: str | None = Query(default=None),
    family_root: str | None = Query(default=None),
    decision_horizon_minutes: int | None = Query(default=None, ge=1),
    volatility_regime: str | None = Query(default=None),
    minimum_scored_sessions: int = Query(default=5, ge=1, le=1000),
):
    regime_filter = volatility_regime if isinstance(volatility_regime, str) else None
    try:
        return await asyncio.to_thread(
            build_closing_tape_scorecard,
            _configured_api_market_database_path(),
            project_root=PROJECT_ROOT,
            model_version=model_version,
            family_root=family_root,
            decision_horizon_minutes=decision_horizon_minutes,
            volatility_regime=regime_filter,
            minimum_scored_sessions=minimum_scored_sessions,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (RuntimeError, sqlite3.DatabaseError) as exc:
        return _unavailable_scorecard_payload(
            f"closing-tape governance evidence is unavailable: {exc}"
        )
