"""Application lifecycle hooks for backend FastAPI app."""

import asyncio
import copy
import hashlib
import inspect
import json
import logging
import math
import os
import time
from contextlib import asynccontextmanager, nullcontext
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from fastapi import FastAPI

from backend.config import (
    LIVE_DATA_STALE_AFTER_SECONDS,
    MARKET_DATA_PROVIDER,
    SYMBOLS,
)
from backend.ai_predictor import build_ai_prediction
from backend.database import init_db, save_prediction_snapshot
from backend.historical_context import get_historical_context, historical_adjustment
from backend.inference import (
    GEX_INFERENCE_FEATURE_SCHEMA_VERSION,
    build_live_inference_features,
    load_all_models,
)
from backend.market_structure import (
    get_market_structure_journal,
    run_market_structure_capture_loop,
)
from backend.research.live_shadow import LiveShadowResearchRecorder
from backend.runtime_controls import SystemSleepGuard
from backend.streamer import get_streamer, start_streaming
from backend.workstation import (
    build_workstation_state,
    passport_contract_is_complete,
    payload_age_seconds,
    payload_has_fallback_provenance,
    payload_revision_key,
    unavailable_workstation_state,
    workstation_state_store,
)

logger = logging.getLogger(__name__)

_application_sleep_guard = SystemSleepGuard()


def runtime_control_health() -> dict[str, object]:
    """Expose process-lifetime runtime controls without claiming more than observed."""

    return {"sleep_prevention": _application_sleep_guard.to_dict()}

try:
    from backend.prediction_passport import (
        issue_prediction_passport as _issue_prediction_passport,
        issue_snapshot_passport as _issue_snapshot_passport,
    )
except ImportError:  # Clean adapter boundary while passport support is optional.
    _issue_prediction_passport = None
    _issue_snapshot_passport = None


class WorkstationPredictionCoordinator:
    """Compute and publish predictions independently of HTTP client activity."""

    def __init__(
        self,
        *,
        streamer: Any,
        shadow_research: Any,
        capture_seconds: float,
        prediction_builder: Callable[..., dict[str, Any]] = build_ai_prediction,
        snapshot_saver: Callable[..., object | None] = save_prediction_snapshot,
        context_loader: Callable[[str], dict[str, Any]] = get_historical_context,
        adjustment_builder: Callable[..., tuple[float, list[dict]]] = historical_adjustment,
        monotonic_clock: Callable[[], float] = time.monotonic,
        passport_writer: Callable[..., dict[str, Any]] | None = None,
        snapshot_passport_writer: Callable[..., dict[str, Any]] | None = None,
    ) -> None:
        self.streamer = streamer
        self.shadow_research = shadow_research
        self.capture_seconds = max(0.0, float(capture_seconds))
        self.prediction_builder = prediction_builder
        self.snapshot_saver = snapshot_saver
        self.context_loader = context_loader
        self.adjustment_builder = adjustment_builder
        self.monotonic_clock = monotonic_clock
        self.passport_writer = passport_writer
        self.snapshot_passport_writer = snapshot_passport_writer
        self.last_prediction_capture: dict[str, float] = {}
        # Only committed input captures carry calculation_id. Their cadence is
        # independent of periodic prediction saves; retain one success per symbol.
        self.last_shadow_capture: dict[str, tuple[str, int, str]] = {}
        # One bounded retained identity per symbol. This lets a source revision
        # age from fresh to stale without minting a second forecast id.
        self.last_passport_by_revision: dict[
            str, tuple[tuple[Any, ...], dict[str, Any]]
        ] = {}

    def _remember_passport(
        self,
        *,
        symbol: str,
        data: dict[str, Any],
        passport: dict[str, Any] | None,
    ) -> None:
        if passport_contract_is_complete(passport):
            self.last_passport_by_revision[symbol] = (
                payload_revision_key(data),
                copy.deepcopy(passport),
            )

    def _retained_passport_for_revision(
        self, *, symbol: str, data: dict[str, Any]
    ) -> dict[str, Any] | None:
        retained = self.last_passport_by_revision.get(symbol)
        if retained is None or retained[0] != payload_revision_key(data):
            return None
        return copy.deepcopy(retained[1])

    @staticmethod
    def _origin_key(
        data: dict[str, Any],
        *,
        prediction: dict[str, Any],
        state: str | None = None,
    ) -> str:
        # The lifecycle can publish the same calculation once while fresh and
        # once again when its observed freshness state changes. Include the
        # exact passport input so those materially different observations do
        # not collide under one immutable origin.
        encoded = json.dumps(
            {
                "revision": payload_revision_key(data),
                "state": state or "forecast",
                "passport_input": prediction,
            },
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _write_revision_passport(
        self,
        prediction: dict[str, Any],
        *,
        data: dict[str, Any],
        requested_state: str | None = None,
        publication_guard: Callable[[], bool] | None = None,
    ) -> dict[str, Any] | None:
        if self.passport_writer is None:
            return None
        try:
            kwargs = {
                "prediction_mode": "backend_lifecycle",
                "origin_kind": "lifecycle_revision",
                "origin_key": self._origin_key(
                    data,
                    prediction=prediction,
                    state=requested_state,
                ),
                "requested_state": requested_state,
            }
            if publication_guard is not None and self._accepts_keyword(
                self.passport_writer, "publication_guard"
            ):
                kwargs["publication_guard"] = publication_guard
            return self.passport_writer(
                prediction,
                **kwargs,
            )
        except Exception:
            logger.exception(
                "Prediction-passport issuance failed for %s; publishing nullable forecast_id",
                prediction.get("symbol") or data.get("symbol"),
            )
            return None

    def _write_snapshot_passport(
        self,
        saved: object,
        prediction: dict[str, Any],
        *,
        publication_guard: Callable[[], bool] | None = None,
    ) -> dict[str, Any] | None:
        if self.snapshot_passport_writer is None:
            return None
        try:
            kwargs: dict[str, Any] = {"prediction": prediction}
            if publication_guard is not None and self._accepts_keyword(
                self.snapshot_passport_writer, "publication_guard"
            ):
                kwargs["publication_guard"] = publication_guard
            return self.snapshot_passport_writer(int(getattr(saved, "id")), **kwargs)
        except Exception:
            logger.exception(
                "Snapshot-passport issuance failed for %s; publishing nullable forecast_id",
                prediction.get("symbol"),
            )
            return None

    @staticmethod
    def _accepts_keyword(writer: Callable[..., Any], keyword: str) -> bool:
        try:
            parameters = inspect.signature(writer).parameters.values()
        except (TypeError, ValueError):
            return False
        return any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            or parameter.name == keyword
            for parameter in parameters
        )

    @staticmethod
    def _positive_generation(value: Any) -> int | None:
        # Do not coerce booleans or fractional/non-finite numbers into a live
        # generation. JSON integer strings remain compatible with older callers.
        if isinstance(value, bool):
            return None
        if isinstance(value, str):
            value = value.strip()
            if not value or any(character not in "0123456789" for character in value):
                return None
        elif not isinstance(value, int):
            return None
        generation = int(value)
        return generation if generation > 0 else None

    def _record_committed_shadow(self, symbol: str, data: dict[str, Any]) -> None:
        """Capture this committed revision while the caller holds the live guard."""

        calculation_id = data.get("calculation_id")
        if not isinstance(calculation_id, str) or not calculation_id.strip():
            return
        # The stream publishes this ID only after the calculation inputs commit.
        # Never borrow an ID from an earlier revision or increase input capture.
        generation = self._positive_generation(data.get("subscription_generation"))
        if generation is None:
            return
        identity = (str(data["subscription_epoch_id"]), generation, calculation_id)
        if self.last_shadow_capture.get(symbol) == identity:
            return
        try:
            result = self.shadow_research.record(data)
            if getattr(result, "error", None):
                logger.warning(
                    "Shadow research capture failed for %s: %s", symbol, result.error
                )
                return
            if result is None or getattr(result, "skipped_reason", None):
                return
        except Exception:
            logger.exception("Shadow research capture failed for %s", symbol)
            return
        # Failed or skipped attempts must remain eligible for a later retry.
        self.last_shadow_capture[symbol] = identity

    def _publication_allowed(self, data: dict[str, Any]) -> bool:
        return self._guard_snapshot(data)["requested_state"] is None

    def _publication_context(self, data: dict[str, Any]):
        factory = getattr(self.streamer, "prediction_publication_guard", None)
        if not callable(factory):
            return nullcontext(self._publication_allowed(data))
        return factory(
            subscription_epoch_id=str(data.get("subscription_epoch_id") or ""),
            subscription_generation=int(data.get("subscription_generation") or 0),
        )

    def _guard_snapshot(self, data: dict[str, Any]) -> dict[str, Any]:
        """Classify whether one payload may proceed to inference and publication."""

        provider = str(data.get("provider") or MARKET_DATA_PROVIDER)
        is_fallback = payload_has_fallback_provenance(data)
        validation_is_valid = bool(
            data.get("validation_is_valid") is True
            and data.get("gamma_excluded_from_model") is False
        )
        active_epoch_id = str(
            getattr(self.streamer, "subscription_epoch_id", "") or ""
        ).strip()
        payload_epoch_id = str(data.get("subscription_epoch_id") or "").strip()
        active_epoch_valid = bool(
            len(active_epoch_id) == 64
            and all(character in "0123456789abcdef" for character in active_epoch_id)
        )
        payload_epoch_valid = bool(
            len(payload_epoch_id) == 64
            and all(character in "0123456789abcdef" for character in payload_epoch_id)
        )
        epoch_mismatch = bool(
            not active_epoch_valid
            or not payload_epoch_valid
            or payload_epoch_id != active_epoch_id
        )
        active_generation = getattr(self.streamer, "active_generation", None)
        payload_generation = data.get("subscription_generation")
        active_generation_value = self._positive_generation(active_generation)
        payload_generation_value = self._positive_generation(payload_generation)
        generation_mismatch = bool(
            active_generation_value is None
            or payload_generation_value is None
            or payload_generation_value != active_generation_value
        )
        handoff_status = str(
            getattr(self.streamer, "handoff_status", "") or ""
        ).lower()
        handoff_unready = handoff_status != "active"
        spot = data.get("price") if data.get("price") is not None else data.get("spot_price")
        try:
            spot = float(spot) if spot is not None else None
        except (TypeError, ValueError):
            spot = None
        if spot is not None and not math.isfinite(spot):
            spot = None
        age = payload_age_seconds(data)
        is_stale = age is None or age > LIVE_DATA_STALE_AFTER_SECONDS

        requested_state: str | None = None
        if (
            is_fallback
            or not validation_is_valid
            or epoch_mismatch
            or generation_mismatch
            or handoff_unready
        ):
            requested_state = "abstain"
        elif spot is None or spot <= 0:
            requested_state = "unavailable"
        elif is_stale:
            requested_state = "stale"

        validation_reasons = list(data.get("validation_failure_reasons") or [])
        if not validation_is_valid:
            validation_reasons.append("SOURCE_VALIDATION_NOT_CONFIRMED")
        if is_fallback:
            validation_reasons.append("NON_PRODUCTION_FALLBACK")
        if generation_mismatch:
            validation_reasons.append("SUBSCRIPTION_GENERATION_MISMATCH")
        if epoch_mismatch:
            validation_reasons.append("SUBSCRIPTION_EPOCH_MISMATCH")
        if handoff_unready:
            validation_reasons.append(f"HANDOFF_NOT_ACTIVE:{handoff_status}")
        return {
            "provider": provider,
            "is_fallback": is_fallback,
            "spot": spot,
            "age": age,
            "is_stale": is_stale,
            "requested_state": requested_state,
            "validation_failure_reasons": list(dict.fromkeys(validation_reasons)),
        }

    def _publish_guarded_state(
        self,
        *,
        symbol: str,
        data: dict[str, Any],
        guard: dict[str, Any],
    ) -> dict[str, Any]:
        """Publish a numeric-free state for a payload that failed a live gate."""

        requested_state = str(guard["requested_state"])
        passport_usable = requested_state in {"stale", "unavailable"}
        abstention = {
            "symbol": symbol,
            "timestamp": data.get("timestamp")
            or data.get("latest_ts_recv_utc")
            or datetime.now(timezone.utc).isoformat(),
            "provider": guard["provider"],
            "usable": passport_usable,
            "stale": bool(guard["is_stale"]),
            "is_fallback": bool(guard["is_fallback"]),
            "current_price": guard["spot"],
            "pin_payload": data,
            "subscription_epoch_id": data.get("subscription_epoch_id"),
            "subscription_generation": data.get("subscription_generation"),
            "quote_age_seconds": guard["age"],
            "validation_status": (
                "stale" if requested_state == "stale" else "invalid"
            ),
            "validation_failure_reasons": guard["validation_failure_reasons"],
        }
        passport = None
        if requested_state == "stale":
            passport = self._retained_passport_for_revision(
                symbol=symbol,
                data=data,
            )
        if passport is None:
            passport = self._write_revision_passport(
                abstention, data=data, requested_state=requested_state
            )
            self._remember_passport(symbol=symbol, data=data, passport=passport)
        state_payload = {
            **data,
            # A stale observation can remain structurally valid; freshness is
            # represented separately. Other guarded states fail validation.
            "validation_is_valid": requested_state == "stale",
            "validation_failure_reasons": guard["validation_failure_reasons"],
        }
        state = build_workstation_state(
            symbol=symbol,
            payload=state_payload,
            prediction=None,
            passport=passport,
        )
        return workstation_state_store.publish(state)

    def process_payload(self, data: dict[str, Any]) -> dict[str, Any] | None:
        """Publish one backend-produced market revision and capture it when due."""

        symbol = str(data.get("symbol") or "").upper()
        if not symbol:
            return None

        guard = self._guard_snapshot(data)
        if guard["requested_state"] is not None:
            return self._publish_guarded_state(symbol=symbol, data=data, guard=guard)

        provider = str(guard["provider"])
        spot = float(guard["spot"])
        age = guard["age"]

        try:
            vix_payload = None
            pin_fetcher = getattr(self.streamer, "get_latest_pin", None)
            if symbol != "VIX" and callable(pin_fetcher):
                vix_payload = pin_fetcher("VIX")
            context = self.context_loader(symbol)
            historical_delta = self.adjustment_builder(symbol, spot, context)
            prediction = self.prediction_builder(
                symbol,
                data,
                vix_payload=vix_payload,
                historical_context=context,
                historical_adjustment=historical_delta,
            )
            if not isinstance(prediction, dict):
                raise TypeError("prediction builder returned a non-object result")
            live_inference_features = build_live_inference_features(
                data,
                current_price=spot,
            )
        except Exception:
            logger.exception("Prediction inference pipeline failed for %s", symbol)
            failed_guard = {
                **guard,
                "requested_state": "unavailable",
                "validation_failure_reasons": list(
                    dict.fromkeys(
                        [
                            *list(guard.get("validation_failure_reasons") or []),
                            "INFERENCE_PIPELINE_UNAVAILABLE",
                        ]
                    )
                ),
            }
            return self._publish_guarded_state(
                symbol=symbol,
                data=data,
                guard=failed_guard,
            )
        feature_snapshot = prediction.get("feature_snapshot") or {}
        prediction_record = {
            **prediction,
            "symbol": symbol,
            "provider": provider,
            # Decision-grade persistence requires affirmative model evidence;
            # omission is not consent to publish a numeric forecast.
            "usable": prediction.get("usable") is True,
            "pin_payload": data,
            "subscription_epoch_id": data.get("subscription_epoch_id"),
            "subscription_generation": data.get("subscription_generation"),
            "quote_age_seconds": data.get("quote_age_seconds"),
            "data_age_seconds": age,
            "active_contract_count": data.get("contracts")
            if data.get("contracts") is not None
            else data.get("contracts_count"),
            "fresh_quote_count": data.get("fresh_quote_count"),
            "gamma_pin": data.get("gamma_pin"),
            "max_pain": data.get("max_pain"),
            "zero_gamma": data.get("zero_gamma"),
            "gross_gex": data.get("gross_gex"),
            "net_gex": data.get("net_gex"),
            "call_gex": data.get("call_gex_total"),
            "put_gex": data.get("put_gex_total"),
            "feature_schema_version": prediction.get("feature_schema_version")
            or feature_snapshot.get("schema_version"),
            "live_inference_feature_schema_version": str(
                data.get("inference_feature_schema_version")
                or GEX_INFERENCE_FEATURE_SCHEMA_VERSION
            ),
            "live_inference_features": live_inference_features,
            "feature_hash": prediction.get("feature_hash")
            or hashlib.sha256(
                json.dumps(
                    feature_snapshot,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ).encode("utf-8")
            ).hexdigest(),
        }

        if not prediction_record["usable"]:
            unusable_guard = {
                **guard,
                "requested_state": "abstain",
                "validation_failure_reasons": list(
                    dict.fromkeys(
                        [
                            *list(guard.get("validation_failure_reasons") or []),
                            "PREDICTION_USABLE_NOT_CONFIRMED",
                        ]
                    )
                ),
            }
            return self._publish_guarded_state(
                symbol=symbol,
                data=data,
                guard=unusable_guard,
            )

        # Recheck after inference so a generation change or handoff that began
        # during model work cannot persist or publish the obsolete forecast.
        guard = self._guard_snapshot(data)
        if guard["requested_state"] is not None:
            return self._publish_guarded_state(symbol=symbol, data=data, guard=guard)

        saved = None
        passport = None
        now = self.monotonic_clock()
        deferred_guard = None
        publication_guard = lambda: self._publication_allowed(data)
        with self._publication_context(data) as publication_lease:
            guard = self._guard_snapshot(data)
            if not publication_lease or guard["requested_state"] is not None:
                if guard["requested_state"] is None:
                    guard = {
                        **guard,
                        "requested_state": "abstain",
                        "validation_failure_reasons": ["PUBLICATION_GUARD_REJECTED"],
                    }
                deferred_guard = guard
            else:
                if now - self.last_prediction_capture.get(symbol, 0.0) >= self.capture_seconds:
                    try:
                        saver_kwargs: dict[str, Any] = {
                            "prediction_mode": "backend_periodic"
                        }
                        if self._accepts_keyword(
                            self.snapshot_saver, "publication_guard"
                        ):
                            saver_kwargs["publication_guard"] = publication_guard
                        saved = self.snapshot_saver(prediction_record, **saver_kwargs)
                    except Exception:
                        logger.exception(
                            "Prediction snapshot persistence failed for %s; state will still publish",
                            symbol,
                        )

                guard = self._guard_snapshot(data)
                if guard["requested_state"] is not None:
                    deferred_guard = guard
                    saved = None
                else:
                    passport = (
                        self._write_snapshot_passport(
                            saved,
                            prediction_record,
                            publication_guard=publication_guard,
                        )
                        if saved is not None
                        else None
                    )
                    if not passport_contract_is_complete(passport):
                        passport = self._write_revision_passport(
                            prediction_record,
                            data=data,
                            publication_guard=publication_guard,
                        )
                    guard = self._guard_snapshot(data)
                    if guard["requested_state"] is not None:
                        deferred_guard = guard
                        saved = None
                        passport = None

        if deferred_guard is not None:
            return self._publish_guarded_state(
                symbol=symbol,
                data=data,
                guard=deferred_guard,
            )
        if saved is not None:
            self.last_prediction_capture[symbol] = now

        self._remember_passport(symbol=symbol, data=data, passport=passport)
        state_prediction = prediction_record
        if not passport_contract_is_complete(passport):
            passport = None
            state_prediction = {
                **prediction_record,
                "usable": False,
                "validation_status": "invalid",
                "validation_failure_reasons": list(
                    dict.fromkeys(
                        [
                            *list(prediction_record.get("validation_failure_reasons") or []),
                            "PASSPORT_ISSUANCE_UNAVAILABLE",
                        ]
                    )
                ),
            }
        # Reacquire the same runtime barrier for the final check and state
        # publication.  A handoff cannot slip between validation and exposing a
        # decision-valid state to readers.
        final_guard = None
        with self._publication_context(data) as publication_lease:
            guard = self._guard_snapshot(data)
            if publication_lease and guard["requested_state"] is None:
                self._record_committed_shadow(symbol, data)
                # Journal latency must not make the final published state stale.
                guard = self._guard_snapshot(data)
            if not publication_lease or guard["requested_state"] is not None:
                if guard["requested_state"] is None:
                    guard = {
                        **guard,
                        "requested_state": "abstain",
                        "validation_failure_reasons": ["PUBLICATION_GUARD_REJECTED"],
                    }
                final_guard = guard
            else:
                snapshot_id = getattr(saved, "id", None)
                state = build_workstation_state(
                    symbol=symbol,
                    payload=data,
                    prediction=state_prediction,
                    feature_snapshot=feature_snapshot,
                    passport=passport,
                    prediction_snapshot_id=snapshot_id,
                )
                return workstation_state_store.publish(state)
        return self._publish_guarded_state(
            symbol=symbol,
            data=data,
            guard=final_guard,
        )


async def run_workstation_state_loop(
    streamer: Any,
    coordinator: WorkstationPredictionCoordinator,
    *,
    poll_seconds: float = 0.25,
) -> None:
    """Observe backend buffers and publish each new revision exactly once."""

    seen: dict[str, tuple[Any, ...]] = {}
    while True:
        latest_by_symbol = streamer.get_all_latest()
        for raw_symbol, raw_payload in latest_by_symbol.items():
            if not isinstance(raw_payload, dict):
                continue
            payload = dict(raw_payload)
            payload.setdefault("symbol", str(raw_symbol).upper())
            symbol = str(payload.get("symbol") or raw_symbol).upper()
            revision_key = payload_revision_key(payload)
            age = payload_age_seconds(payload)
            is_stale = age is None or age > LIVE_DATA_STALE_AFTER_SECONDS
            active_generation = getattr(streamer, "active_generation", None)
            active_epoch_id = getattr(streamer, "subscription_epoch_id", None)
            handoff_status = str(
                getattr(streamer, "handoff_status", "active") or "active"
            ).lower()
            observation_key = (
                revision_key,
                is_stale,
                active_epoch_id,
                active_generation,
                handoff_status,
            )
            if seen.get(symbol) == observation_key:
                continue
            try:
                await asyncio.to_thread(coordinator.process_payload, payload)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Workstation state publication failed for %s", symbol)
                continue
            seen[symbol] = observation_key
        await asyncio.sleep(max(0.05, float(poll_seconds)))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    logger.info("🚀 Starting institutional-grade prediction API...")
    logger.info(
        "Startup self-check | active_path=%s provider=%s databento_key=%s massive_polygon=disabled",
        str(Path.cwd()),
        MARKET_DATA_PROVIDER,
        "present" if os.getenv("DATABENTO_API_KEY") else "missing",
    )

    sleep_guard_active = _application_sleep_guard.activate()
    if _application_sleep_guard.requested and not sleep_guard_active:
        logger.warning(
            "Could not activate process-lifetime sleep prevention: %s",
            _application_sleep_guard.error,
        )
    else:
        logger.info(
            "Runtime sleep prevention: %s",
            _application_sleep_guard.to_dict(),
        )

    init_db()

    logger.info("Initializing inference runtime and loading available trained artifacts...")
    loaded_models = load_all_models()
    logger.info("Inference artifacts loaded: %s", loaded_models or "none")

    logger.info("Starting market data stream...")
    streamer = get_streamer()
    market_structure_journal = get_market_structure_journal()
    shadow_research = LiveShadowResearchRecorder()
    prediction_capture_seconds = float(os.getenv("PREDICTION_CAPTURE_INTERVAL_SECONDS", "60"))
    coordinator = WorkstationPredictionCoordinator(
        streamer=streamer,
        shadow_research=shadow_research,
        capture_seconds=prediction_capture_seconds,
        passport_writer=_issue_prediction_passport,
        snapshot_passport_writer=_issue_snapshot_passport,
    )
    workstation_state_store.reset()
    configured_symbols = getattr(streamer, "symbols", None) or SYMBOLS
    for symbol in configured_symbols:
        workstation_state_store.publish(unavailable_workstation_state(str(symbol)))

    streaming_task = asyncio.create_task(start_streaming())
    market_structure_task = asyncio.create_task(
        run_market_structure_capture_loop(streamer, market_structure_journal)
    )
    workstation_task = asyncio.create_task(
        run_workstation_state_loop(streamer, coordinator)
    )

    logger.info("✅ API ready for requests")
    try:
        yield
    finally:
        logger.info("Shutting down...")
        streamer_instance = get_streamer()
        await streamer_instance.stop()
        streaming_task.cancel()
        market_structure_task.cancel()
        workstation_task.cancel()
        await asyncio.gather(
            streaming_task,
            market_structure_task,
            workstation_task,
            return_exceptions=True,
        )
        _application_sleep_guard.release()
