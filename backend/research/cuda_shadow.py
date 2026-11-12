"""Opt-in, research-only CUDA model inference isolated from primary forecasts.

The worker consumes only already-committed Databento calculation payloads.  It
does not open a market-data connection, mutate the deterministic production
prediction, or write production tables.  Invalid timing/data, a CPU fallback,
or an incomplete/rejected model artifact all fail closed.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import logging
import math
import os
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from backend.inference import (
    CUDA_SHADOW_MAX_ABS_MOVE_BPS_HARD_CEILING,
    CUDA_SHADOW_OUTPUT_CONTRACT_VERSION,
    CUDA_SHADOW_PREDICTION_HORIZON_SECONDS,
    get_inference_engine,
)
from backend.research.live_shadow import (
    MIN_PRIMARY_PAIR_COVERAGE_RATIO,
    _optional_int,
    _parse_utc,
    _positive_float,
    _process_identity,
)
from backend.research.shadow_formula import (
    NAIVE_LAST_PRICE_V1,
    FormulaDefinition,
    ShadowFormulaEngine,
    ShadowGuardrails,
    ShadowPrediction,
    ShadowPredictionJournal,
    observation_from_databento_payload,
)


UTC = timezone.utc
logger = logging.getLogger(__name__)
DEFAULT_CUDA_SHADOW_JOURNAL_PATH = (
    Path(__file__).resolve().parents[2] / "data" / "cuda_shadow_research.db"
)
CUDA_SHADOW_ALLOWED_SYMBOLS = frozenset({"SPX", "NDX"})


def _env_enabled(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _valid_artifact_identity(evidence: Mapping[str, Any]) -> bool:
    artifact_sha256 = str(evidence.get("artifact_sha256") or "")
    return (
        evidence.get("status") == "LOADED_SHADOW_ONLY"
        and len(artifact_sha256) == 64
        and artifact_sha256 == artifact_sha256.lower()
        and all(character in "0123456789abcdef" for character in artifact_sha256)
    )


def _formula_for_artifact(evidence: Mapping[str, Any]) -> FormulaDefinition:
    """Bind every journal identity to the exact artifact and feature contract."""
    artifact_sha256 = str(evidence.get("artifact_sha256") or "missing-artifact")
    feature_names = tuple(str(name) for name in (evidence.get("feature_names") or ()))
    return FormulaDefinition(
        formula_id="shadow-cuda-artifact",
        version=(
            f"1.0.0-{CUDA_SHADOW_OUTPUT_CONTRACT_VERSION}-"
            f"sha256-{artifact_sha256}"
        ),
        description=(
            "Contract-gated CUDA artifact output recorded only in the isolated "
            "shadow journal; it has no production prediction authority."
        ),
        equation="predicted_price(t+300s) = artifact_output_future_price",
        prediction_horizon_seconds=CUDA_SHADOW_PREDICTION_HORIZON_SECONDS,
        required_raw_features=feature_names,
        feature_units={name: "artifact-declared" for name in feature_names},
        transformations={},
        coefficients={},
        fitted=True,
        promotion_allowed=False,
    )


def cuda_shadow_runtime_status(
    *,
    engine: Any | None = None,
    enabled: bool | None = None,
) -> dict[str, Any]:
    """Expose device and artifact evidence even when shadow execution is off."""
    runtime = engine or get_inference_engine()
    configured = (
        _env_enabled("CUDA_SHADOW_RESEARCH_ENABLED", False)
        if enabled is None
        else bool(enabled)
    )
    device = str(getattr(runtime, "device", "unknown"))
    models = sorted(str(symbol).upper() for symbol in getattr(runtime, "models", {}))
    evidence_getter = getattr(runtime, "get_model_load_evidence", None)
    evidence = evidence_getter() if callable(evidence_getter) else {}
    ready_models = sorted(
        symbol
        for symbol in (set(models) & CUDA_SHADOW_ALLOWED_SYMBOLS)
        if _valid_artifact_identity(evidence.get(symbol, {}))
    )
    symbol_readiness = {
        symbol: {
            "ready": symbol in ready_models,
            "artifact_status": (evidence.get(symbol) or {}).get("status", "UNKNOWN"),
        }
        for symbol in sorted(CUDA_SHADOW_ALLOWED_SYMBOLS)
    }
    journal_path = Path(
        os.getenv("CUDA_SHADOW_RESEARCH_DB_PATH")
        or DEFAULT_CUDA_SHADOW_JOURNAL_PATH
    ).resolve()
    if not configured:
        status = "DISABLED"
    elif not device.startswith("cuda"):
        status = "CUDA_RUNTIME_UNAVAILABLE"
    elif not ready_models:
        status = "NO_VALID_CORE_ARTIFACT"
    elif set(ready_models) != CUDA_SHADOW_ALLOWED_SYMBOLS:
        status = "PARTIAL_READY_SHADOW_ONLY"
    else:
        status = "READY_SHADOW_ONLY"
    return {
        "status": status,
        "enabled": configured,
        "research_only": True,
        "promotion_allowed": False,
        "primary_prediction_replaced": False,
        "device": device,
        "device_setup_status": getattr(
            runtime, "device_setup_status", "UNKNOWN"
        ),
        "device_setup_error": dict(
            getattr(runtime, "device_setup_error", None) or {}
        ),
        "models_loaded": models,
        "shadow_ready_models": ready_models,
        "symbol_readiness": symbol_readiness,
        "artifact_evidence": evidence,
        "journal_path": str(journal_path),
        "journal_present": journal_path.is_file(),
    }


@dataclass(frozen=True)
class CudaShadowRecordResult:
    prediction: ShadowPrediction | None = None
    saved_count: int = 0
    skipped_reason: str | None = None
    error: str | None = None
    research_only: bool = True
    primary_prediction_replaced: bool = False


class CudaShadowResearchRecorder:
    """Persist contract-gated model output to a dedicated research database."""

    def __init__(
        self,
        *,
        inference_engine: Any | None = None,
        journal_path: str | Path | None = None,
        enabled: bool | None = None,
        max_quote_age_seconds: float = 30.0,
    ) -> None:
        self.inference_engine = inference_engine or get_inference_engine()
        self.enabled = (
            _env_enabled("CUDA_SHADOW_RESEARCH_ENABLED", False)
            if enabled is None
            else bool(enabled)
        )
        configured_path = (
            journal_path
            or os.getenv("CUDA_SHADOW_RESEARCH_DB_PATH")
            or DEFAULT_CUDA_SHADOW_JOURNAL_PATH
        )
        self._journal_path = Path(configured_path)
        # Disabled/rejected startup remains file- and lock-free.  Construct
        # the isolated journal only after opt-in, CUDA, and artifact gates pass.
        self.journal: ShadowPredictionJournal | None = None
        self.guardrail_engine = ShadowFormulaEngine(
            ShadowGuardrails(max_quote_age_seconds=float(max_quote_age_seconds))
        )
        if self.ready_symbols:
            self.journal = ShadowPredictionJournal(self._journal_path)

    @property
    def ready_symbols(self) -> tuple[str, ...]:
        if not self.enabled or not str(self.inference_engine.device).startswith("cuda"):
            return ()
        candidates = (
            set(str(symbol).upper() for symbol in self.inference_engine.models)
            & CUDA_SHADOW_ALLOWED_SYMBOLS
        )
        return tuple(
            sorted(
                symbol
                for symbol in candidates
                if _valid_artifact_identity(
                    self.inference_engine.get_model_load_evidence(symbol)
                )
            )
        )

    def status(self) -> dict[str, Any]:
        return cuda_shadow_runtime_status(
            engine=self.inference_engine,
            enabled=self.enabled,
        )

    def record(
        self,
        payload: Mapping[str, Any],
        *,
        processed_at_utc: datetime | None = None,
        forced_abstention_reasons: Sequence[str] = (),
    ) -> CudaShadowRecordResult:
        """Run one shadow inference; every failure is contained and nonnumeric."""
        if not self.enabled:
            return CudaShadowRecordResult(skipped_reason="CUDA_SHADOW_RESEARCH_DISABLED")
        symbol = str(payload.get("symbol") or "").strip().upper()
        if not str(self.inference_engine.device).startswith("cuda"):
            return CudaShadowRecordResult(skipped_reason="CUDA_RUNTIME_UNAVAILABLE")
        if (
            symbol not in CUDA_SHADOW_ALLOWED_SYMBOLS
            or symbol not in self.inference_engine.models
        ):
            # Do not create repetitive abstention rows when no valid model exists.
            return CudaShadowRecordResult(skipped_reason="MODEL_ARTIFACT_NOT_LOADED")
        evidence = self.inference_engine.get_model_load_evidence(symbol)
        if not _valid_artifact_identity(evidence):
            return CudaShadowRecordResult(
                skipped_reason="MODEL_ARTIFACT_SHA256_INVALID"
            )
        if self.journal is None:
            self.journal = ShadowPredictionJournal(self._journal_path)
        try:
            return self._record(
                payload,
                processed_at_utc=processed_at_utc,
                forced_abstention_reasons=forced_abstention_reasons,
            )
        except Exception as exc:
            logger.warning("CUDA shadow inference failed closed for %s: %s", symbol, exc)
            return CudaShadowRecordResult(error=f"{type(exc).__name__}: {exc}")

    def _record(
        self,
        payload: Mapping[str, Any],
        *,
        processed_at_utc: datetime | None,
        forced_abstention_reasons: Sequence[str],
    ) -> CudaShadowRecordResult:
        symbol = str(payload.get("symbol") or "").strip().upper()
        event_at = _parse_utc(payload.get("latest_ts_event_utc"))
        recv_at = _parse_utc(payload.get("latest_ts_recv_utc"))
        index_at = _parse_utc(payload.get("observation_index_utc"))
        processed_at = _parse_utc(processed_at_utc or datetime.now(UTC))
        spot = _positive_float(
            payload.get("price")
            if payload.get("price") is not None
            else payload.get("spot_last")
        )
        process_identity = _process_identity(payload)
        if self.journal is None:
            return CudaShadowRecordResult(skipped_reason="SHADOW_JOURNAL_NOT_INITIALIZED")
        reasons = list(forced_abstention_reasons)
        if _optional_int(payload.get("provider_timestamp_order_errors")) not in (None, 0):
            reasons.append("SOURCE_PROVIDER_TIMESTAMP_ORDER_ERRORS")
        if (_optional_int(payload.get("mapping_version_missing_count")) or 0) > 0:
            reasons.append("INCOMPLETE_POINT_IN_TIME_SYMBOL_MAPPINGS")
        pair_coverage = _finite(payload.get("primary_pair_coverage_ratio"))
        if pair_coverage is None:
            reasons.append("PRIMARY_PAIR_COVERAGE_UNKNOWN")
        elif pair_coverage < MIN_PRIMARY_PAIR_COVERAGE_RATIO:
            reasons.append(
                "INSUFFICIENT_PRIMARY_PAIR_COVERAGE: "
                f"{pair_coverage:.4f} < {MIN_PRIMARY_PAIR_COVERAGE_RATIO:.4f}"
            )
        clock_status = str(
            (payload.get("processing_clock_telemetry") or {}).get("status") or ""
        ).strip().lower()
        if clock_status != "synchronized":
            reasons.append(
                "PROCESSING_CLOCK_UNSYNCHRONIZED"
                if clock_status == "unsynchronized"
                else "PROCESSING_CLOCK_UNKNOWN"
            )

        observation = observation_from_databento_payload(
            payload,
            ts_event_utc=event_at,
            ts_recv_utc=recv_at,
            observation_index_utc=index_at,
            processed_at_utc=processed_at,
            feature_asof_utc={"spot": index_at},
            spot_return_5m=None,
            trusted_quote_age_seconds=payload.get("quote_age_max_seconds"),
            fresh_quote_count=_optional_int(payload.get("fresh_quote_count")),
            paired_quote_count=_optional_int(payload.get("paired_quote_count")),
            crossed_quote_count=_optional_int(
                payload.get("contributing_crossed_quote_count")
            ),
            expected_quote_count=_optional_int(payload.get("expected_quote_count")),
            instrument_definition_version=(
                str(payload.get("instrument_definition_version"))
                if payload.get("instrument_definition_version")
                else None
            ),
            symbol_mapping_version=(
                str(payload.get("symbol_mapping_version"))
                if payload.get("symbol_mapping_version")
                else None
            ),
        )
        if (
            index_at is not None
            and process_identity is not None
            and self.journal.has_observation_index(
                symbol,
                index_at,
                subscription_epoch_id=process_identity[0],
                subscription_generation=process_identity[1],
            )
        ):
            reasons.append("DUPLICATE_SYMBOL_INDEX_TIMESTAMP")

        baseline = self.guardrail_engine.evaluate(
            observation,
            NAIVE_LAST_PRICE_V1,
            forced_abstention_reasons=tuple(reasons),
        )
        evidence = self.inference_engine.get_model_load_evidence(symbol)
        formula = _formula_for_artifact(evidence)
        model_result: Mapping[str, Any] | None = None
        model_reasons = list(baseline.abstention_reasons)
        declared_features = tuple(
            str(name) for name in (evidence.get("feature_names") or ())
        )
        if not declared_features:
            model_reasons.append("MODEL_FEATURE_CONTRACT_MISSING")
        predicted_price: float | None = None
        raw_output: float | None = None
        features: dict[str, Any] = {}

        if not model_reasons:
            model_result = self.inference_engine.predict_from_live_payload(
                symbol,
                payload,
                current_price=spot,
            )
            if not isinstance(model_result, Mapping):
                model_reasons.append("MODEL_INFERENCE_ABSTAINED")
            else:
                raw_output = _finite(model_result.get("predicted_value"))
                contract = model_result.get("output_contract")
                features = dict(model_result.get("feature_snapshot") or {})
                if tuple(features) != declared_features:
                    model_reasons.append("MODEL_FEATURE_SNAPSHOT_CONTRACT_MISMATCH")
                elif any(_finite(features.get(name)) is None for name in declared_features):
                    model_reasons.append("MODEL_FEATURE_SNAPSHOT_NONFINITE")
                if (
                    str(model_result.get("artifact_sha256") or "")
                    != str(evidence.get("artifact_sha256") or "")
                ):
                    model_reasons.append("MODEL_ARTIFACT_IDENTITY_MISMATCH")
                if not isinstance(contract, Mapping):
                    model_reasons.append("MODEL_OUTPUT_CONTRACT_MISSING")
                elif dict(contract) != dict(evidence.get("output_contract") or {}):
                    model_reasons.append("MODEL_OUTPUT_CONTRACT_MISMATCH")
                if raw_output is None:
                    model_reasons.append("MODEL_OUTPUT_NONFINITE")
                elif isinstance(contract, Mapping) and contract.get("kind") == "future_price":
                    predicted_price = raw_output
                else:
                    model_reasons.append("MODEL_OUTPUT_KIND_UNSUPPORTED")
                if predicted_price is not None and (
                    not math.isfinite(predicted_price) or predicted_price <= 0
                ):
                    model_reasons.append("MODEL_PREDICTED_PRICE_INVALID")
                    predicted_price = None
                if predicted_price is not None and spot is not None:
                    contract_limit = _finite(
                        contract.get("max_abs_move_bps")
                        if isinstance(contract, Mapping)
                        else None
                    )
                    move_bps = (predicted_price - spot) / spot * 10_000.0
                    if (
                        contract_limit is None
                        or contract_limit <= 0
                        or contract_limit
                        > CUDA_SHADOW_MAX_ABS_MOVE_BPS_HARD_CEILING
                        or abs(move_bps) > contract_limit
                    ):
                        model_reasons.append("MODEL_MOVE_EXCEEDS_CONTRACT_BOUND")
                        predicted_price = None

        model_reasons = list(dict.fromkeys(model_reasons))
        abstained = bool(model_reasons)
        if abstained:
            predicted_price = None
        predicted_delta_points = (
            predicted_price - spot
            if predicted_price is not None and spot is not None
            else None
        )
        predicted_delta_bps = (
            predicted_delta_points / spot * 10_000.0
            if predicted_delta_points is not None and spot
            else None
        )
        confidence = _finite((model_result or {}).get("confidence")) or 0.0
        confidence = max(0.0, min(1.0, confidence)) if not abstained else 0.0
        artifact_sha256 = str(
            (model_result or {}).get("artifact_sha256")
            or evidence.get("artifact_sha256")
            or "missing-artifact"
        )
        prediction_identity = "|".join(
            (
                formula.identity,
                symbol,
                baseline.prediction_timestamp_utc.isoformat(),
                str(payload.get("calculation_id") or "missing-calculation"),
                str(payload.get("subscription_epoch_id") or "missing-epoch"),
                str(payload.get("subscription_generation") or "missing-generation"),
                artifact_sha256,
            )
        )
        provenance = {
            **dict(baseline.provenance),
            "research_only": True,
            "promotion_allowed": False,
            "primary_prediction_replaced": False,
            "model_load_evidence": evidence,
            "artifact_feature_contract": {
                "required_raw_features": list(declared_features),
                "feature_schema_version": evidence.get("feature_schema_version"),
            },
            "inference_device": str(self.inference_engine.device),
            "inference_time_ms": _finite(
                (model_result or {}).get("inference_time_ms")
            ),
            "output_contract": dict(
                (model_result or {}).get("output_contract") or {}
            ),
            "model_feature_asof_utc": {
                name: index_at.isoformat().replace("+00:00", "Z")
                if index_at is not None
                else None
                for name in declared_features
            },
        }
        prediction = replace(
            baseline,
            prediction_id=hashlib.sha256(prediction_identity.encode("utf-8")).hexdigest(),
            formula=formula,
            predicted_price=predicted_price,
            predicted_delta_points=predicted_delta_points,
            predicted_delta_bps=predicted_delta_bps,
            uncertainty_bps=None,
            confidence=confidence,
            confidence_kind="uncalibrated internal score; not a probability",
            abstained=abstained,
            abstention_reasons=tuple(model_reasons),
            raw_feature_snapshot=features,
            transformed_features=(
                {"model_raw_output": raw_output}
                if raw_output is not None
                else {}
            ),
            provenance=provenance,
            mode="shadow",
            production_signal_replaced=False,
        )
        saved = self.journal.save(prediction)
        return CudaShadowRecordResult(
            prediction=prediction,
            saved_count=1 if saved else 0,
        )


async def run_cuda_shadow_research_loop(
    streamer: Any,
    recorder: CudaShadowResearchRecorder,
    *,
    poll_seconds: float = 1.0,
) -> None:
    """Observe committed payloads independently; never delay primary publication."""
    ready_symbols = set(recorder.ready_symbols)
    if not ready_symbols:
        logger.info("CUDA shadow worker inactive: %s", recorder.status()["status"])
        return
    seen: dict[str, tuple[str, int, str]] = {}
    while True:
        latest_by_symbol = streamer.get_all_latest()
        for raw_symbol, raw_payload in latest_by_symbol.items():
            symbol = str(raw_symbol or "").strip().upper()
            if symbol not in ready_symbols or not isinstance(raw_payload, Mapping):
                continue
            calculation_id = str(raw_payload.get("calculation_id") or "").strip()
            process_identity = _process_identity(raw_payload)
            if not calculation_id or process_identity is None:
                continue
            identity = (*process_identity, calculation_id)
            if seen.get(symbol) == identity:
                continue
            # Snapshot before handing work to a separate thread so later stream
            # mutation cannot alter point-in-time research inputs.
            payload = copy.deepcopy(dict(raw_payload))
            result = await asyncio.to_thread(recorder.record, payload)
            seen[symbol] = identity
            if result.error:
                logger.warning("CUDA shadow worker error for %s: %s", symbol, result.error)
        await asyncio.sleep(max(0.25, float(poll_seconds)))
