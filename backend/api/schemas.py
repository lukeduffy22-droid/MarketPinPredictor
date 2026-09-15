"""Pydantic schemas shared by backend API routers."""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class PredictionResponse(BaseModel):
    symbol: str
    current_price: float
    predicted_close: float
    confidence: float
    inference_time_ms: float
    timestamp: str
    decision_grade: bool = False
    forecast_id: Optional[str] = None
    forecast_state: Optional[str] = None
    state_status: Optional[str] = None
    state_revision: Optional[int] = None
    state_sequence: Optional[int] = None
    prediction_mode: str
    is_estimate: bool
    prediction_authority: Dict[str, Any]


class HealthResponse(BaseModel):
    status: str
    version: str
    cuda_available: bool
    models_loaded: List[str]
    streaming_active: bool
    uptime_seconds: float


class MarketDataResponse(BaseModel):
    symbol: str
    price: float
    timestamp: str
    data_age_seconds: float


class EODCloseRequest(BaseModel):
    close: float
    trading_date: Optional[str] = None
    source: str = "manual-unverified"
    source_reference: Optional[str] = None
    observed_at_utc: Optional[str] = None
    correction_of_id: Optional[int] = None
    prediction_ids: Optional[List[int]] = None


class DatabentoCacheRefreshRequest(BaseModel):
    """Explicit authorization for destructive universe-cache maintenance."""

    confirm: bool = False
    force_maintenance: bool = False


class WorkstationStateV1(BaseModel):
    """Versioned, lifecycle-published state consumed by workstation clients."""

    model_config = ConfigDict(extra="forbid")

    contract_version: Literal["workstation-state.v1"]
    sequence: int
    state_revision: int
    event_id: str
    forecast_id: Optional[str] = None
    forecast_state: Optional[
        Literal["VALID", "RESEARCH_ONLY", "ABSTAIN", "STALE", "UNAVAILABLE"]
    ] = None
    decision_grade: bool
    prediction_snapshot_id: Optional[int] = None
    symbol: str
    status: Literal[
        "warming", "live", "stale", "invalid", "closed_context", "unavailable"
    ]
    generated_at_utc: Optional[str] = None
    source_as_of_utc: Optional[str] = None
    provider: str
    subscription_epoch_id: Optional[str] = None
    subscription_generation: Optional[int] = None
    current_price: Optional[float] = None
    prediction_authority: Dict[str, Any]
    prediction: Optional[Dict[str, Any]] = None
    pin_payload: Optional[Dict[str, Any]] = None
    health: Dict[str, Any]


class WorkstationEventV1(BaseModel):
    """One SSE snapshot/update with explicit replay-gap semantics."""

    model_config = ConfigDict(extra="forbid")

    contract_version: Literal["workstation-events.v1"]
    event_type: Literal["snapshot", "update", "heartbeat", "resync_required"]
    sequence: int
    event_id: str
    requires_resync: bool
    requested_after_sequence: Optional[int] = None
    oldest_available_sequence: Optional[int] = None
    latest_sequence: int
    generated_at_utc: str
    states: List[WorkstationStateV1]


class PassportOriginV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["prediction-passport-v1"]
    origin_kind: str
    origin_key: str
    prediction_snapshot_id: Optional[int] = None


class PassportTargetV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["official_cash_close"]
    trading_date: str
    prediction_timestamp_utc: str
    target_timestamp_utc: str
    horizon_seconds: int = Field(ge=0)


class PassportProvenanceV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    provider: Optional[str] = None
    quote_timestamp_utc: Optional[str] = None
    subscription_epoch_id: Optional[str] = None
    subscription_generation: Optional[int] = None
    calculation_id: Optional[str] = None
    calculation_status: Optional[str] = None
    calculated_at_utc: Optional[str] = None
    input_schema_version: Optional[str] = None
    calculation_input_sha256: Optional[str] = None
    source_payload_sha256: Optional[str] = None
    universe_sha256: Optional[str] = None


class PassportModelEvidenceV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    model_version: Optional[str] = None
    model_type: Optional[str] = None
    model_artifact_sha256: Optional[str] = None
    feature_schema_version: Optional[str] = None
    feature_hash: Optional[str] = None
    formula_versions: Dict[str, Any]
    inference_device: Optional[str] = None
    calibration_method: Optional[str] = None
    calibration_evidence_sha256: Optional[str] = None
    calibration_evidence: Optional[Dict[str, Any]] = None


class PassportBaselineV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: Literal["persistence"]
    value: Optional[float] = None


class PassportPredictionV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    reference_price: Optional[float] = None
    point_estimate: Optional[float] = None
    interval_lower: Optional[float] = None
    interval_upper: Optional[float] = None
    interval_target_coverage: Optional[float] = None
    confidence_raw: Optional[float] = None
    confidence_scale: str
    confidence_kind: str
    baseline: PassportBaselineV1


class PassportQualityV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    validation_status: str
    # Passports issued before this field was captured must remain readable;
    # absence means unknown and must not be inferred from a status label.
    source_validation_is_valid: Optional[bool] = None
    data_age_seconds: Optional[float] = None
    quote_age_seconds: Optional[float] = None
    active_contract_count: Optional[int] = None
    fresh_quote_count: Optional[int] = None
    regime: str
    state_reasons: List[str]
    missing_evidence: List[str]


class PassportReplayV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: Literal["VERIFIED", "MISMATCH", "UNAVAILABLE"]
    reason: Optional[str] = None
    replayed_point_estimate: Optional[float] = None
    stored_point_estimate: Optional[float] = None
    error_points: Optional[float] = None
    tolerance_points: Optional[float] = None


class ForecastPassportV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["prediction-passport-v1"]
    forecast_id: str
    origin: PassportOriginV1
    symbol: str
    prediction_mode: str
    state: Literal[
        "VALID", "RESEARCH_ONLY", "RESEARCH", "ABSTAIN", "STALE", "UNAVAILABLE"
    ]
    decision_grade: bool
    target: PassportTargetV1
    provenance: PassportProvenanceV1
    model: PassportModelEvidenceV1
    prediction: PassportPredictionV1
    quality: PassportQualityV1
    drivers: List[Any]
    feature_snapshot: Dict[str, Any]
    # Older v1 rows may predate issuance-time replay capture. Absence remains
    # absence; callers can use the explicit replay endpoint without changing
    # the hash-bound immutable payload.
    replay: Optional[PassportReplayV1] = None


class PassportOutcomeV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    score_key: str
    close_observation_id: int
    # ``verified`` is the legacy alias for verified close evidence only.
    verified: bool
    close_verified: Literal[True]
    evidence_scope: Literal["diagnostic_research_only"]
    training_eligible: Literal[False]
    performance_claim_eligible: Literal[False]
    actual_close: float
    error_points: float
    absolute_error_points: Optional[float] = None
    baseline_absolute_error_points: Optional[float] = None
    baseline_lift_points: Optional[float] = None
    interval_covered: Optional[bool] = None
    close_source: str
    close_source_reference: str
    close_source_artifact_sha256: str
    scored_at_utc: str


class ForecastPassportDetailV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    passport: ForecastPassportV1
    record_sha256: str
    created_at_utc: str
    outcome: Optional[PassportOutcomeV1] = None


class ForecastPassportSummaryV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    forecast_id: str
    symbol: str
    state: Literal[
        "VALID", "RESEARCH_ONLY", "RESEARCH", "ABSTAIN", "STALE", "UNAVAILABLE"
    ]
    decision_grade: bool
    prediction_mode: str
    prediction_timestamp_utc: str
    target_timestamp_utc: str
    horizon_seconds: int
    provider: Optional[str] = None
    model_version: Optional[str] = None
    point_estimate: Optional[float] = None
    missing_evidence: List[str]
    record_sha256: str
    created_at_utc: str


class ForecastPassportListV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    items: List[ForecastPassportSummaryV1]


class PassportReplayResponseV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    forecast_id: str
    record_sha256: str
    replay: PassportReplayV1


class AnalystFindingV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    claim_code: Literal[
        "STATE_AND_AUTHORITY",
        "TARGET_WINDOW",
        "POINT_VS_REFERENCE",
        "FORECAST_INTERVAL",
        "SOURCE_FRESHNESS",
        "EVIDENCE_GAPS",
        "MODEL_PROVENANCE",
        "REPLAY_STATUS",
        "OUTCOME_VS_BASELINE",
        "POINT_UNAVAILABLE",
        "OUTCOME_UNAVAILABLE",
    ]
    statement: str
    evidence_paths: List[str]


class AnalystNumericClaimV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    value: float
    evidence_path: str
    finding_index: int


class PassportAnalystResponseV1(BaseModel):
    model_config = ConfigDict(extra="forbid")
    response_code: Literal["ANSWERED", "INSUFFICIENT_EVIDENCE", "OUT_OF_SCOPE"]
    claim_template_version: Literal["passport-claims-v1"]
    summary: str
    findings: List[AnalystFindingV1]
    numeric_claims: List[AnalystNumericClaimV1]
    evidence_paths_verified: bool
    numeric_fields_verified: bool
    prohibited_action_checks_passed: bool
    verification_scope: Literal["allowlisted_claim_templates_v1"]
    model: str
    forecast_id: str
    record_sha256: str
