import copy
import json
from types import SimpleNamespace

import pytest

from backend.passport_analyst import (
    ANALYST_CLAIM_CODES,
    ANALYST_RESPONSE_CODES,
    CLAIM_EVIDENCE_PATHS,
    CLAIM_TEMPLATE_VERSION,
    DEFAULT_ANALYST_MAX_OUTPUT_TOKENS,
    DEFAULT_ANALYST_MODEL,
    DEFAULT_ANALYST_TIMEOUT_SECONDS,
    DEFAULT_REASONING_EFFORT,
    AnalystEvidenceError,
    AnalystServiceError,
    ask_passport,
    verify_analyst_answer,
)


def _passport():
    return {
        "forecast_id": "forecast-1",
        "record_sha256": "a" * 64,
        "state": "RESEARCH_ONLY",
        "decision_grade": False,
        "target": {
            "kind": "official_cash_close",
            "trading_date": "2026-06-18",
            "prediction_timestamp_utc": "2026-06-18T15:30:00+00:00",
            "target_timestamp_utc": "2026-06-18T20:00:00+00:00",
        },
        "prediction": {
            "point_estimate": 7511.25,
            "reference_price": 7500.0,
            "interval_lower": 7490.0,
            "interval_upper": 7530.0,
            "interval_target_coverage": 0.9,
        },
        "provenance": {
            "quote_timestamp_utc": "2026-06-18T15:29:59+00:00",
        },
        "quality": {
            "data_age_seconds": 1.5,
            "quote_age_seconds": 0.5,
            "source_validation_is_valid": True,
            "missing_evidence": [],
        },
        "model": {
            "model_version": "gex-v1",
            "model_type": "Databento Quant Ensemble",
            "model_artifact_sha256": "b" * 64,
        },
        "replay": {"status": "VERIFIED"},
        "outcome": None,
    }


def _claim(code, *, paths=None, **extra):
    return {
        "claim_code": code,
        "evidence_paths": list(
            CLAIM_EVIDENCE_PATHS[code] if paths is None else paths
        ),
        **extra,
    }


def _selection(*codes, response_code="ANSWERED"):
    return {
        "response_code": response_code,
        "claims": [_claim(code) for code in codes],
    }


def test_server_renders_allowlisted_claims_and_numeric_bindings():
    codes = (
        "STATE_AND_AUTHORITY",
        "TARGET_WINDOW",
        "POINT_VS_REFERENCE",
        "FORECAST_INTERVAL",
        "SOURCE_FRESHNESS",
        "EVIDENCE_GAPS",
        "MODEL_PROVENANCE",
        "REPLAY_STATUS",
    )

    verified = verify_analyst_answer(_selection(*codes), _passport())

    assert verified["response_code"] == "ANSWERED"
    assert verified["claim_template_version"] == CLAIM_TEMPLATE_VERSION
    assert verified["summary"] == (
        "The immutable passport supports the factual findings below."
    )
    assert [finding["claim_code"] for finding in verified["findings"]] == list(codes)
    assert verified["findings"][2]["statement"] == (
        "The stored point estimate 7511.25 is above the stored reference price 7500."
    )
    assert verified["findings"][2]["evidence_paths"] == list(
        CLAIM_EVIDENCE_PATHS["POINT_VS_REFERENCE"]
    )
    assert verified["numeric_claims"] == [
        {
            "value": 7511.25,
            "evidence_path": "prediction.point_estimate",
            "finding_index": 2,
        },
        {
            "value": 7500.0,
            "evidence_path": "prediction.reference_price",
            "finding_index": 2,
        },
        {
            "value": 7490.0,
            "evidence_path": "prediction.interval_lower",
            "finding_index": 3,
        },
        {
            "value": 7530.0,
            "evidence_path": "prediction.interval_upper",
            "finding_index": 3,
        },
        {
            "value": 0.9,
            "evidence_path": "prediction.interval_target_coverage",
            "finding_index": 3,
        },
        {
            "value": 1.5,
            "evidence_path": "quality.data_age_seconds",
            "finding_index": 4,
        },
        {
            "value": 0.5,
            "evidence_path": "quality.quote_age_seconds",
            "finding_index": 4,
        },
    ]
    assert verified["evidence_paths_verified"] is True
    assert verified["numeric_fields_verified"] is True
    assert verified["prohibited_action_checks_passed"] is True
    assert verified["verification_scope"] == "allowlisted_claim_templates_v1"


def test_server_renders_outcome_comparison_and_numbers():
    passport = _passport()
    passport["outcome"] = {
        "actual_close": 7509.0,
        "absolute_error_points": 2.25,
        "baseline_absolute_error_points": 7.0,
    }

    verified = verify_analyst_answer(
        _selection("OUTCOME_VS_BASELINE"), passport
    )

    assert verified["findings"][0]["statement"] == (
        "The stored actual close is 7509; model absolute error 2.25 is smaller than "
        "baseline absolute error 7."
    )
    assert [claim["value"] for claim in verified["numeric_claims"]] == [
        7509.0,
        2.25,
        7.0,
    ]


def test_server_renders_explicit_unavailable_facts_only_when_absent():
    passport = _passport()
    passport["prediction"]["point_estimate"] = None

    verified = verify_analyst_answer(
        _selection("POINT_UNAVAILABLE", "OUTCOME_UNAVAILABLE"), passport
    )

    assert [finding["statement"] for finding in verified["findings"]] == [
        "The passport does not contain a point estimate.",
        "The passport does not contain an outcome observation.",
    ]
    assert verified["numeric_claims"] == []


@pytest.mark.parametrize(
    ("response_code", "expected_summary"),
    [
        (
            "INSUFFICIENT_EVIDENCE",
            "The immutable passport does not contain an allowlisted fact that answers "
            "this question.",
        ),
        (
            "OUT_OF_SCOPE",
            "This analyst only reports allowlisted facts from the immutable passport.",
        ),
    ],
)
def test_non_answer_statuses_render_fixed_server_responses(
    response_code, expected_summary
):
    verified = verify_analyst_answer(
        _selection(response_code=response_code), _passport()
    )

    assert verified["response_code"] == response_code
    assert verified["summary"] == expected_summary
    assert verified["findings"] == []
    assert verified["numeric_claims"] == []


@pytest.mark.parametrize(
    "selection",
    [
        {
            "response_code": "ANSWERED",
            "claims": [_claim("STATE_AND_AUTHORITY")],
            "summary": "Markets are collapsing.",
        },
        {
            "response_code": "ANSWERED",
            "claims": [
                _claim(
                    "STATE_AND_AUTHORITY",
                    statement="The forecast has perfect provenance.",
                )
            ],
        },
    ],
)
def test_model_supplied_prose_or_extra_fields_are_rejected(selection):
    with pytest.raises(AnalystEvidenceError, match="unexpected fields"):
        verify_analyst_answer(selection, _passport())


def test_unknown_and_duplicate_claim_codes_are_rejected():
    unknown = {
        "response_code": "ANSWERED",
        "claims": [{"claim_code": "PERFECT_PROVENANCE", "evidence_paths": []}],
    }
    duplicate = _selection("STATE_AND_AUTHORITY", "STATE_AND_AUTHORITY")

    with pytest.raises(AnalystEvidenceError, match="unknown analyst claim code"):
        verify_analyst_answer(unknown, _passport())
    with pytest.raises(AnalystEvidenceError, match="duplicate analyst claim code"):
        verify_analyst_answer(duplicate, _passport())


@pytest.mark.parametrize(
    "paths",
    [
        ["state"],
        ["decision_grade", "state"],
        ["state", "decision_grade", "model.model_version"],
    ],
)
def test_claim_paths_must_equal_the_allowlisted_tuple(paths):
    selection = {
        "response_code": "ANSWERED",
        "claims": [_claim("STATE_AND_AUTHORITY", paths=paths)],
    }

    with pytest.raises(AnalystEvidenceError, match="paths do not match"):
        verify_analyst_answer(selection, _passport())


@pytest.mark.parametrize(
    "selection",
    [
        {"response_code": "ANSWERED", "claims": []},
        _selection("STATE_AND_AUTHORITY", response_code="INSUFFICIENT_EVIDENCE"),
        _selection("STATE_AND_AUTHORITY", response_code="OUT_OF_SCOPE"),
        {"response_code": "UNVERIFIED", "claims": []},
    ],
)
def test_response_code_and_claim_status_mismatches_are_rejected(selection):
    with pytest.raises(AnalystEvidenceError):
        verify_analyst_answer(selection, _passport())


def test_claim_count_is_bounded_before_rendering():
    selection = _selection(*ANALYST_CLAIM_CODES[:9])

    with pytest.raises(AnalystEvidenceError, match="bounded list"):
        verify_analyst_answer(selection, _passport())


def test_missing_nonfinite_and_wrong_typed_evidence_fail_closed():
    missing = _passport()
    del missing["prediction"]["reference_price"]
    nonfinite = _passport()
    nonfinite["prediction"]["point_estimate"] = float("nan")
    wrong_type = _passport()
    wrong_type["decision_grade"] = "false"

    with pytest.raises(AnalystEvidenceError, match="unknown evidence path"):
        verify_analyst_answer(_selection("POINT_VS_REFERENCE"), missing)
    with pytest.raises(AnalystEvidenceError, match="must be finite"):
        verify_analyst_answer(_selection("POINT_VS_REFERENCE"), nonfinite)
    with pytest.raises(AnalystEvidenceError, match="must be boolean"):
        verify_analyst_answer(_selection("STATE_AND_AUTHORITY"), wrong_type)


def test_present_evidence_cannot_be_labeled_unavailable():
    with pytest.raises(AnalystEvidenceError, match="point estimate is present"):
        verify_analyst_answer(_selection("POINT_UNAVAILABLE"), _passport())


def test_analyst_uses_typed_schema_and_safe_defaults(monkeypatch):
    payload = _selection("POINT_VS_REFERENCE")
    monkeypatch.delenv("MARKETPIN_ANALYST_MODEL", raising=False)
    monkeypatch.delenv("MARKETPIN_ANALYST_REASONING_EFFORT", raising=False)

    assert DEFAULT_ANALYST_MODEL == "gpt-5.4-mini"
    assert DEFAULT_REASONING_EFFORT == "medium"

    class _Responses:
        def __init__(self):
            self.kwargs = None

        def create(self, **kwargs):
            self.kwargs = kwargs
            return SimpleNamespace(output_text=json.dumps(payload))

    responses = _Responses()
    result = ask_passport(
        _passport(),
        "What is the estimate relative to the reference?",
        client=SimpleNamespace(responses=responses),
    )

    assert result["model"] == DEFAULT_ANALYST_MODEL
    assert result["record_sha256"] == "a" * 64
    assert result["findings"][0]["statement"].startswith(
        "The stored point estimate 7511.25"
    )
    assert responses.kwargs["reasoning"] == {"effort": DEFAULT_REASONING_EFFORT}
    assert responses.kwargs["max_output_tokens"] == DEFAULT_ANALYST_MAX_OUTPUT_TOKENS
    assert responses.kwargs["timeout"] == DEFAULT_ANALYST_TIMEOUT_SECONDS
    assert responses.kwargs["store"] is False
    assert "Return no prose and no numbers" in responses.kwargs["instructions"]
    output_format = responses.kwargs["text"]["format"]
    assert output_format["strict"] is True
    assert output_format["schema"]["additionalProperties"] is False
    assert set(output_format["schema"]["properties"]) == {
        "response_code",
        "claims",
    }
    assert output_format["schema"]["properties"]["response_code"]["enum"] == list(
        ANALYST_RESPONSE_CODES
    )
    assert output_format["schema"]["properties"]["claims"]["items"][
        "properties"
    ]["claim_code"]["enum"] == list(ANALYST_CLAIM_CODES)
    sent_input = json.loads(responses.kwargs["input"])
    assert sent_input["claim_evidence_paths"] == {
        code: list(paths) for code, paths in CLAIM_EVIDENCE_PATHS.items()
    }


def test_ask_rejects_model_supplied_prose_even_if_client_ignores_schema():
    payload = {
        **_selection("STATE_AND_AUTHORITY"),
        "summary": "Markets are collapsing and the forecast has perfect provenance.",
    }

    class _Responses:
        def create(self, **_kwargs):
            return SimpleNamespace(output_text=json.dumps(payload))

    with pytest.raises(AnalystEvidenceError, match="unexpected fields"):
        ask_passport(
            _passport(),
            "What is stored?",
            client=SimpleNamespace(responses=_Responses()),
        )


def test_analyst_honors_explicit_model_and_max_reasoning(monkeypatch):
    monkeypatch.setenv("MARKETPIN_ANALYST_MODEL", "gpt-5.6-sol")
    monkeypatch.setenv("MARKETPIN_ANALYST_REASONING_EFFORT", "max")

    class _Responses:
        def __init__(self):
            self.kwargs = None

        def create(self, **kwargs):
            self.kwargs = kwargs
            return SimpleNamespace(output_text=json.dumps(_selection("REPLAY_STATUS")))

    responses = _Responses()
    result = ask_passport(
        _passport(), "What is stored?", client=SimpleNamespace(responses=responses)
    )

    assert result["model"] == "gpt-5.6-sol"
    assert responses.kwargs["reasoning"] == {"effort": "max"}


def test_analyst_reports_exhausted_api_credits():
    class _QuotaError(RuntimeError):
        code = "insufficient_quota"

    class _Responses:
        def create(self, **_kwargs):
            raise _QuotaError("no credits remaining")

    with pytest.raises(AnalystServiceError, match="no remaining credits"):
        ask_passport(
            _passport(),
            "What is stored?",
            client=SimpleNamespace(responses=_Responses()),
        )


def test_placeholder_api_key_fails_before_client_or_model_call(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-your-api-key-placeholder")

    def _must_not_construct_client(*_args, **_kwargs):
        raise AssertionError("OpenAI client must not be constructed for placeholder credentials")

    monkeypatch.setattr("openai.OpenAI", _must_not_construct_client)

    with pytest.raises(AnalystServiceError, match="not configured with a valid key"):
        ask_passport(_passport(), "What is stored?")


def test_rejected_api_key_has_clean_error_without_key_material():
    class _AuthenticationError(RuntimeError):
        status_code = 401
        code = "invalid_api_key"

    class _Responses:
        def create(self, **_kwargs):
            raise _AuthenticationError("Incorrect API key provided: secret-value")

    with pytest.raises(
        AnalystServiceError, match="configured OpenAI API key was rejected"
    ) as exc_info:
        ask_passport(
            _passport(),
            "What is stored?",
            client=SimpleNamespace(responses=_Responses()),
        )

    assert "secret-value" not in str(exc_info.value)


def test_analyst_rejects_incomplete_model_response():
    class _Responses:
        def create(self, **_kwargs):
            return SimpleNamespace(
                status="incomplete",
                incomplete_details=SimpleNamespace(reason="max_output_tokens"),
                output_text="",
            )

    with pytest.raises(
        AnalystServiceError,
        match=r"incomplete response \(max_output_tokens\)",
    ):
        ask_passport(
            _passport(),
            "What is stored?",
            client=SimpleNamespace(responses=_Responses()),
        )


def test_analyst_rejects_reasoning_effort_not_supported_by_sol(monkeypatch):
    monkeypatch.setenv("MARKETPIN_ANALYST_REASONING_EFFORT", "minimal")

    with pytest.raises(
        AnalystServiceError,
        match="MARKETPIN_ANALYST_REASONING_EFFORT is unsupported",
    ):
        ask_passport(
            _passport(),
            "What is stored?",
            client=SimpleNamespace(responses=SimpleNamespace()),
        )
