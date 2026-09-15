from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from starlette.requests import Request

from backend.api.routers import passports as passport_router
from backend.passport_analyst import AnalystEvidenceError
from backend.prediction_passport import PassportIntegrityError


TOKEN = "test-governance-token-123"
TOKEN_HEADER = {"X-MarketPin-Governance-Token": TOKEN}


def _request(host: str) -> Request:
    return Request(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": "/",
            "raw_path": b"/",
            "query_string": b"",
            "root_path": "",
            "headers": [],
            "client": (host, 50000),
            "server": ("localhost", 80),
        }
    )


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(passport_router.router)
    return TestClient(app, client=("127.0.0.1", 50000))


def setup_function() -> None:
    with passport_router._ANALYST_RATE_LOCK:
        passport_router._ANALYST_REQUESTS.clear()


def test_governance_access_fails_closed_and_requires_loopback(monkeypatch):
    monkeypatch.delenv("MARKETPIN_GOVERNANCE_ACCESS_TOKEN", raising=False)
    try:
        passport_router._governance_access(_request("127.0.0.1"), None)
    except HTTPException as exc:
        assert exc.status_code == 503
    else:
        raise AssertionError("missing governance configuration must fail closed")

    monkeypatch.setenv("MARKETPIN_GOVERNANCE_ACCESS_TOKEN", TOKEN)
    for supplied in (None, "wrong-token"):
        try:
            passport_router._governance_access(_request("127.0.0.1"), supplied)
        except HTTPException as exc:
            assert exc.status_code == 401
        else:
            raise AssertionError("missing or invalid governance token must be rejected")

    passport_router._governance_access(_request("127.0.0.1"), TOKEN)
    passport_router._governance_access(_request("::ffff:127.0.0.1"), TOKEN)

    try:
        passport_router._governance_access(_request("192.0.2.10"), TOKEN)
    except HTTPException as exc:
        assert exc.status_code == 403
    else:
        raise AssertionError("remote governance access must be disabled by default")

    monkeypatch.setenv("MARKETPIN_GOVERNANCE_ALLOW_REMOTE", "1")
    passport_router._governance_access(_request("192.0.2.10"), TOKEN)


def test_router_dependency_protects_every_passport_endpoint(monkeypatch):
    monkeypatch.setenv("MARKETPIN_GOVERNANCE_ACCESS_TOKEN", TOKEN)
    client = _client()

    requests = (
        ("get", "/v1/workstation/passports", None),
        ("get", "/v1/workstation/passports/forecast-1", None),
        ("post", "/v1/workstation/passports/forecast-1/replay", None),
        (
            "post",
            "/v1/workstation/passports/forecast-1/ask",
            {"question": "What evidence is stored?"},
        ),
    )
    for method, path, body in requests:
        response = client.request(method, path, json=body)
        assert response.status_code == 401, (method, path, response.text)


def test_openapi_declares_typed_responses_and_api_key_security():
    client = _client()
    schema = client.get("/openapi.json").json()

    assert schema["components"]["securitySchemes"]["APIKeyHeader"] == {
        "type": "apiKey",
        "in": "header",
        "name": "X-MarketPin-Governance-Token",
    }
    operations = (
        ("/v1/workstation/passports", "get", "ForecastPassportListV1"),
        (
            "/v1/workstation/passports/{forecast_id}",
            "get",
            "ForecastPassportDetailV1",
        ),
        (
            "/v1/workstation/passports/{forecast_id}/replay",
            "post",
            "PassportReplayResponseV1",
        ),
        (
            "/v1/workstation/passports/{forecast_id}/ask",
            "post",
            "PassportAnalystResponseV1",
        ),
    )
    for path, method, response_name in operations:
        operation = schema["paths"][path][method]
        assert operation["security"] == [{"APIKeyHeader": []}]
        assert operation["responses"]["200"]["content"]["application/json"][
            "schema"
        ]["$ref"].endswith(f"/{response_name}")

    analyst_schema = schema["components"]["schemas"]["PassportAnalystResponseV1"]
    assert {"response_code", "claim_template_version", "summary", "findings"}.issubset(
        analyst_schema["required"]
    )
    assert analyst_schema["properties"]["response_code"]["enum"] == [
        "ANSWERED",
        "INSUFFICIENT_EVIDENCE",
        "OUT_OF_SCOPE",
    ]
    assert analyst_schema["properties"]["claim_template_version"]["const"] == (
        "passport-claims-v1"
    )
    finding_schema = schema["components"]["schemas"]["AnalystFindingV1"]
    assert "claim_code" in finding_schema["required"]
    assert "STATE_AND_AUTHORITY" in finding_schema["properties"]["claim_code"][
        "enum"
    ]


def test_list_is_typed_and_integrity_failures_return_conflict(monkeypatch):
    monkeypatch.setenv("MARKETPIN_GOVERNANCE_ACCESS_TOKEN", TOKEN)
    monkeypatch.setattr(
        passport_router, "list_prediction_passport_summaries", lambda **_kwargs: []
    )
    client = _client()

    response = client.get("/v1/workstation/passports", headers=TOKEN_HEADER)
    assert response.status_code == 200
    assert response.json() == {"items": []}

    def _corrupt(**_kwargs):
        raise PassportIntegrityError("passport record hash mismatch")

    monkeypatch.setattr(
        passport_router, "list_prediction_passport_summaries", _corrupt
    )
    response = client.get("/v1/workstation/passports", headers=TOKEN_HEADER)
    assert response.status_code == 409


def test_ask_rejects_blank_questions_and_rate_limits_verified_responses(monkeypatch):
    monkeypatch.setenv("MARKETPIN_GOVERNANCE_ACCESS_TOKEN", TOKEN)
    monkeypatch.setenv("MARKETPIN_ANALYST_REQUESTS_PER_MINUTE", "1")
    monkeypatch.setattr(
        passport_router,
        "read_prediction_passport",
        lambda _forecast_id: {
            "forecast_id": "forecast-1",
            "record_sha256": "a" * 64,
        },
    )
    monkeypatch.setattr(
        passport_router,
        "ask_passport",
        lambda _passport, _question: {
            "response_code": "INSUFFICIENT_EVIDENCE",
            "claim_template_version": "passport-claims-v1",
            "summary": "No numeric claim is required.",
            "findings": [],
            "numeric_claims": [],
            "evidence_paths_verified": True,
            "numeric_fields_verified": True,
            "prohibited_action_checks_passed": True,
            "verification_scope": "allowlisted_claim_templates_v1",
            "model": "test-model",
            "forecast_id": "forecast-1",
            "record_sha256": "a" * 64,
        },
    )
    client = _client()

    blank = client.post(
        "/v1/workstation/passports/forecast-1/ask",
        headers=TOKEN_HEADER,
        json={"question": "   "},
    )
    assert blank.status_code == 422

    first = client.post(
        "/v1/workstation/passports/forecast-1/ask",
        headers=TOKEN_HEADER,
        json={"question": "What evidence is stored?"},
    )
    assert first.status_code == 200
    assert first.json()["prohibited_action_checks_passed"] is True

    second = client.post(
        "/v1/workstation/passports/forecast-1/ask",
        headers=TOKEN_HEADER,
        json={"question": "Repeat the evidence."},
    )
    assert second.status_code == 429


def test_ask_distinguishes_corrupt_evidence_from_ungrounded_model_output(monkeypatch):
    monkeypatch.setenv("MARKETPIN_GOVERNANCE_ACCESS_TOKEN", TOKEN)
    client = _client()

    monkeypatch.setattr(
        passport_router,
        "read_prediction_passport",
        lambda _forecast_id: (_ for _ in ()).throw(
            PassportIntegrityError("passport record hash mismatch")
        ),
    )
    corrupt = client.post(
        "/v1/workstation/passports/forecast-1/ask",
        headers=TOKEN_HEADER,
        json={"question": "What evidence is stored?"},
    )
    assert corrupt.status_code == 409

    monkeypatch.setattr(
        passport_router,
        "read_prediction_passport",
        lambda _forecast_id: {
            "forecast_id": "forecast-1",
            "record_sha256": "a" * 64,
        },
    )
    monkeypatch.setattr(
        passport_router,
        "ask_passport",
        lambda _passport, _question: (_ for _ in ()).throw(
            AnalystEvidenceError("unsupported numeric claim")
        ),
    )
    ungrounded = client.post(
        "/v1/workstation/passports/forecast-1/ask",
        headers=TOKEN_HEADER,
        json={"question": "What evidence is stored?"},
    )
    assert ungrounded.status_code == 502
    assert "unsupported numeric claim" not in ungrounded.text
