import json
from types import SimpleNamespace

import pytest
import requests

from tools import configure_diagnostic_access as access


@pytest.mark.parametrize("outcome,expected,exit_code", [
    (200, "AUTHENTICATED", 0), (401, "ACCESS_NOT_VERIFIED", 1),
    (requests.ReadTimeout("private token must never appear"), "API_TIMEOUT", 1),
    (requests.ConnectionError("private token must never appear"), "API_UNAVAILABLE", 1),
])
def test_bounded_authenticated_probe_preserves_failures_without_exposing_secrets(monkeypatch, capsys, outcome, expected, exit_code):
    monkeypatch.setattr(access.sys, "argv", ["check", "--check-live"])
    monkeypatch.setattr(access, "governance_token", lambda: "private-token-example")

    def get(url, **kwargs):
        assert url == "http://127.0.0.1:8000/v1/workstation/passports"
        assert kwargs["params"] == {"limit": 1}
        assert kwargs["timeout"] == (2, 10)
        assert kwargs["allow_redirects"] is False
        if isinstance(outcome, Exception):
            raise outcome
        return SimpleNamespace(status_code=outcome)

    monkeypatch.setattr(requests, "get", get)
    assert access.main() == exit_code
    output = capsys.readouterr().out
    assert "private" not in output
    assert json.loads(output)["runtime_loading"] == expected
