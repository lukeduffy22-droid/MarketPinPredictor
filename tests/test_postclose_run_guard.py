from datetime import date
import hashlib
import json

import pytest

from backend.postclose_run_guard import PostcloseAlreadyRunning, postclose_run_guard
from tools import run_verified_close_postclose as runner


def test_manual_and_scheduled_settlement_are_serialized(tmp_path):
    with postclose_run_guard(tmp_path, "settle"):
        with pytest.raises(PostcloseAlreadyRunning):
            with postclose_run_guard(tmp_path, "catchup"):
                pytest.fail("overlapping settlement acquired the lease")
        with postclose_run_guard(tmp_path, "select"):
            pass  # publication-window selection must remain independent
    with postclose_run_guard(tmp_path, "catchup"):
        pass


def test_lock_releases_after_failure(tmp_path):
    with pytest.raises(ValueError):
        with postclose_run_guard(tmp_path, "settle"):
            raise ValueError("fixture")
    with postclose_run_guard(tmp_path, "settle"):
        pass


def test_busy_cli_does_not_fetch_or_ingest(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(runner, "run_settlement", lambda *a: pytest.fail("must not settle"))
    with postclose_run_guard(tmp_path, "catchup"):
        assert runner.main(["settle", "--trading-date", "2026-10-02", "--project-root", str(tmp_path)]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "BUSY"


@pytest.mark.parametrize("fail", [False, True])
def test_attempt_receipts_preserve_success_or_failure(tmp_path, monkeypatch, fail):
    def operation(*args):
        if fail:
            raise ValueError("official source lacks exact date")
        return {"trading_date": "2026-10-02", "result_path": "fixture-receipt"}
    monkeypatch.setattr(runner, "run_settlement", operation)
    if fail:
        with pytest.raises(ValueError, match="exact date"):
            runner.run_with_attempt_receipt(tmp_path, date(2026, 10, 2), "settle")
    else:
        runner.run_with_attempt_receipt(tmp_path, date(2026, 10, 2), "settle")
    path, = (tmp_path / "data/postclose_attempts/2026-10-02").glob("*.json")
    assert path.stem == hashlib.sha256(path.read_bytes()).hexdigest()
    receipt = json.loads(path.read_text())
    assert receipt["status"] == ("FAILED" if fail else "SUCCEEDED")
    assert receipt["completed_at_utc"] >= receipt["started_at_utc"]
    assert not (tmp_path / "data/postclose_pipeline").exists()
