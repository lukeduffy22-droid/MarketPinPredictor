import json
from pathlib import Path
import subprocess
import sys

import pytest

from tools.source_validation import run_validation, source_manifest, verify_receipt


@pytest.fixture
def checkout(tmp_path):
    root = tmp_path / "checkout"
    root.mkdir()
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    (root / "app.py").write_text("answer = 42\n")
    subprocess.run(["git", "add", "app.py"], cwd=root, check=True)
    subprocess.run(["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "fixture"], cwd=root, check=True)
    return root


def test_receipt_detects_untracked_runtime_module_and_later_edits(checkout, tmp_path):
    receipt = run_validation(checkout, tmp_path / "run", [sys.executable, "-c", "assert 2 + 2 == 4"])
    assert verify_receipt(checkout, receipt, receipt_path=tmp_path / "run/receipt.json")["valid"]
    (checkout / "backend").mkdir()
    (checkout / "backend" / "required.py").write_text("value = 1\n")
    result = verify_receipt(checkout, receipt)
    assert not result["valid"]
    assert "backend/required.py" in result["changed_paths"]


def test_successful_command_cannot_certify_source_it_mutated(checkout, tmp_path):
    receipt = run_validation(checkout, tmp_path / "run", [sys.executable, "-c", "from pathlib import Path; Path('app.py').write_text('answer = 0\\n')"])
    assert receipt["exit_code"] == 0
    assert not receipt["source_stable"]
    assert "SOURCE_CHANGED_DURING_VALIDATION" in verify_receipt(checkout, receipt)["reasons"]


def test_failure_and_receipt_tampering_do_not_validate(checkout, tmp_path):
    receipt = run_validation(checkout, tmp_path / "run", [sys.executable, "-c", "raise SystemExit(7)"])
    assert receipt["exit_code"] == 7
    assert not verify_receipt(checkout, receipt)["valid"]
    receipt["exit_code"] = 0
    assert "RECEIPT_DIGEST_MISMATCH" in verify_receipt(checkout, receipt)["reasons"]


def test_environment_database_and_runtime_artifacts_are_isolated(checkout, tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///C:/production/market_data.db")
    monkeypatch.setenv("DATABENTO_API_KEY", "not-a-real-secret")
    (checkout / ".env").write_text("private=true")
    (checkout / "data").mkdir()
    (checkout / "data" / "market_data.db").write_bytes(b"retained")
    out = tmp_path / "run"
    command = [sys.executable, "-c", "import os; from pathlib import Path; assert os.environ['DATABENTO_API_KEY'] == ''; assert 'production' not in os.environ['DATABASE_URL']; assert os.environ['MARKETPIN_FORECAST_RESEARCH_AUTOSTART'] == '0'"]
    receipt = run_validation(checkout, out, command)
    assert receipt["exit_code"] == 0
    assert set(source_manifest(checkout)) == {"app.py"}
    assert (checkout / "data" / "market_data.db").read_bytes() == b"retained"
    with pytest.raises(FileExistsError):
        run_validation(checkout, out, command)


def test_modified_or_missing_validation_log_invalidates_receipt(checkout, tmp_path):
    output = tmp_path / "run"
    receipt = run_validation(checkout, output, [sys.executable, "-c", "print('passed')"])
    path = output / "receipt.json"
    assert verify_receipt(checkout, receipt, receipt_path=path)["valid"]
    (output / "validation.log").write_text("different evidence")
    assert "VALIDATION_LOG_DIGEST_MISMATCH" in verify_receipt(checkout, receipt, receipt_path=path)["reasons"]
    (output / "validation.log").unlink()
    assert "VALIDATION_LOG_UNAVAILABLE" in verify_receipt(checkout, receipt, receipt_path=path)["reasons"]
