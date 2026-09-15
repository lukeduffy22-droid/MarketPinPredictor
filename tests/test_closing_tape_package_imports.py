import json
import subprocess
import sys


def test_readiness_import_does_not_load_replay_or_model_dependencies():
    code = """
import json
import sys
from backend.closing_tape.readiness import audit_training_readiness
print(json.dumps({
    "callable": callable(audit_training_readiness),
    "pandas": "pandas" in sys.modules,
    "databento": "databento" in sys.modules,
    "torch": "torch" in sys.modules,
    "finalize": "backend.closing_tape.finalize" in sys.modules,
}))
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload == {
        "callable": True,
        "pandas": False,
        "databento": False,
        "torch": False,
        "finalize": False,
    }


def test_lazy_package_exports_preserve_public_api():
    import backend.closing_tape as closing_tape

    from backend.closing_tape.config import FeedSpec
    from backend.closing_tape.readiness import audit_training_readiness

    assert closing_tape.FeedSpec is FeedSpec
    assert closing_tape.audit_training_readiness is audit_training_readiness
