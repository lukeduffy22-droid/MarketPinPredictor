"""Emit a standalone CUDA runtime attestation without importing the application.

The probe is intentionally independent of FastAPI, Databento, and the market-data
runtime so it can be executed safely by the same Windows account that launches the
application.  A successful result requires both CUDA discovery and a synchronized
tensor operation; ``torch.cuda.is_available()`` alone is not sufficient evidence.
"""

from __future__ import annotations

import argparse
import ctypes
import getpass
import json
import os
import platform
import socket
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "marketpin.cuda-runtime-attestation.v1"


def _normalized_path(value: str | os.PathLike[str]) -> str:
    return os.path.normcase(os.path.realpath(os.fspath(value)))


def _windows_session_id() -> int | None:
    if os.name != "nt":
        return None
    session_id = ctypes.c_ulong()
    succeeded = ctypes.windll.kernel32.ProcessIdToSessionId(  # type: ignore[attr-defined]
        os.getpid(), ctypes.byref(session_id)
    )
    return int(session_id.value) if succeeded else None


def _device_record(torch_module: Any, index: int) -> dict[str, Any]:
    properties = torch_module.cuda.get_device_properties(index)
    capability = torch_module.cuda.get_device_capability(index)
    return {
        "index": index,
        "name": str(torch_module.cuda.get_device_name(index)),
        "capability": [int(capability[0]), int(capability[1])],
        "total_memory_bytes": int(properties.total_memory),
        "multi_processor_count": int(properties.multi_processor_count),
    }


def _synchronized_kernel_probe(torch_module: Any) -> dict[str, Any]:
    started = time.perf_counter()
    left = torch_module.arange(1, 257, dtype=torch_module.float32, device="cuda")
    right = torch_module.arange(257, 513, dtype=torch_module.float32, device="cuda")
    observed = float(torch_module.sum(left * right).item())
    torch_module.cuda.synchronize()
    expected = float(sum(i * (i + 256) for i in range(1, 257)))
    return {
        "operation": "cuda_vector_multiply_sum_256",
        "observed": observed,
        "expected": expected,
        "matches_expected": observed == expected,
        "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
    }


def collect_runtime_attestation(
    *,
    expected_python: str | os.PathLike[str] | None = None,
    torch_module: Any | None = None,
    nvidia_smi_record: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Collect interpreter, PyTorch build, discovery, and kernel evidence."""

    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "host": socket.gethostname(),
        "account": getpass.getuser(),
        "process_id": os.getpid(),
        "parent_process_id": os.getppid(),
        "windows_session_id": _windows_session_id(),
        "python_executable": str(Path(sys.executable).resolve()),
        "python_version": platform.python_version(),
        "virtual_env": os.environ.get("VIRTUAL_ENV"),
        "cuda_visible_devices": os.environ.get("CUDA_VISIBLE_DEVICES"),
        "cuda_device_order": os.environ.get("CUDA_DEVICE_ORDER"),
        "nvidia_visible_devices": os.environ.get("NVIDIA_VISIBLE_DEVICES"),
        # Never discover or execute vendor utilities through PATH here. This
        # probe may run as SYSTEM with the repository as its working directory;
        # the PyTorch device/kernel evidence below is the authoritative check.
        "nvidia_smi": nvidia_smi_record
        or {
            "status": "not_collected",
            "reason": "external process execution intentionally disabled",
        },
        "expected_python": (
            str(Path(expected_python).resolve()) if expected_python is not None else None
        ),
        "python_matches_expected": None,
        "torch_imported": False,
        "torch_version": None,
        "torch_cuda_build": None,
        "compiled_arch_list": [],
        "cuda_available": False,
        "cuda_device_count": 0,
        "devices": [],
        "kernel_probe": None,
        "status": "error",
        "reason": None,
    }

    if expected_python is not None:
        result["python_matches_expected"] = _normalized_path(
            sys.executable
        ) == _normalized_path(expected_python)
        if not result["python_matches_expected"]:
            result["status"] = "fail"
            result["reason"] = "INTERPRETER_MISMATCH"
            return result

    try:
        if torch_module is None:
            import torch as torch_module  # type: ignore[no-redef]

        result["torch_imported"] = True
        result["torch_version"] = str(torch_module.__version__)
        result["torch_cuda_build"] = torch_module.version.cuda
        result["compiled_arch_list"] = list(torch_module.cuda.get_arch_list())
        result["cuda_available"] = bool(torch_module.cuda.is_available())
        result["cuda_device_count"] = int(torch_module.cuda.device_count())

        if not result["cuda_available"]:
            result["status"] = "fail"
            result["reason"] = "CUDA_NOT_AVAILABLE"
            return result
        if result["cuda_device_count"] < 1:
            result["status"] = "fail"
            result["reason"] = "CUDA_DEVICE_COUNT_ZERO"
            return result

        result["devices"] = [
            _device_record(torch_module, index)
            for index in range(result["cuda_device_count"])
        ]
        result["kernel_probe"] = _synchronized_kernel_probe(torch_module)
        if not result["kernel_probe"]["matches_expected"]:
            result["status"] = "fail"
            result["reason"] = "CUDA_KERNEL_RESULT_MISMATCH"
            return result

        result["status"] = "pass"
        return result
    except Exception as exc:  # diagnostic boundary: preserve the exact class and text
        result["status"] = "error"
        result["reason"] = "CUDA_PROBE_EXCEPTION"
        result["error_type"] = type(exc).__name__
        result["error_message"] = str(exc)
        return result


def write_attestation(path: Path, payload: dict[str, Any]) -> None:
    """Atomically write one attestation JSON document."""

    target = path.resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, target)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--expect-python",
        help="Fail unless the running interpreter resolves to this exact path.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Atomically save the JSON attestation at this path.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    result = collect_runtime_attestation(expected_python=arguments.expect_python)
    serialized = json.dumps(result, indent=2, sort_keys=True)
    print(serialized)
    if arguments.output is not None:
        write_attestation(arguments.output, result)
    return 0 if result["status"] == "pass" else 2


if __name__ == "__main__":
    raise SystemExit(main())
