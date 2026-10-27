from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from tools import cuda_runtime_probe


class _FakeTensor:
    def __init__(self, values: list[float]) -> None:
        self.values = values

    def __mul__(self, other: "_FakeTensor") -> "_FakeTensor":
        return _FakeTensor([left * right for left, right in zip(self.values, other.values)])

    def item(self) -> float:
        assert len(self.values) == 1
        return self.values[0]


class _FakeCuda:
    def __init__(self, *, available: bool = True) -> None:
        self.available = available
        self.synchronized = False

    def get_arch_list(self) -> list[str]:
        return ["sm_120"]

    def is_available(self) -> bool:
        return self.available

    def device_count(self) -> int:
        return 1 if self.available else 0

    def get_device_properties(self, index: int) -> SimpleNamespace:
        assert index == 0
        return SimpleNamespace(total_memory=8_000_000_000, multi_processor_count=48)

    def get_device_capability(self, index: int) -> tuple[int, int]:
        assert index == 0
        return (12, 0)

    def get_device_name(self, index: int) -> str:
        assert index == 0
        return "Fake RTX"

    def synchronize(self) -> None:
        self.synchronized = True


class _FakeTorch:
    __version__ = "2.10.0+cu128"
    version = SimpleNamespace(cuda="12.8")
    float32 = "float32"

    def __init__(self, *, available: bool = True) -> None:
        self.cuda = _FakeCuda(available=available)

    @staticmethod
    def arange(start: int, stop: int, **_: object) -> _FakeTensor:
        return _FakeTensor([float(value) for value in range(start, stop)])

    @staticmethod
    def sum(tensor: _FakeTensor) -> _FakeTensor:
        return _FakeTensor([sum(tensor.values)])


def test_runtime_attestation_requires_a_real_kernel_result() -> None:
    torch_module = _FakeTorch()

    result = cuda_runtime_probe.collect_runtime_attestation(torch_module=torch_module)

    assert result["status"] == "pass"
    assert result["cuda_available"] is True
    assert result["compiled_arch_list"] == ["sm_120"]
    assert result["devices"][0]["capability"] == [12, 0]
    assert result["kernel_probe"]["matches_expected"] is True
    assert result["nvidia_smi"]["status"] == "not_collected"
    assert torch_module.cuda.synchronized is True


def test_runtime_attestation_fails_when_cuda_is_not_available() -> None:
    result = cuda_runtime_probe.collect_runtime_attestation(
        torch_module=_FakeTorch(available=False),
        nvidia_smi_record={"status": "skipped_for_test"},
    )

    assert result["status"] == "fail"
    assert result["reason"] == "CUDA_NOT_AVAILABLE"
    assert result["kernel_probe"] is None


def test_runtime_attestation_fails_before_import_on_interpreter_mismatch() -> None:
    result = cuda_runtime_probe.collect_runtime_attestation(
        expected_python=Path("Z:/definitely/not/the/running/python.exe"),
        torch_module=object(),
        nvidia_smi_record={"status": "skipped_for_test"},
    )

    assert result["status"] == "fail"
    assert result["reason"] == "INTERPRETER_MISMATCH"
    assert result["torch_imported"] is False


def test_write_attestation_creates_valid_json(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "attestation.json"
    payload = {"schema_version": cuda_runtime_probe.SCHEMA_VERSION, "status": "pass"}

    cuda_runtime_probe.write_attestation(target, payload)

    assert json.loads(target.read_text(encoding="utf-8")) == payload
    assert not list(target.parent.glob("*.tmp"))
