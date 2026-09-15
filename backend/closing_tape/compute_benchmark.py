from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from typing import Iterable


@dataclass(frozen=True)
class DeviceBenchmark:
    device: str
    seconds: float
    samples: int
    samples_per_second: float
    final_loss: float


@dataclass(frozen=True)
class ComputeBenchmarkReport:
    torch_version: str
    cuda_available: bool
    cuda_device_name: str | None
    cuda_capability: tuple[int, int] | None
    input_features: int
    batch_size: int
    iterations: int
    results: tuple[DeviceBenchmark, ...]
    cuda_speedup_over_cpu: float | None
    recommended_training_device: str
    recommendation_reason: str
    accuracy_claim: bool = False

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def benchmark_tabular_training(
    *,
    samples: int = 65_536,
    input_features: int = 64,
    batch_size: int = 4_096,
    iterations: int = 20,
    warmup_iterations: int = 3,
    devices: Iterable[str] = ("cpu", "cuda"),
    minimum_cuda_speedup: float = 1.25,
) -> ComputeBenchmarkReport:
    """Benchmark representative training math; never infer forecast quality."""
    import torch

    if min(samples, input_features, batch_size, iterations) <= 0 or warmup_iterations < 0:
        raise ValueError("benchmark sizes must be positive and warmup cannot be negative")
    torch.manual_seed(7)
    source_x = torch.randn(samples, input_features)
    source_y = torch.randn(samples, 1)
    measurements: list[DeviceBenchmark] = []

    for requested in devices:
        device = str(requested).lower()
        if device not in {"cpu", "cuda"}:
            raise ValueError(f"unsupported benchmark device: {device}")
        if device == "cuda" and not torch.cuda.is_available():
            continue
        torch.manual_seed(7)
        model = torch.nn.Sequential(
            torch.nn.Linear(input_features, 256),
            torch.nn.GELU(),
            torch.nn.Linear(256, 128),
            torch.nn.GELU(),
            torch.nn.Linear(128, 1),
        ).to(device)
        optimizer = torch.optim.AdamW(model.parameters(), lr=1e-3)
        x = source_x.to(device)
        y = source_y.to(device)

        def step(index: int):
            start = (index * batch_size) % samples
            stop = min(start + batch_size, samples)
            if stop - start < batch_size:
                start = 0
                stop = min(batch_size, samples)
            optimizer.zero_grad(set_to_none=True)
            loss = torch.nn.functional.mse_loss(model(x[start:stop]), y[start:stop])
            loss.backward()
            optimizer.step()
            return loss

        final_loss = None
        for index in range(warmup_iterations):
            final_loss = step(index)
        if device == "cuda":
            torch.cuda.synchronize()
        started = time.perf_counter()
        for index in range(iterations):
            final_loss = step(index + warmup_iterations)
        if device == "cuda":
            torch.cuda.synchronize()
        elapsed = time.perf_counter() - started
        processed = min(batch_size, samples) * iterations
        measurements.append(
            DeviceBenchmark(
                device=device,
                seconds=elapsed,
                samples=processed,
                samples_per_second=processed / elapsed,
                final_loss=float(final_loss.detach().cpu()) if final_loss is not None else float("nan"),
            )
        )

    by_device = {item.device: item for item in measurements}
    speedup = None
    if "cpu" in by_device and "cuda" in by_device:
        speedup = by_device["cpu"].seconds / by_device["cuda"].seconds
    if speedup is not None and speedup >= minimum_cuda_speedup:
        recommendation = "cuda"
        reason = (
            f"measured CUDA training speedup {speedup:.3f}x meets "
            f"the {minimum_cuda_speedup:.3f}x gate"
        )
    else:
        recommendation = "cpu"
        reason = (
            "CUDA was unavailable or did not meet the measured speedup gate; "
            "this recommendation concerns compute throughput only"
        )
    capability = torch.cuda.get_device_capability(0) if torch.cuda.is_available() else None
    return ComputeBenchmarkReport(
        torch_version=str(torch.__version__),
        cuda_available=bool(torch.cuda.is_available()),
        cuda_device_name=torch.cuda.get_device_name(0) if torch.cuda.is_available() else None,
        cuda_capability=tuple(capability) if capability is not None else None,
        input_features=input_features,
        batch_size=batch_size,
        iterations=iterations,
        results=tuple(measurements),
        cuda_speedup_over_cpu=speedup,
        recommended_training_device=recommendation,
        recommendation_reason=reason,
        accuracy_claim=False,
    )
