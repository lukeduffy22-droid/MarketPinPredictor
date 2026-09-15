"""Offline CPU-versus-CUDA benchmark for Databento IV/gamma/GEX batching.

This tool never changes the live streamer or its configuration.  It compares
the retained scalar CPU reference, the live NumPy batch implementation, and a
research-only CUDA implementation using float64 and the same 60 bisection
steps.  CUDA timing is reported both device-only and end-to-end (host/device
transfers plus result materialization).
"""

from __future__ import annotations

import argparse
import cProfile
import json
import math
import os
import platform
import statistics
import sys
import time
from pathlib import Path
from typing import Callable

import numpy as np


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.databento_streamer import (  # noqa: E402
    CONTRACT_MULTIPLIER,
    MIN_TTE_DAYS,
    _configured_risk_free_rate,
    batch_iv_gamma_gex,
    black_scholes_gamma,
    black_scholes_price,
    implied_volatility,
)


def _synthetic_inputs(rows: int, seed: int) -> dict[str, object]:
    rng = np.random.default_rng(seed)
    spot = 7_650.0
    strikes = spot * rng.uniform(0.97, 1.03, size=rows)
    years = rng.choice(
        np.asarray([MIN_TTE_DAYS, 1.0 / 365.0, 3.0 / 365.0, 7.0 / 365.0, 30.0 / 365.0]),
        size=rows,
    )
    option_types = np.where(rng.random(rows) < 0.5, "C", "P")
    true_vols = rng.uniform(0.12, 0.65, size=rows)
    open_interest = rng.integers(1, 5_000, size=rows).astype(np.float64)
    mids = np.fromiter(
        (
            black_scholes_price(spot, strike, year, vol, option_type)
            for strike, year, vol, option_type in zip(
                strikes, years, true_vols, option_types, strict=True
            )
        ),
        dtype=np.float64,
        count=rows,
    )
    return {
        "spot": spot,
        "strikes": strikes,
        "years": years,
        "mids": mids,
        "option_types": option_types,
        "open_interest": open_interest,
    }


def _scalar_batch(inputs: dict[str, object]) -> dict[str, np.ndarray]:
    spot = float(inputs["spot"])
    strikes = np.asarray(inputs["strikes"], dtype=np.float64)
    years = np.asarray(inputs["years"], dtype=np.float64)
    mids = np.asarray(inputs["mids"], dtype=np.float64)
    option_types = np.asarray(inputs["option_types"], dtype="U1")
    open_interest = np.asarray(inputs["open_interest"], dtype=np.float64)
    valid_mask = np.zeros(len(strikes), dtype=bool)
    ivs = np.full(len(strikes), np.nan, dtype=np.float64)
    gammas = np.full(len(strikes), np.nan, dtype=np.float64)
    gex = np.full(len(strikes), np.nan, dtype=np.float64)
    for index in range(len(strikes)):
        iv = implied_volatility(
            spot,
            float(strikes[index]),
            float(years[index]),
            float(mids[index]),
            str(option_types[index]),
        )
        if iv is None:
            continue
        gamma = black_scholes_gamma(spot, float(strikes[index]), float(years[index]), iv)
        sign = 1.0 if option_types[index] == "C" else -1.0
        valid_mask[index] = True
        ivs[index] = iv
        gammas[index] = gamma
        gex[index] = sign * gamma * open_interest[index] * CONTRACT_MULTIPLIER
    return {"valid_mask": valid_mask, "iv": ivs, "gamma": gammas, "gex": gex}


def _numpy_batch(inputs: dict[str, object]) -> dict[str, np.ndarray]:
    return batch_iv_gamma_gex(
        inputs["spot"],
        inputs["strikes"],
        inputs["years"],
        inputs["mids"],
        inputs["option_types"],
        inputs["open_interest"],
    )


def _time_cpu(
    function: Callable[[dict[str, object]], dict[str, np.ndarray]],
    inputs: dict[str, object],
    *,
    warmup: int,
    repeats: int,
) -> tuple[dict[str, float], dict[str, np.ndarray]]:
    result = function(inputs)
    for _ in range(warmup):
        result = function(inputs)
    timings_ms = []
    for _ in range(repeats):
        started = time.perf_counter_ns()
        result = function(inputs)
        timings_ms.append((time.perf_counter_ns() - started) / 1_000_000.0)
    ordered = sorted(timings_ms)
    p95_index = min(len(ordered) - 1, math.ceil(len(ordered) * 0.95) - 1)
    return (
        {
            "median_ms": statistics.median(timings_ms),
            "min_ms": min(timings_ms),
            "p95_ms": ordered[p95_index],
        },
        result,
    )


def _profile_cpu(
    name: str,
    function: Callable[[dict[str, object]], dict[str, np.ndarray]],
    inputs: dict[str, object],
    *,
    limit: int = 12,
) -> dict[str, object]:
    profiler = cProfile.Profile()
    profiler.enable()
    function(inputs)
    profiler.disable()
    rows = []
    for entry in profiler.getstats():
        if isinstance(entry.code, str):
            filename = "~"
            line = 0
            function_name = entry.code
        else:
            filename = entry.code.co_filename
            line = entry.code.co_firstlineno
            function_name = entry.code.co_name
        rows.append(
            {
                "function": function_name,
                "file": os.path.basename(filename),
                "line": line,
                "calls": entry.callcount,
                "self_ms": entry.inlinetime * 1_000.0,
                "cumulative_ms": entry.totaltime * 1_000.0,
            }
        )
    rows.sort(key=lambda row: row["cumulative_ms"], reverse=True)
    return {"name": name, "top_by_cumulative_time": rows[:limit]}


def _error_summary(reference: dict[str, np.ndarray], candidate: dict[str, np.ndarray]) -> dict[str, object]:
    same_mask = np.array_equal(reference["valid_mask"], candidate["valid_mask"])
    comparable = reference["valid_mask"] & candidate["valid_mask"]
    summary: dict[str, object] = {
        "same_valid_mask": bool(same_mask),
        "reference_valid_rows": int(reference["valid_mask"].sum()),
        "candidate_valid_rows": int(candidate["valid_mask"].sum()),
    }
    all_close = same_mask
    tolerances = {
        # IV is an intermediate root, not the published signal.  A 1e-8
        # absolute/relative tolerance is substantially tighter than market
        # quote precision while avoiding false failures for nearly intrinsic,
        # minimum-TTE options where the root is ill-conditioned.
        "iv": (1e-8, 1e-8),
        "gamma": (1e-8, 1e-11),
        "gex": (1e-8, 1e-8),
    }
    for field, (rtol, atol) in tolerances.items():
        if np.any(comparable):
            difference = np.abs(reference[field][comparable] - candidate[field][comparable])
            maximum = float(np.max(difference))
            close = bool(
                np.allclose(
                    reference[field][comparable],
                    candidate[field][comparable],
                    rtol=rtol,
                    atol=atol,
                )
            )
        else:
            maximum = None
            close = True
        summary[f"{field}_max_abs_error"] = maximum
        summary[f"{field}_within_tolerance"] = close
        all_close = all_close and close
    summary["numerically_equivalent"] = bool(all_close)
    return summary


def _torch_cuda_benchmark(
    inputs: dict[str, object],
    numpy_reference: dict[str, np.ndarray],
    *,
    warmup: int,
    repeats: int,
) -> dict[str, object]:
    try:
        import torch
    except ImportError as exc:
        return {"available": False, "reason": f"PyTorch unavailable: {exc}"}
    if not torch.cuda.is_available():
        return {"available": False, "reason": "torch.cuda.is_available() is false"}

    device = torch.device("cuda:0")
    risk_free_rate = _configured_risk_free_rate()

    def to_device() -> dict[str, object]:
        return {
            "spot": float(inputs["spot"]),
            "strikes": torch.as_tensor(inputs["strikes"], dtype=torch.float64, device=device),
            "years": torch.as_tensor(inputs["years"], dtype=torch.float64, device=device),
            "mids": torch.as_tensor(inputs["mids"], dtype=torch.float64, device=device),
            "is_call": torch.as_tensor(
                np.asarray(inputs["option_types"]) == "C", dtype=torch.bool, device=device
            ),
            "is_put": torch.as_tensor(
                np.asarray(inputs["option_types"]) == "P", dtype=torch.bool, device=device
            ),
            "open_interest": torch.as_tensor(
                inputs["open_interest"], dtype=torch.float64, device=device
            ),
        }

    def price(
        spot: float,
        strikes,
        years,
        vols,
        is_call,
    ):
        sqrt_years = torch.sqrt(years)
        d1 = (
            torch.log(spot / strikes)
            + (risk_free_rate + 0.5 * vols * vols) * years
        ) / (vols * sqrt_years)
        d2 = d1 - vols * sqrt_years
        discounted_strikes = strikes * torch.exp(-risk_free_rate * years)
        normal_cdf_d1 = 0.5 * (1.0 + torch.erf(d1 / math.sqrt(2.0)))
        normal_cdf_d2 = 0.5 * (1.0 + torch.erf(d2 / math.sqrt(2.0)))
        call_prices = spot * normal_cdf_d1 - discounted_strikes * normal_cdf_d2
        put_prices = (
            discounted_strikes * (1.0 - normal_cdf_d2)
            - spot * (1.0 - normal_cdf_d1)
        )
        return torch.where(is_call, call_prices, put_prices)

    def calculate(device_inputs: dict[str, object]):
        spot = device_inputs["spot"]
        strikes = device_inputs["strikes"]
        years = device_inputs["years"]
        mids = device_inputs["mids"]
        is_call = device_inputs["is_call"]
        is_put = device_inputs["is_put"]
        open_interest = device_inputs["open_interest"]
        intrinsic = torch.where(
            is_call,
            torch.clamp(spot - strikes, min=0.0),
            torch.clamp(strikes - spot, min=0.0),
        )
        base_valid = (
            torch.isfinite(strikes)
            & torch.isfinite(years)
            & torch.isfinite(mids)
            & torch.isfinite(open_interest)
            & (strikes > 0.0)
            & (years > 0.0)
            & (is_call | is_put)
            & (mids > intrinsic)
        )
        low = torch.full_like(strikes, 0.0001)
        high = torch.full_like(strikes, 5.0)
        lower_errors = price(spot, strikes, years, low, is_call) - mids
        upper_errors = price(spot, strikes, years, high, is_call) - mids
        valid = (
            base_valid
            & torch.isfinite(lower_errors)
            & torch.isfinite(upper_errors)
            & (lower_errors <= 0.0)
            & (upper_errors >= 0.0)
        )
        low_at_root = valid & (lower_errors == 0.0)
        high_at_root = valid & (upper_errors == 0.0)
        active = valid & ~(low_at_root | high_at_root)
        for _ in range(60):
            midpoint = (low + high) * 0.5
            midpoint_errors = price(spot, strikes, years, midpoint, is_call) - mids
            low = torch.where(active & (midpoint_errors < 0.0), midpoint, low)
            high = torch.where(active & (midpoint_errors >= 0.0), midpoint, high)
        iv = torch.where(
            low_at_root,
            torch.full_like(low, 0.0001),
            torch.where(high_at_root, torch.full_like(high, 5.0), (low + high) * 0.5),
        )
        sqrt_years = torch.sqrt(years)
        d1 = (
            torch.log(spot / strikes)
            + (risk_free_rate + 0.5 * iv * iv) * years
        ) / (iv * sqrt_years)
        gamma = (
            torch.exp(-0.5 * d1 * d1)
            / math.sqrt(2.0 * math.pi)
            / (spot * iv * sqrt_years)
        )
        gex = torch.where(is_call, 1.0, -1.0) * gamma * open_interest * CONTRACT_MULTIPLIER
        valid = valid & torch.isfinite(iv) & torch.isfinite(gamma) & torch.isfinite(gex)
        nan = torch.full_like(iv, float("nan"))
        return {
            "valid_mask": valid,
            "iv": torch.where(valid, iv, nan),
            "gamma": torch.where(valid, gamma, nan),
            "gex": torch.where(valid, gex, nan),
        }

    prepared = to_device()
    for _ in range(warmup):
        calculate(prepared)
    torch.cuda.synchronize(device)

    device_timings_ms = []
    device_result = None
    for _ in range(repeats):
        torch.cuda.synchronize(device)
        started = time.perf_counter_ns()
        device_result = calculate(prepared)
        torch.cuda.synchronize(device)
        device_timings_ms.append((time.perf_counter_ns() - started) / 1_000_000.0)

    end_to_end_timings_ms = []
    host_result = None
    for _ in range(repeats):
        torch.cuda.synchronize(device)
        started = time.perf_counter_ns()
        one_prepared = to_device()
        one_result = calculate(one_prepared)
        host_result = {
            "valid_mask": one_result["valid_mask"].cpu().numpy(),
            "iv": one_result["iv"].cpu().numpy(),
            "gamma": one_result["gamma"].cpu().numpy(),
            "gex": one_result["gex"].cpu().numpy(),
        }
        torch.cuda.synchronize(device)
        end_to_end_timings_ms.append((time.perf_counter_ns() - started) / 1_000_000.0)

    assert device_result is not None and host_result is not None

    def timing_summary(values: list[float]) -> dict[str, float]:
        ordered = sorted(values)
        p95_index = min(len(ordered) - 1, math.ceil(len(ordered) * 0.95) - 1)
        return {
            "median_ms": statistics.median(values),
            "min_ms": min(values),
            "p95_ms": ordered[p95_index],
        }

    return {
        "available": True,
        "device": torch.cuda.get_device_name(device),
        "torch_version": torch.__version__,
        "cuda_version": torch.version.cuda,
        "dtype": "float64",
        "device_only": timing_summary(device_timings_ms),
        "end_to_end": timing_summary(end_to_end_timings_ms),
        "equivalence_vs_numpy": _error_summary(numpy_reference, host_result),
    }


def _parse_rows(value: str) -> list[int]:
    rows = [int(item.strip()) for item in value.split(",") if item.strip()]
    if not rows or any(row < 1 for row in rows):
        raise argparse.ArgumentTypeError("rows must be a comma-separated list of positive integers")
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=_parse_rows, default=[540, 2_793, 8_192])
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--seed", type=int, default=20260825)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.repeats < 1 or args.warmup < 0:
        parser.error("repeats must be positive and warmup cannot be negative")

    report: dict[str, object] = {
        "benchmark": "databento-iv-gamma-gex-cpu-vs-cuda-v1",
        "live_path_changed_by_benchmark": False,
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "cpu": platform.processor(),
        "numpy": np.__version__,
        "repeats": args.repeats,
        "warmup": args.warmup,
        "rows": [],
    }
    profile_inputs = _synthetic_inputs(args.rows[0], args.seed)
    report["cpu_profile"] = [
        _profile_cpu("scalar", _scalar_batch, profile_inputs),
        _profile_cpu("numpy", _numpy_batch, profile_inputs),
    ]

    for index, row_count in enumerate(args.rows):
        inputs = _synthetic_inputs(row_count, args.seed + index)
        scalar_timing, scalar_result = _time_cpu(
            _scalar_batch, inputs, warmup=args.warmup, repeats=args.repeats
        )
        numpy_timing, numpy_result = _time_cpu(
            _numpy_batch, inputs, warmup=args.warmup, repeats=args.repeats
        )
        cuda = _torch_cuda_benchmark(
            inputs, numpy_result, warmup=args.warmup, repeats=args.repeats
        )
        row_report = {
            "row_count": row_count,
            "scalar_cpu": scalar_timing,
            "numpy_cpu": numpy_timing,
            "numpy_speedup_vs_scalar": scalar_timing["median_ms"] / numpy_timing["median_ms"],
            "numpy_equivalence_vs_scalar": _error_summary(scalar_result, numpy_result),
            "cuda": cuda,
        }
        if cuda.get("available"):
            row_report["cuda_device_speedup_vs_numpy"] = (
                numpy_timing["median_ms"] / cuda["device_only"]["median_ms"]
            )
            row_report["cuda_end_to_end_speedup_vs_numpy"] = (
                numpy_timing["median_ms"] / cuda["end_to_end"]["median_ms"]
            )
        report["rows"].append(row_report)

    rendered = json.dumps(report, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
