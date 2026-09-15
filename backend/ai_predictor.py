"""Explainable Databento EOD prediction layer.

This module intentionally avoids LLM-based price guessing. It creates a
backtest-friendly feature snapshot from live Databento OPRA pin/GEX payloads and
returns a deterministic ensemble target with confidence and explanatory signals.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

import torch

from backend.workstation import payload_has_fallback_provenance

MODEL_VERSION = "databento_quant_ensemble_v1"
FEATURE_SCHEMA_VERSION = "databento-close-features-2.0"
REPLAY_CONTRACT_VERSION = "databento-quant-ensemble-replay-v1"
CONFIDENCE_KIND = "data_quality_heuristic"
CONFIDENCE_SCALE = "percent_0_100"
EQUITY_INDEX_SYMBOLS = {"SPX", "NDX", "SPY", "QQQ", "RUT", "OEX", "DJX", "RUI", "XAU", "HGX", "OSX", "UTY", "XSP", "XND", "MRUT"}


@lru_cache(maxsize=1)
def model_artifact_sha256() -> str:
    """Fingerprint the deployed deterministic model implementation.

    This ensemble is code-defined rather than loaded from a serialized weight
    artifact.  Hashing the implementation file gives each passport an exact
    deployed artifact identity without pretending that an absent ``.pt`` file
    exists.
    """
    with Path(__file__).open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def feature_snapshot_sha256(snapshot: dict[str, Any]) -> str:
    canonical = json.dumps(
        snapshot,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def replay_ai_prediction(feature_snapshot: dict[str, Any]) -> float:
    """Reproduce a stored ensemble point estimate from point-in-time features."""
    if feature_snapshot.get("replay_contract_version") != REPLAY_CONTRACT_VERSION:
        raise ValueError("unsupported or missing prediction replay contract")
    anchors = feature_snapshot.get("anchor_inputs")
    if not isinstance(anchors, list):
        raise ValueError("prediction replay anchors are missing")
    weighted_sum = 0.0
    total_weight = 0.0
    for item in anchors:
        if not isinstance(item, dict):
            raise ValueError("prediction replay anchor is malformed")
        value = _num(item.get("value"))
        weight = _num(item.get("weight"))
        if value is None or weight is None or weight < 0:
            raise ValueError("prediction replay anchor is non-finite")
        if weight > 0:
            weighted_sum += value * weight
            total_weight += weight
    if total_weight <= 0:
        spot = _num(feature_snapshot.get("spot"))
        if spot is None or spot <= 0:
            raise ValueError("prediction replay has no weighted anchors or valid spot")
        target = spot
    else:
        target = weighted_sum / total_weight
    vix_adjustment = _num(feature_snapshot.get("vix_adjustment"))
    historical_adjustment = _num(feature_snapshot.get("historical_adjustment"))
    if vix_adjustment is None or historical_adjustment is None:
        raise ValueError("prediction replay adjustments are missing")
    replayed = target + vix_adjustment + historical_adjustment
    if not math.isfinite(replayed) or replayed <= 0:
        raise ValueError("prediction replay produced an invalid close estimate")
    return replayed


def inference_device() -> torch.device:
    """Return the available inference device without claiming CUDA when unavailable."""
    return torch.device("cuda:0" if torch.cuda.is_available() else "cpu")


def _num(value: Any) -> float | None:
    try:
        if value is None:
            return None
        number = float(value)
        if not math.isfinite(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


def _usable(payload: dict | None) -> bool:
    if not payload:
        return False
    if payload_has_fallback_provenance(payload):
        return False
    if payload.get("validation_is_valid") is not True:
        return False
    if payload.get("gamma_excluded_from_model") is not False:
        return False
    if payload.get("usable_for_prediction") is False:
        return False
    spot = _num(payload.get("price"))
    if spot is None or spot <= 0:
        return False
    return any(_num(payload.get(key)) is not None for key in ("likely_close", "predicted_close", "gamma_pin", "max_pain"))


def _anchor_weight(spot: float, anchor: float | None, base_weight: float) -> float:
    if anchor is None or spot <= 0:
        return 0.0
    distance_pct = abs(anchor - spot) / spot
    if distance_pct > 0.03:
        return base_weight * 0.20
    if distance_pct > 0.015:
        return base_weight * 0.50
    return base_weight


def _weighted_average(anchors: list[tuple[str, float, float]], device: torch.device) -> float | None:
    total_weight = sum(weight for _, _, weight in anchors if weight > 0)
    if total_weight <= 0:
        return None
    values = torch.tensor(
        [value for _, value, weight in anchors if weight > 0],
        dtype=torch.float64,
        device=device,
    )
    weights = torch.tensor(
        [weight for _, _, weight in anchors if weight > 0],
        dtype=torch.float64,
        device=device,
    )
    return float(torch.sum(values * weights).div(torch.sum(weights)).item())


def _confidence(payload: dict, target: float, spot: float, vix_adjustment: float, historical_context: dict | None = None) -> float:
    contracts = _num(payload.get("contracts")) or 0.0
    quotes_cached = _num(payload.get("quotes_cached")) or 0.0
    net_gex = abs(_num(payload.get("net_gex")) or 0.0)
    paired_quotes = _num(payload.get("paired_quote_count")) or 0.0
    call_quotes = _num(payload.get("call_quote_count")) or 0.0
    put_quotes = _num(payload.get("put_quote_count")) or 0.0
    distance_pct = abs(target - spot) / spot if spot else 1.0

    score = 48.0
    score += min(18.0, contracts * 0.35)
    score += min(10.0, quotes_cached * 0.04)
    score += min(8.0, math.log10(net_gex + 1.0) * 1.5)
    if paired_quotes < 10:
        score -= 8.0
    if call_quotes and put_quotes:
        side_balance = min(call_quotes, put_quotes) / max(call_quotes, put_quotes)
        if side_balance < 0.5:
            score -= 6.0

    if distance_pct <= 0.0015:
        score += 7.0
    elif distance_pct <= 0.004:
        score += 4.0
    elif distance_pct > 0.015:
        score -= 8.0

    if abs(vix_adjustment) > spot * 0.0005:
        score -= 3.0

    if historical_context:
        if historical_context.get("historical_adjustment_eligible") is False:
            score -= 2.0
        else:
            history_rows = historical_context.get("history_rows") or 0
            realized_vol = historical_context.get("realized_vol_20d")
            if history_rows >= 500:
                score += 4.0
            if isinstance(realized_vol, (int, float)) and math.isfinite(realized_vol):
                if realized_vol > 0.35:
                    score -= 6.0
                elif realized_vol < 0.18:
                    score += 2.0

    return max(35.0, min(95.0, score))


def build_ai_prediction(symbol: str, payload: dict, vix_payload: dict | None = None, historical_context: dict | None = None, historical_adjustment: tuple[float, list[dict]] | None = None) -> dict:
    """Build an explainable EOD prediction from Databento live GEX payloads."""
    symbol = symbol.upper()
    if not _usable(payload):
        reasons = payload.get("validation_failure_reasons") if payload else None
        return {
            "symbol": symbol,
            "usable": False,
            "reason": (reasons[0] if reasons else "No usable Databento payload"),
            "model_version": MODEL_VERSION,
            "model_artifact_sha256": model_artifact_sha256(),
            "inference_device": str(inference_device()),
            "confidence": None,
            "confidence_kind": "unavailable",
            "confidence_scale": "not_applicable",
            "confidence_calibrated": False,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    device = inference_device()
    spot = _num(payload.get("price")) or 0.0
    likely = _num(payload.get("likely_close")) or _num(payload.get("predicted_close"))
    gamma_pin = _num(payload.get("gamma_pin"))
    zero_gamma = _num(payload.get("zero_gamma"))
    max_pain = _num(payload.get("max_pain"))

    anchors: list[tuple[str, float, float]] = []
    if likely is not None:
        anchors.append(("live_gex_target", likely, _anchor_weight(spot, likely, 0.42)))
    if gamma_pin is not None:
        anchors.append(("gamma_pin", gamma_pin, _anchor_weight(spot, gamma_pin, 0.28)))
    if zero_gamma is not None:
        anchors.append(("zero_gamma", zero_gamma, _anchor_weight(spot, zero_gamma, 0.18)))
    if max_pain is not None:
        anchors.append(("max_pain", max_pain, _anchor_weight(spot, max_pain, 0.12)))

    target = _weighted_average(anchors, device)
    if target is None:
        target = spot

    signals = []
    for name, value, weight in anchors:
        signals.append({
            "name": name,
            "value": value,
            "weight": weight,
            "distance_points": value - spot,
        })

    vix_adjustment = 0.0
    vix_pressure = None
    if symbol in EQUITY_INDEX_SYMBOLS and vix_payload is not None and _usable(vix_payload):
        vix_spot = _num(vix_payload.get("price"))
        vix_target = _num(vix_payload.get("likely_close")) or _num(vix_payload.get("predicted_close"))
        if vix_spot and vix_target:
            vix_pressure = (vix_target - vix_spot) / vix_spot
            # Rising VIX is bearish for equity indexes. Keep this a small, capped context adjustment.
            vix_adjustment = float(
                torch.tensor(
                    -spot * max(min(vix_pressure * 0.05, 0.0008), -0.0008),
                    dtype=torch.float64,
                    device=device,
                ).item()
            )
            target += vix_adjustment
            signals.append({
                "name": "vix_vol_pressure",
                "value": vix_target,
                "weight": 0.05,
                "distance_points": vix_adjustment,
            })

    historical_points = 0.0
    if historical_adjustment:
        historical_points, historical_signals = historical_adjustment
        target += historical_points
        signals.extend(historical_signals)

    confidence = _confidence(payload, target, spot, vix_adjustment, historical_context)
    expected_move_points = target - spot
    expected_move_pct = expected_move_points / spot * 100 if spot else 0.0
    net_bias = "bullish" if expected_move_points > 0 else "bearish" if expected_move_points < 0 else "neutral"

    feature_snapshot = {
        "schema_version": FEATURE_SCHEMA_VERSION,
        "replay_contract_version": REPLAY_CONTRACT_VERSION,
        "spot": spot,
        "likely_close": likely,
        "gamma_pin": gamma_pin,
        "zero_gamma": zero_gamma,
        "max_pain": max_pain,
        "net_gex": _num(payload.get("net_gex")),
        "gross_gex": _num(payload.get("gross_gex")),
        "contracts": _num(payload.get("contracts")),
        "quotes_cached": _num(payload.get("quotes_cached")),
        "anchor_inputs": [
            {"name": name, "value": value, "weight": weight}
            for name, value, weight in anchors
        ],
        "vix_adjustment": vix_adjustment,
        "historical_adjustment": historical_points,
        "historical_context": historical_context or {},
    }
    return {
        "symbol": symbol,
        "usable": True,
        "provider": payload.get("provider", "databento"),
        "model_version": MODEL_VERSION,
        "model_artifact_sha256": model_artifact_sha256(),
        "model_type": "Databento Quant Ensemble",
        "inference_device": str(device),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "current_price": spot,
        "predicted_close": target,
        "confidence": confidence,
        "confidence_kind": CONFIDENCE_KIND,
        "confidence_scale": CONFIDENCE_SCALE,
        "confidence_calibrated": False,
        "expected_move_points": expected_move_points,
        "expected_move_pct": expected_move_pct,
        "net_bias": net_bias,
        "vix_pressure": vix_pressure,
        "vix_adjustment": vix_adjustment,
        "historical_adjustment": historical_points,
        "signals": signals,
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "feature_hash": feature_snapshot_sha256(feature_snapshot),
        "feature_snapshot": feature_snapshot,
        "pin_payload": payload,
    }
