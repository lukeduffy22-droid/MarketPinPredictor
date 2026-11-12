"""Descriptive paired evaluation with sessions as the resampling unit."""
from collections import defaultdict
from statistics import fmean
import random


def paired_session_summary(rows, *, bootstrap_samples=1000):
    """Rows contain session, prediction, baseline and outcome; no fitted choices.

    Equal session weighting prevents a busy intraday session dominating evidence.
    The deterministic block bootstrap describes sampled-session uncertainty; it
    does not establish independence, probability calibration or promotion.
    """
    groups = defaultdict(list)
    for row in rows:
        error = row["prediction"] - row["outcome"]
        baseline = row["baseline"] - row["outcome"]
        groups[row["session"]].append((error, baseline))
    sessions = [{"session": session, "n": len(values),
        "mae_points": fmean(abs(v[0]) for v in values),
        "baseline_mae_points": fmean(abs(v[1]) for v in values),
        "bias_points": fmean(v[0] for v in values)} for session, values in sorted(groups.items())]
    result = {"independent_session_count": len(sessions), "paired_outcomes": sum(r["n"] for r in sessions),
              "weighting": "equal_session", "sessions": sessions, "uncertainty": None,
              "performance_claim_eligible": False, "calibration_verified": False}
    if not sessions:
        return result
    mae, baseline = fmean(r["mae_points"] for r in sessions), fmean(r["baseline_mae_points"] for r in sessions)
    result.update(mae_points=mae, baseline_mae_points=baseline,
                  bias_points=fmean(r["bias_points"] for r in sessions),
                  skill_vs_baseline=1-mae/baseline if baseline else None)
    if len(sessions) >= 2:
        randomizer = random.Random(271026)
        gains = [r["baseline_mae_points"]-r["mae_points"] for r in sessions]
        draws = sorted(fmean(randomizer.choices(gains, k=len(gains))) for _ in range(bootstrap_samples))
        result["uncertainty"] = {"kind": "descriptive_session_block_bootstrap", "nominal_level": .95,
            "paired_mae_advantage_points": [draws[int(.025*(len(draws)-1))], draws[int(.975*(len(draws)-1))]],
            "resamples": bootstrap_samples, "coverage_verified": False,
            "limitation": "Small or dependent session samples may not represent future regimes."}
    return result
