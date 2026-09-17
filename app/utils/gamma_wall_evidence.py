"""Normalize derived wall displays without changing retained producer records."""

import math

import pandas as pd


def normalize_gamma_walls(frame: pd.DataFrame) -> pd.DataFrame:
    """Keep missing sides unknown; calculated sums do not prove chain coverage."""
    rows = []
    for original in frame.to_dict("records"):
        row = dict(original)
        for field in ("days_to_expiry", "expiration_count"):
            row.setdefault(field, None)
        if "net_gex" not in row:
            for alias in ("gex", "net_gamma", "Net GEX", "net_gex_b"):
                if alias in row:
                    row["net_gex"] = row[alias]
                    break
        states = []
        for side in ("call", "put"):
            value = row.get(f"{side}_gex")
            count = row.get(f"{side}_calculated_contracts")
            try:
                value = float(value)
                value = value if math.isfinite(value) else None
            except (TypeError, ValueError):
                value = None
            try:
                count = float(count)
                count = count if math.isfinite(count) and count >= 0 and count.is_integer() else None
            except (TypeError, ValueError):
                count = None
            if count == 0 or value is None:
                state = "zero_unverified" if row.get(f"{side}_evidence") == "zero_unverified" and count is None else "no_calculated_evidence"
                value = None
            elif value == 0 and count is None:
                value, state = None, "zero_unverified"
            else:
                state = "calculated" if count is not None else "coverage_unreported"
            row[f"{side}_gex"] = value
            row[f"{side}_evidence"] = state
            states.append(state)
        if any(row[f"{side}_gex"] is None for side in ("call", "put")):
            row["total_gex"] = None
            row["coverage"] = "partial_or_unknown"
        else:
            if pd.isna(row.get("total_gex")):
                row["total_gex"] = abs(row["call_gex"]) + abs(row["put_gex"])
            row["coverage"] = (
                "calculated_sides_only" if states == ["calculated", "calculated"]
                else "coverage_unreported"
            )
        rows.append(row)
    return pd.DataFrame(rows)
