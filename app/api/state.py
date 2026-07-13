"""Shared mutable API runtime state."""

last_prediction_ts = {s: 0.0 for s in ("SPX", "NDX", "DJI", "RUT")}
coefficients_cache = {}
