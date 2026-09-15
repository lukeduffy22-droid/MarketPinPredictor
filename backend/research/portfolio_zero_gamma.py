"""Research-only portfolio gamma spot sweep.

This module is intentionally isolated from the production streamer and target
selection. It answers a different question from the legacy strike-bucket
crossing: holding observed IV, open interest, and time-to-expiry assumptions
fixed, where would signed portfolio gamma cross zero as hypothetical spot moves?

The calculation is not a promoted forecast or trading signal. Open interest does
not reveal dealer inventory, and the sticky-strike IV assumption will diverge
from a live volatility surface as spot moves.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
import math
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

import numpy as np


PORTFOLIO_ZERO_GAMMA_SHADOW_VERSION = "portfolio-zero-gamma-spot-sweep-shadow-v1"
_SECONDS_PER_YEAR = 365.0 * 24.0 * 60.0 * 60.0
_EASTERN = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class PortfolioZeroGammaSweep:
    formula_version: str
    mode: str
    promoted: bool
    calculation_status: str
    reference_spot: float
    lower_spot: float
    upper_spot: float
    grid_points: int
    contracts_received: int
    contracts_used: int
    selected_crossing: float | None
    crossing_levels: tuple[float, ...]
    selection_rule: str
    spots: tuple[float, ...]
    signed_portfolio_gamma_1pct: tuple[float, ...]
    assumptions: tuple[str, ...]
    rejection_reasons: tuple[str, ...]

    def to_dict(self, *, include_curve: bool = True) -> dict[str, Any]:
        result = {
            "formula_version": self.formula_version,
            "mode": self.mode,
            "promoted": self.promoted,
            "calculation_status": self.calculation_status,
            "reference_spot": self.reference_spot,
            "lower_spot": self.lower_spot,
            "upper_spot": self.upper_spot,
            "grid_points": self.grid_points,
            "contracts_received": self.contracts_received,
            "contracts_used": self.contracts_used,
            "selected_crossing": self.selected_crossing,
            "crossing_levels": list(self.crossing_levels),
            "selection_rule": self.selection_rule,
            "assumptions": list(self.assumptions),
            "rejection_reasons": list(self.rejection_reasons),
        }
        if include_curve:
            result["spots"] = list(self.spots)
            result["signed_portfolio_gamma_1pct"] = list(
                self.signed_portfolio_gamma_1pct
            )
        return result


def _finite(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _aware_utc(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    parsed = value
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(parsed, datetime):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _expiration_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _years_for_contract(
    contract: Mapping[str, Any],
    *,
    as_of_utc: datetime | None,
) -> float | None:
    for field in ("years_to_expiration", "time_to_expiry_years"):
        years = _finite(contract.get(field))
        if years is not None and years > 0:
            return years

    expiration = _expiration_date(
        contract.get("expiration_date") or contract.get("expiration")
    )
    if expiration is not None and as_of_utc is not None:
        expiration_et = datetime.combine(expiration, time(16, 0), tzinfo=_EASTERN)
        years = (
            expiration_et.astimezone(timezone.utc) - as_of_utc
        ).total_seconds() / _SECONDS_PER_YEAR
        return years if years > 0 else None

    days = _finite(contract.get("days_to_expiry"))
    if days is not None and days > 0:
        return days / 365.0
    return None


def sign_crossings(
    spots: np.ndarray,
    values: np.ndarray,
) -> tuple[float, ...]:
    """Return every finite adjacent sign crossing using linear interpolation."""
    spot_values = np.asarray(spots, dtype=np.float64)
    gamma_values = np.asarray(values, dtype=np.float64)
    if spot_values.ndim != 1 or gamma_values.ndim != 1:
        raise ValueError("crossing inputs must be one-dimensional")
    if len(spot_values) != len(gamma_values):
        raise ValueError("crossing inputs must have equal lengths")
    if len(spot_values) < 2 or not np.all(np.diff(spot_values) > 0):
        raise ValueError("spots must contain at least two strictly increasing values")

    crossings: list[float] = []
    for index in range(len(spot_values) - 1):
        left_spot = float(spot_values[index])
        right_spot = float(spot_values[index + 1])
        left = float(gamma_values[index])
        right = float(gamma_values[index + 1])
        if not (math.isfinite(left) and math.isfinite(right)):
            continue
        crossing = None
        if left == 0.0:
            crossing = left_spot
        elif right == 0.0:
            crossing = right_spot
        elif left * right < 0.0:
            crossing = left_spot + (0.0 - left) * (right_spot - left_spot) / (right - left)
        if crossing is not None and (
            not crossings or not math.isclose(crossing, crossings[-1], abs_tol=1e-12)
        ):
            crossings.append(float(crossing))
    return tuple(crossings)


def portfolio_zero_gamma_spot_sweep(
    contracts: Iterable[Mapping[str, Any]],
    *,
    reference_spot: float,
    as_of_utc: datetime | str | None = None,
    span_pct: float = 0.08,
    grid_points: int = 401,
    risk_free_rate: float = 0.045,
    contract_multiplier: float = 100.0,
) -> PortfolioZeroGammaSweep:
    """Calculate a shadow-only portfolio gamma crossing across hypothetical spot.

    Expected persisted row fields are ``strike``, ``option_type``, ``iv``,
    ``open_interest``, and either a positive year fraction or an expiration date
    paired with ``as_of_utc``. Invalid rows are rejected rather than imputed.
    """
    rows = list(contracts)
    spot = _finite(reference_spot)
    span = _finite(span_pct)
    rate = _finite(risk_free_rate)
    multiplier = _finite(contract_multiplier)
    if spot is None or spot <= 0:
        raise ValueError("reference_spot must be finite and positive")
    if span is None or not 0 < span < 1:
        raise ValueError("span_pct must be between zero and one")
    if grid_points < 3:
        raise ValueError("grid_points must be at least three")
    if rate is None:
        raise ValueError("risk_free_rate must be finite")
    if multiplier is None or multiplier <= 0:
        raise ValueError("contract_multiplier must be finite and positive")

    calculation_time = _aware_utc(as_of_utc)
    strikes: list[float] = []
    years: list[float] = []
    ivs: list[float] = []
    open_interest: list[float] = []
    signs: list[float] = []
    rejected: list[str] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            rejected.append(f"row {index}: not a mapping")
            continue
        strike = _finite(row.get("strike"))
        iv = _finite(row.get("iv"))
        oi = _finite(row.get("open_interest"))
        option_type = str(row.get("option_type") or "").upper()[:1]
        year_fraction = _years_for_contract(row, as_of_utc=calculation_time)
        invalid_fields = []
        if strike is None or strike <= 0:
            invalid_fields.append("strike")
        if iv is None or iv <= 0:
            invalid_fields.append("iv")
        if oi is None or oi <= 0:
            invalid_fields.append("open_interest")
        if option_type not in {"C", "P"}:
            invalid_fields.append("option_type")
        if year_fraction is None or year_fraction <= 0:
            invalid_fields.append("time_to_expiration")
        if invalid_fields:
            rejected.append(f"row {index}: invalid " + ", ".join(invalid_fields))
            continue
        strikes.append(float(strike))
        years.append(float(year_fraction))
        ivs.append(float(iv))
        open_interest.append(float(oi))
        signs.append(1.0 if option_type == "C" else -1.0)

    lower_spot = spot * (1.0 - span)
    upper_spot = spot * (1.0 + span)
    spot_grid = np.linspace(lower_spot, upper_spot, grid_points, dtype=np.float64)
    assumptions = (
        "research/shadow only; never used as the production target",
        "observed implied volatility is held fixed by contract (sticky strike)",
        "open interest and call-positive/put-negative sign convention are held fixed",
        "open interest is not direct evidence of dealer inventory",
        "time to expiration is frozen at the calculation as-of time",
        "signed gamma is scaled to a 1% underlying move using spot^2 * 0.01",
    )

    if not strikes:
        return PortfolioZeroGammaSweep(
            formula_version=PORTFOLIO_ZERO_GAMMA_SHADOW_VERSION,
            mode="shadow_research",
            promoted=False,
            calculation_status="no_usable_contracts",
            reference_spot=spot,
            lower_spot=lower_spot,
            upper_spot=upper_spot,
            grid_points=grid_points,
            contracts_received=len(rows),
            contracts_used=0,
            selected_crossing=None,
            crossing_levels=(),
            selection_rule="nearest crossing to reference spot",
            spots=(),
            signed_portfolio_gamma_1pct=(),
            assumptions=assumptions,
            rejection_reasons=tuple(rejected),
        )

    strike_array = np.asarray(strikes, dtype=np.float64)[None, :]
    year_array = np.asarray(years, dtype=np.float64)[None, :]
    iv_array = np.asarray(ivs, dtype=np.float64)[None, :]
    oi_array = np.asarray(open_interest, dtype=np.float64)[None, :]
    sign_array = np.asarray(signs, dtype=np.float64)[None, :]
    hypothetical_spot = spot_grid[:, None]
    sqrt_years = np.sqrt(year_array)
    d1 = (
        np.log(hypothetical_spot / strike_array)
        + (rate + 0.5 * iv_array * iv_array) * year_array
    ) / (iv_array * sqrt_years)
    gamma = np.exp(-0.5 * d1 * d1) / (
        math.sqrt(2.0 * math.pi) * hypothetical_spot * iv_array * sqrt_years
    )
    signed_gamma = (
        gamma
        * oi_array
        * multiplier
        * sign_array
        * hypothetical_spot
        * hypothetical_spot
        * 0.01
    ).sum(axis=1)
    crossings = sign_crossings(spot_grid, signed_gamma)
    selected = min(crossings, key=lambda value: abs(value - spot)) if crossings else None
    status = "crossing_found" if selected is not None else "no_crossing_in_grid"
    return PortfolioZeroGammaSweep(
        formula_version=PORTFOLIO_ZERO_GAMMA_SHADOW_VERSION,
        mode="shadow_research",
        promoted=False,
        calculation_status=status,
        reference_spot=spot,
        lower_spot=lower_spot,
        upper_spot=upper_spot,
        grid_points=grid_points,
        contracts_received=len(rows),
        contracts_used=len(strikes),
        selected_crossing=selected,
        crossing_levels=crossings,
        selection_rule="nearest crossing to reference spot",
        spots=tuple(float(value) for value in spot_grid),
        signed_portfolio_gamma_1pct=tuple(float(value) for value in signed_gamma),
        assumptions=assumptions,
        rejection_reasons=tuple(rejected),
    )


def portfolio_zero_gamma_from_calculation_inputs(
    calculation_inputs: Mapping[str, Any],
    *,
    reference_spot: float | None = None,
    span_pct: float = 0.08,
    grid_points: int = 401,
) -> PortfolioZeroGammaSweep:
    """Run the shadow sweep from a persisted ``gamma-inputs-v2`` document."""
    if not isinstance(calculation_inputs, Mapping):
        raise ValueError("calculation_inputs must be a mapping")
    rows = calculation_inputs.get("calculated_gex_rows")
    if not isinstance(rows, list):
        raise ValueError("calculated_gex_rows must be a list")
    output = calculation_inputs.get("output_summary")
    output = output if isinstance(output, Mapping) else {}
    resolved_spot = reference_spot
    if resolved_spot is None:
        resolved_spot = output.get("price") or output.get("spot_last")
    parameters = calculation_inputs.get("parameters")
    parameters = parameters if isinstance(parameters, Mapping) else {}
    return portfolio_zero_gamma_spot_sweep(
        rows,
        reference_spot=resolved_spot,
        as_of_utc=calculation_inputs.get("calculated_at_utc"),
        span_pct=span_pct,
        grid_points=grid_points,
        risk_free_rate=parameters.get("risk_free_rate", 0.045),
        contract_multiplier=parameters.get("contract_multiplier", 100.0),
    )
