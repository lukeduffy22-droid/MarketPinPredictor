from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Mapping


CANARY_FAMILY = "RUT"
CORE_FAMILIES = ("SPX", "NDX")
CORE_FRESHNESS_BUDGET_SECONDS = 15.0
CORE_LAG_BUDGET_SECONDS = 2.0
CANARY_FRESHNESS_BUDGET_SECONDS = 30.0
CANARY_LAG_BUDGET_SECONDS = 3.0
CORE_PRIMARY_PAIR_COVERAGE_RATIO = 0.10
CORE_FRESH_QUOTE_COVERAGE_RATIO = 0.50


@dataclass(frozen=True)
class CanaryDecision:
    state: str
    family: str
    rollback_required: bool
    reasons: tuple[str, ...]
    budgets: dict[str, float]
    allowed_families: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class RUTOptionalFamilyCanary:
    """Fail-closed RUT canary state without implicitly adding subscriptions."""

    def __init__(self, *, enabled: bool, requested_families: tuple[str, ...]) -> None:
        self.enabled = bool(enabled)
        self.requested_families = tuple(
            sorted({str(value).strip().upper() for value in requested_families})
        )
        self._rolled_back_reasons: tuple[str, ...] = ()

    @staticmethod
    def _budgets() -> dict[str, float]:
        return {
            "core_freshness_seconds": CORE_FRESHNESS_BUDGET_SECONDS,
            "core_lag_seconds": CORE_LAG_BUDGET_SECONDS,
            "canary_freshness_seconds": CANARY_FRESHNESS_BUDGET_SECONDS,
            "canary_lag_seconds": CANARY_LAG_BUDGET_SECONDS,
            "core_primary_pair_coverage_ratio": CORE_PRIMARY_PAIR_COVERAGE_RATIO,
            "core_fresh_quote_coverage_ratio": CORE_FRESH_QUOTE_COVERAGE_RATIO,
        }

    def status(self) -> CanaryDecision:
        """Report configuration/current terminal state without consuming observations."""
        if not self.enabled:
            state, reasons = "disabled", ()
        elif CANARY_FAMILY not in self.requested_families:
            state, reasons = "blocked", ("RUT_NOT_REQUESTED_NO_IMPLICIT_SUBSCRIPTION",)
        elif self._rolled_back_reasons:
            state, reasons = "rolled_back", self._rolled_back_reasons
        else:
            state, reasons = "armed", ()
        rollback_required = state == "rolled_back"
        return CanaryDecision(
            state,
            CANARY_FAMILY,
            rollback_required,
            reasons,
            self._budgets(),
            tuple(family for family in self.requested_families if not rollback_required or family != CANARY_FAMILY),
        )

    def preview(self, observations: Mapping[str, Mapping[str, object]]) -> CanaryDecision:
        """Evaluate observations without latching a rollback decision."""
        return self._evaluate(observations, latch_rollback=False)

    def evaluate(self, observations: Mapping[str, Mapping[str, object]]) -> CanaryDecision:
        return self._evaluate(observations, latch_rollback=True)

    def _evaluate(
        self,
        observations: Mapping[str, Mapping[str, object]],
        *,
        latch_rollback: bool,
    ) -> CanaryDecision:
        if not self.enabled:
            return CanaryDecision(
                "disabled",
                CANARY_FAMILY,
                False,
                (),
                self._budgets(),
                self.requested_families,
            )
        if CANARY_FAMILY not in self.requested_families:
            return CanaryDecision(
                "blocked",
                CANARY_FAMILY,
                False,
                ("RUT_NOT_REQUESTED_NO_IMPLICIT_SUBSCRIPTION",),
                self._budgets(),
                self.requested_families,
            )
        if self._rolled_back_reasons:
            return CanaryDecision(
                "rolled_back",
                CANARY_FAMILY,
                True,
                self._rolled_back_reasons,
                self._budgets(),
                tuple(
                    family
                    for family in self.requested_families
                    if family != CANARY_FAMILY
                ),
            )

        reasons: list[str] = []
        for family in CORE_FAMILIES:
            status = observations.get(family)
            if not status:
                reasons.append(f"CORE_MISSING:{family}")
                continue
            if not bool(status.get("validation_is_valid")):
                reasons.append(f"CORE_INVALID:{family}")
            age = status.get("data_age_seconds")
            lag = status.get("receive_to_process_lag_p95_seconds")
            if age is None or float(age) > CORE_FRESHNESS_BUDGET_SECONDS:
                reasons.append(f"CORE_FRESHNESS_BUDGET:{family}")
            if lag is None or float(lag) < 0 or float(lag) > CORE_LAG_BUDGET_SECONDS:
                reasons.append(f"CORE_LAG_BUDGET:{family}")
            pair_coverage = status.get("primary_pair_coverage_ratio")
            if pair_coverage is None or float(pair_coverage) < CORE_PRIMARY_PAIR_COVERAGE_RATIO:
                reasons.append(f"CORE_PRIMARY_PAIR_COVERAGE:{family}")
            fresh_count = status.get("fresh_quote_count")
            expected_count = status.get("expected_quote_count")
            if expected_count is None or int(expected_count) <= 0 or fresh_count is None:
                reasons.append(f"CORE_FRESH_QUOTE_COVERAGE:{family}")
            elif float(fresh_count) / float(expected_count) < CORE_FRESH_QUOTE_COVERAGE_RATIO:
                reasons.append(f"CORE_FRESH_QUOTE_COVERAGE:{family}")

        canary = observations.get(CANARY_FAMILY)
        if not canary:
            reasons.append("CANARY_MISSING:RUT")
        else:
            if not bool(canary.get("validation_is_valid")):
                reasons.append("CANARY_INVALID:RUT")
            age = canary.get("data_age_seconds")
            lag = canary.get("receive_to_process_lag_p95_seconds")
            if age is None or float(age) > CANARY_FRESHNESS_BUDGET_SECONDS:
                reasons.append("CANARY_FRESHNESS_BUDGET:RUT")
            if lag is None:
                reasons.append("CANARY_LAG_UNAVAILABLE:RUT")
            elif float(lag) < 0 or float(lag) > CANARY_LAG_BUDGET_SECONDS:
                reasons.append("CANARY_LAG_BUDGET:RUT")

        if reasons:
            evaluated_reasons = tuple(reasons)
            if latch_rollback:
                self._rolled_back_reasons = evaluated_reasons
            return CanaryDecision(
                "rolled_back",
                CANARY_FAMILY,
                True,
                evaluated_reasons,
                self._budgets(),
                tuple(
                    family
                    for family in self.requested_families
                    if family != CANARY_FAMILY
                ),
            )
        return CanaryDecision(
            "observing",
            CANARY_FAMILY,
            False,
            (),
            self._budgets(),
            self.requested_families,
        )
