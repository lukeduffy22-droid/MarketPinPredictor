from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable

import numpy as np
import pandas as pd

from .dataset import PRODUCTION_FAMILIES
from .research import build_session_close_labels
from .surface import MODEL_FEATURE_COLUMNS, select_decision_horizon_features


@dataclass(frozen=True)
class FamilyTrainingCoverage:
    family_root: str
    horizon_rows: int
    labeled_rows: int
    labeled_sessions: int
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class FeatureTrainingCoverage:
    feature: str
    finite_rows: int
    missing_ratio: float


@dataclass(frozen=True)
class FamilyCloseLabelCoverage:
    family_root: str
    verified_sessions: int


@dataclass(frozen=True)
class CloseLabelPreflightReport:
    ready: bool
    verified_rows: int
    complete_bundle_sessions: int
    minimum_sessions: int
    expected_families: tuple[str, ...]
    reasons: tuple[str, ...]
    family_coverage: tuple[FamilyCloseLabelCoverage, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class CloseTrainingReadinessReport:
    ready: bool
    decision_horizon_minutes_before_close: int
    horizon_rows: int
    labeled_rows: int
    labeled_sessions: int
    source_hashes: int
    incumbent_rows: int
    incumbent_sessions: int
    minimum_sessions: int
    minimum_family_sessions: int
    reasons: tuple[str, ...]
    family_coverage: tuple[FamilyTrainingCoverage, ...]
    feature_coverage: tuple[FeatureTrainingCoverage, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def audit_close_label_preflight(
    closes: pd.DataFrame,
    *,
    minimum_sessions: int = 60,
    expected_families: Iterable[str] = PRODUCTION_FAMILIES,
) -> CloseLabelPreflightReport:
    """Cheaply reject label-insufficient runs before loading the TCBBO surface."""
    if minimum_sessions < 1:
        raise ValueError("minimum_sessions must be positive")
    families = tuple(dict.fromkeys(str(value).strip().upper() for value in expected_families))
    if not families or any(not family for family in families):
        raise ValueError("expected_families must contain non-empty symbols")
    required = {"family_root", "trading_date"}
    missing_columns = sorted(required - set(closes.columns))
    if missing_columns and not closes.empty:
        raise ValueError(
            "verified close preflight columns missing: " + ", ".join(missing_columns)
        )

    if closes.empty:
        normalized = pd.DataFrame(columns=["family_root", "trading_date"])
    else:
        normalized = closes[["family_root", "trading_date"]].copy()
        normalized["family_root"] = normalized["family_root"].astype(str).str.strip().str.upper()
        normalized["trading_date"] = normalized["trading_date"].astype(str).str.strip()
        normalized = normalized[
            normalized["family_root"].isin(families)
            & normalized["trading_date"].ne("")
        ].drop_duplicates(["family_root", "trading_date"])

    expected = set(families)
    sessions_by_family = {
        family: int(
            normalized.loc[normalized["family_root"] == family, "trading_date"].nunique()
        )
        for family in families
    }
    families_by_date = (
        normalized.groupby("trading_date")["family_root"].agg(lambda values: set(values))
        if not normalized.empty
        else pd.Series(dtype=object)
    )
    complete_sessions = int(sum(value == expected for value in families_by_date))
    reasons: list[str] = []
    if complete_sessions < minimum_sessions:
        reasons.append(
            f"complete verified close bundles {complete_sessions} < {minimum_sessions}"
        )
    for family in families:
        count = sessions_by_family[family]
        if count < minimum_sessions:
            reasons.append(f"{family} verified label sessions {count} < {minimum_sessions}")
    return CloseLabelPreflightReport(
        ready=not reasons,
        verified_rows=int(len(normalized)),
        complete_bundle_sessions=complete_sessions,
        minimum_sessions=minimum_sessions,
        expected_families=families,
        reasons=tuple(reasons),
        family_coverage=tuple(
            FamilyCloseLabelCoverage(family, sessions_by_family[family])
            for family in families
        ),
    )


def prepare_close_training_dataset(
    surface: pd.DataFrame,
    closes: pd.DataFrame,
    *,
    minutes_before_close: int = 15,
    minimum_sessions: int = 60,
    minimum_family_sessions: int = 10,
    expected_families: Iterable[str] = PRODUCTION_FAMILIES,
) -> tuple[pd.DataFrame, CloseTrainingReadinessReport]:
    """Select the exact decision horizon, attach known-later closes, and gate training."""
    if minimum_sessions < 1 or minimum_family_sessions < 1:
        raise ValueError("minimum session requirements must be positive")
    families = tuple(dict.fromkeys(str(value).upper() for value in expected_families))
    if surface.empty:
        family_coverage = tuple(
            FamilyTrainingCoverage(family, 0, 0, 0, ("no surface rows",))
            for family in families
        )
        report = CloseTrainingReadinessReport(
            ready=False, decision_horizon_minutes_before_close=minutes_before_close,
            horizon_rows=0, labeled_rows=0, labeled_sessions=0, source_hashes=0,
            incumbent_rows=0, incumbent_sessions=0,
            minimum_sessions=minimum_sessions,
            minimum_family_sessions=minimum_family_sessions,
            reasons=("no eligible research surface rows",),
            family_coverage=family_coverage, feature_coverage=(),
        )
        return pd.DataFrame(), report

    horizon = select_decision_horizon_features(
        surface, minutes_before_close=minutes_before_close
    )
    labeled = build_session_close_labels(horizon, closes) if not horizon.empty else pd.DataFrame()
    family_reports: list[FamilyTrainingCoverage] = []
    reasons: list[str] = []
    if not labeled.empty:
        if "close_source_artifact_sha256" not in labeled.columns:
            reasons.append("verified close artifact SHA-256 column is missing")
        else:
            close_hashes = labeled["close_source_artifact_sha256"].astype(str).str.lower()
            if not close_hashes.str.fullmatch(r"[0-9a-f]{64}").all():
                reasons.append("verified close artifact SHA-256 values are invalid")
    if not horizon.empty:
        captures_per_day = horizon.groupby("trading_date")["session_id"].nunique()
        sources_per_day = horizon.groupby("trading_date")["source_sha256"].nunique()
        ambiguous_days = sorted(
            set(captures_per_day[captures_per_day > 1].index.astype(str))
            | set(sources_per_day[sources_per_day > 1].index.astype(str))
        )
        if ambiguous_days:
            reasons.append(
                "multiple eligible captures share a trading date; independent-session "
                "evidence is ambiguous: " + ", ".join(ambiguous_days)
            )
    for family in families:
        horizon_rows = int((horizon["family_root"] == family).sum()) if not horizon.empty else 0
        family_labeled = labeled[labeled["family_root"] == family] if not labeled.empty else labeled
        sessions = int(family_labeled["trading_date"].nunique()) if not family_labeled.empty else 0
        family_reasons = []
        if horizon_rows == 0:
            family_reasons.append("no exact-horizon surface rows")
        if sessions < minimum_family_sessions:
            family_reasons.append(
                f"labeled sessions {sessions} < {minimum_family_sessions}"
            )
        if family_reasons:
            reasons.append(f"{family}: {'; '.join(family_reasons)}")
        family_reports.append(
            FamilyTrainingCoverage(
                family_root=family, horizon_rows=horizon_rows,
                labeled_rows=int(len(family_labeled)), labeled_sessions=sessions,
                reasons=tuple(family_reasons),
            )
        )

    labeled_sessions = int(labeled["trading_date"].nunique()) if not labeled.empty else 0
    source_hashes = int(labeled["source_sha256"].nunique()) if not labeled.empty else 0
    if not labeled.empty and "marketpin_predicted_log_return" in labeled.columns:
        incumbent_values = pd.to_numeric(
            labeled["marketpin_predicted_log_return"], errors="coerce"
        )
        incumbent_mask = np.isfinite(incumbent_values.to_numpy(dtype=float))
        incumbent_rows = int(incumbent_mask.sum())
        incumbent_sessions = int(labeled.loc[incumbent_mask, "trading_date"].nunique())
    else:
        incumbent_rows = 0
        incumbent_sessions = 0
    if labeled_sessions < minimum_sessions:
        reasons.append(f"labeled sessions {labeled_sessions} < {minimum_sessions}")
    if source_hashes < minimum_sessions:
        reasons.append(f"independent source hashes {source_hashes} < {minimum_sessions}")
    if incumbent_rows != len(labeled):
        reasons.append(
            f"timestamp-aligned incumbent predictions {incumbent_rows} < labeled rows {len(labeled)}"
        )
    feature_reports: list[FeatureTrainingCoverage] = []
    if not labeled.empty:
        missing_columns = sorted(set(MODEL_FEATURE_COLUMNS) - set(labeled.columns))
        if missing_columns:
            reasons.append(f"model feature contract columns missing: {', '.join(missing_columns)}")
        else:
            numeric = labeled[list(MODEL_FEATURE_COLUMNS)].apply(pd.to_numeric, errors="coerce")
            for feature in MODEL_FEATURE_COLUMNS:
                finite = int(np.isfinite(numeric[feature].to_numpy(dtype=float)).sum())
                missing_ratio = 1.0 - finite / len(numeric)
                feature_reports.append(
                    FeatureTrainingCoverage(feature, finite, float(missing_ratio))
                )
                if finite == 0:
                    reasons.append(f"model feature has no finite values: {feature}")
    elif horizon.empty:
        reasons.append("no rows exist at the exact decision horizon")
    else:
        reasons.append("no verified official closes align with decision rows")

    report = CloseTrainingReadinessReport(
        ready=not reasons,
        decision_horizon_minutes_before_close=minutes_before_close,
        horizon_rows=int(len(horizon)), labeled_rows=int(len(labeled)),
        labeled_sessions=labeled_sessions, source_hashes=source_hashes,
        incumbent_rows=incumbent_rows, incumbent_sessions=incumbent_sessions,
        minimum_sessions=minimum_sessions,
        minimum_family_sessions=minimum_family_sessions,
        reasons=tuple(dict.fromkeys(reasons)),
        family_coverage=tuple(family_reports), feature_coverage=tuple(feature_reports),
    )
    return labeled, report
