from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from .dataset import (
    PRODUCTION_FAMILIES,
    attach_point_in_time_reference_prices,
    load_complete_contract_tape_features,
    load_marketpin_reference_prices,
)
from .parity import estimate_tcbbo_parity_reference_prices
from .surface import (
    MODEL_FEATURE_COLUMNS,
    MODEL_FEATURE_CONTRACT_HASH,
    build_contract_surface_features,
)


@dataclass(frozen=True)
class FamilySurfaceCoverage:
    family_root: str
    contract_rows: int
    primary_reference_rows: int
    parity_reference_rows: int
    primary_matches: int
    fallback_matches: int
    surface_rows: int
    exclusion_reasons: tuple[str, ...]


@dataclass(frozen=True)
class ResearchSurfaceReport:
    catalog_count: int
    eligible_contract_rows: int
    surface_rows: int
    sessions: int
    feature_schema_hash: str | None
    model_feature_contract_hash: str
    model_feature_columns: tuple[str, ...]
    source_sha256s: tuple[str, ...]
    family_coverage: tuple[FamilySurfaceCoverage, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def build_research_surface_dataset(
    catalog_paths: Iterable[str | Path],
    market_db_path: str | Path,
    *,
    expected_families: Iterable[str] = PRODUCTION_FAMILIES,
    max_price_age_seconds: float = 90.0,
) -> tuple[pd.DataFrame, ResearchSurfaceReport]:
    """Assemble the auditable contract-to-surface research dataset."""
    paths = tuple(Path(path) for path in catalog_paths)
    expected = tuple(dict.fromkeys(str(value).upper() for value in expected_families))
    contracts = load_complete_contract_tape_features(paths)
    if contracts.empty:
        coverage = tuple(
            FamilySurfaceCoverage(
                family_root=family, contract_rows=0, primary_reference_rows=0,
                parity_reference_rows=0, primary_matches=0, fallback_matches=0,
                surface_rows=0, exclusion_reasons=("no eligible finalized contract rows",),
            )
            for family in expected
        )
        return pd.DataFrame(), ResearchSurfaceReport(
            catalog_count=len(paths), eligible_contract_rows=0, surface_rows=0,
            sessions=0, feature_schema_hash=None, source_sha256s=(),
            model_feature_contract_hash=MODEL_FEATURE_CONTRACT_HASH,
            model_feature_columns=MODEL_FEATURE_COLUMNS,
            family_coverage=coverage,
        )

    primary = load_marketpin_reference_prices(market_db_path)
    parity = estimate_tcbbo_parity_reference_prices(contracts)
    joined = attach_point_in_time_reference_prices(
        contracts, primary, parity, max_price_age_seconds=max_price_age_seconds
    )
    surface = build_contract_surface_features(joined) if not joined.empty else pd.DataFrame()
    schema_hashes = (
        sorted(surface["feature_schema_hash"].dropna().astype(str).unique().tolist())
        if not surface.empty else []
    )
    if len(schema_hashes) > 1:
        raise ValueError("research surface contains multiple feature schema hashes")

    coverage_rows: list[FamilySurfaceCoverage] = []
    for family in expected:
        contract_count = int((contracts["family_root"] == family).sum())
        primary_count = int((primary["family_root"] == family).sum()) if not primary.empty else 0
        parity_count = int((parity["family_root"] == family).sum()) if not parity.empty else 0
        family_joined = joined[joined["family_root"] == family] if not joined.empty else joined
        tier_counts = (
            family_joined["reference_price_tier"].value_counts().to_dict()
            if not family_joined.empty else {}
        )
        surface_count = int((surface["family_root"] == family).sum()) if not surface.empty else 0
        reasons: list[str] = []
        if contract_count == 0:
            reasons.append("no eligible finalized contract rows")
        elif not len(family_joined):
            if primary_count == 0:
                reasons.append("no eligible primary reference snapshots")
            if parity_count == 0:
                reasons.append("no parity estimate passed pair-count and timestamp gates")
            reasons.append("no point-in-time reference matched within the age limit")
        if contract_count and surface_count == 0:
            reasons.append("no model surface rows survived reference and integrity gates")
        coverage_rows.append(
            FamilySurfaceCoverage(
                family_root=family, contract_rows=contract_count,
                primary_reference_rows=primary_count, parity_reference_rows=parity_count,
                primary_matches=int(tier_counts.get("primary_marketpin_snapshot", 0)),
                fallback_matches=int(tier_counts.get("tcbbo_parity_fallback_estimate", 0)),
                surface_rows=surface_count,
                exclusion_reasons=tuple(dict.fromkeys(reasons)),
            )
        )
    source_hashes = tuple(sorted(contracts["source_sha256"].dropna().astype(str).unique()))
    report = ResearchSurfaceReport(
        catalog_count=len(paths), eligible_contract_rows=int(len(contracts)),
        surface_rows=int(len(surface)),
        sessions=int(surface["session_id"].nunique()) if not surface.empty else 0,
        feature_schema_hash=schema_hashes[0] if schema_hashes else None,
        model_feature_contract_hash=MODEL_FEATURE_CONTRACT_HASH,
        model_feature_columns=MODEL_FEATURE_COLUMNS,
        source_sha256s=source_hashes, family_coverage=tuple(coverage_rows),
    )
    return surface, report
