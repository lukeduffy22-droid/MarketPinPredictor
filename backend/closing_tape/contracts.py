"""Lightweight, versioned evidence contracts shared across tape boundaries."""

EVIDENCE_CONTRACT_VERSION = "tcbbo-observed-v7"
HISTORICAL_EVIDENCE_CONTRACT_VERSION = "databento-historical-bundle-v1"

LIVE_SOURCE_KIND = "databento_live"
HISTORICAL_SOURCE_KIND = "databento_historical"


def evidence_contract_for_source_kind(source_kind: str) -> str:
    """Return the only evidence contract accepted for a source kind."""

    normalized = str(source_kind or "").strip().lower()
    if normalized == LIVE_SOURCE_KIND:
        return EVIDENCE_CONTRACT_VERSION
    if normalized == HISTORICAL_SOURCE_KIND:
        return HISTORICAL_EVIDENCE_CONTRACT_VERSION
    raise ValueError(f"unsupported closing-tape source kind: {source_kind or '<missing>'}")
