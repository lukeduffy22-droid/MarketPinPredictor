"""Helpers for identifying gamma-snapshot export folders.

The ``exports`` directory also contains prediction journals and rendered
screenshots.  Those artifacts have different schemas and must not be counted
as index gamma snapshots in the Streamlit export viewer.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Union


PathLike = Union[str, Path]


def is_gamma_snapshot_file(path: PathLike) -> bool:
    """Return whether an NDJSON file contains gamma-snapshot records.

    The first decodable non-empty record determines the file schema.  Both
    valid and invalid calculations are legitimate snapshots, so field
    presence—not the truth value of ``validation_is_valid``—is the gate.
    """

    candidate = Path(path)
    if not candidate.is_file() or candidate.suffix.lower() != ".ndjson":
        return False

    try:
        with candidate.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(record, dict):
                    return False
                return (
                    "validation_is_valid" in record
                    and "symbol" in record
                    and (
                        "snapshot_version" in record
                        or "gex_formula_version" in record
                    )
                )
    except OSError:
        return False

    return False


def list_gamma_snapshot_symbols(export_base: PathLike) -> list[str]:
    """List symbol folders that contain at least one gamma-snapshot file."""

    root = Path(export_base)
    if not root.is_dir():
        return []

    symbols: list[str] = []
    for directory in root.iterdir():
        if not directory.is_dir():
            continue
        files = sorted(
            directory.glob("*.ndjson"),
            key=lambda item: item.name,
            reverse=True,
        )
        if any(is_gamma_snapshot_file(path) for path in files):
            symbols.append(directory.name)

    return sorted(symbols)
