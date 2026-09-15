from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.closing_tape.close_evidence import VERIFIED_CLOSE_SOURCES_BY_SYMBOL
from tools.ingest_verified_closes import (
    BUNDLE_PROFILES,
    OfficialArtifactStaleError,
    parse_verified_close_record,
)


def audit_close_bundle_csv(
    path: str | Path,
    *,
    trading_date: date,
    bundle_profile: str = "full",
) -> dict[str, object]:
    """Classify retained candidates without staging, ingesting, or scoring anything."""
    expected = BUNDLE_PROFILES.get(bundle_profile)
    if expected is None:
        raise ValueError(f"unknown verified-close bundle profile: {bundle_profile!r}")

    source_path = Path(path)
    candidates: dict[str, list[tuple[int, dict[str, object]]]] = {
        symbol: [] for symbol in expected
    }
    global_errors: list[dict[str, object]] = []
    ignored_other_dates: list[dict[str, object]] = []
    ignored_extra_families: list[dict[str, object]] = []
    with source_path.open("r", encoding="utf-8-sig", newline="") as stream:
        for line_number, raw in enumerate(csv.DictReader(stream), start=2):
            symbol = str(raw.get("symbol") or "").strip().upper()
            try:
                row_date = date.fromisoformat(str(raw.get("trading_date") or ""))
            except ValueError:
                global_errors.append(
                    {"line": line_number, "error": "trading_date must be an ISO date"}
                )
                continue
            if row_date != trading_date:
                ignored_other_dates.append(
                    {"line": line_number, "symbol": symbol, "trading_date": row_date.isoformat()}
                )
                continue
            if symbol not in VERIFIED_CLOSE_SOURCES_BY_SYMBOL:
                global_errors.append(
                    {"line": line_number, "error": f"unsupported close family {symbol!r}"}
                )
                continue
            if symbol not in expected:
                ignored_extra_families.append({"line": line_number, "symbol": symbol})
                continue
            candidates[symbol].append((line_number, raw))

    families: dict[str, dict[str, object]] = {}
    for symbol in sorted(expected):
        symbol_candidates = candidates[symbol]
        if not symbol_candidates:
            families[symbol] = {"status": "MISSING"}
            continue
        if len(symbol_candidates) > 1:
            families[symbol] = {
                "status": "DUPLICATE",
                "lines": [line_number for line_number, _ in symbol_candidates],
                "error": "more than one candidate exists for this symbol and trading date",
            }
            continue
        line_number, raw = symbol_candidates[0]
        try:
            row = parse_verified_close_record(raw, source_path=source_path)
        except OfficialArtifactStaleError as exc:
            families[symbol] = {
                "status": "STALE",
                "line": line_number,
                "error": str(exc),
            }
        except Exception as exc:
            families[symbol] = {
                "status": "INVALID",
                "line": line_number,
                "error": str(exc),
            }
        else:
            families[symbol] = {
                "status": "SEMANTICALLY_VALID",
                "line": line_number,
                "official_close": row.official_close,
                "source": row.source,
                "source_artifact_sha256": row.source_artifact_sha256,
                "source_artifact_path": str(row.source_artifact_path),
            }

    status_by_symbol = {
        symbol: str(details["status"]) for symbol, details in families.items()
    }
    valid = sorted(
        symbol for symbol, status in status_by_symbol.items() if status == "SEMANTICALLY_VALID"
    )
    missing = sorted(symbol for symbol, status in status_by_symbol.items() if status == "MISSING")
    stale = sorted(symbol for symbol, status in status_by_symbol.items() if status == "STALE")
    invalid = sorted(
        symbol for symbol, status in status_by_symbol.items() if status in {"INVALID", "DUPLICATE"}
    )
    ready = len(valid) == len(expected) and not global_errors
    if ready:
        bundle_status = "READY"
    elif invalid or global_errors:
        bundle_status = "INVALID"
    else:
        bundle_status = "INCOMPLETE"
    return {
        "trading_date": trading_date.isoformat(),
        "bundle_profile": bundle_profile,
        "bundle_status": bundle_status,
        "ready_for_ingestion": ready,
        "families": families,
        "valid_symbols": valid,
        "missing_symbols": missing,
        "stale_symbols": stale,
        "invalid_symbols": invalid,
        "global_errors": global_errors,
        "ignored_other_dates": ignored_other_dates,
        "ignored_extra_families": ignored_extra_families,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Audit retained official-close candidates without writing labels, "
            "scoring predictions, or mutating the database"
        )
    )
    parser.add_argument("csv_path")
    parser.add_argument("--trading-date", required=True)
    parser.add_argument("--bundle-profile", choices=sorted(BUNDLE_PROFILES), default="full")
    args = parser.parse_args(argv)
    report = audit_close_bundle_csv(
        args.csv_path,
        trading_date=date.fromisoformat(args.trading_date),
        bundle_profile=args.bundle_profile,
    )
    print(json.dumps(report, sort_keys=True, separators=(",", ":"), allow_nan=False))
    return 0 if report["ready_for_ingestion"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
