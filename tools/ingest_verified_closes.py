from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import math
import os
import re
import shutil
import sqlite3
import sys
import uuid
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.closing_tape.close_evidence import (
    VERIFIED_CLOSE_SOURCES_BY_SYMBOL,
    validate_official_close_reference,
    validate_source_artifact_sha256,
    validate_verified_close_observed_at,
)
from backend.database_target import (
    configured_market_database_path,
    sqlite_database_path_from_url,
)
PRODUCTION_CLOSE_FAMILIES = frozenset(VERIFIED_CLOSE_SOURCES_BY_SYMBOL)
CORE_CLOSE_FAMILIES = frozenset({"SPX", "NDX"})
BUNDLE_PROFILES = {
    "full": PRODUCTION_CLOSE_FAMILIES,
    "core": CORE_CLOSE_FAMILIES,
}


@dataclass(frozen=True)
class VerifiedCloseInput:
    symbol: str
    trading_date: date
    official_close: float
    source: str
    source_reference: str
    source_artifact_sha256: str
    source_artifact_path: Path
    observed_at_utc: datetime
    correction_of_id: int | None


class OfficialArtifactStaleError(ValueError):
    """The retained official artifact does not publish the requested trading day."""


def _artifact_text(path: Path) -> str:
    try:
        rendered = path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("official rendered artifact must be UTF-8 text or HTML") from exc
    rendered = html.unescape(re.sub(r"<[^>]+>", " ", rendered))
    return re.sub(r"\s+", " ", rendered).strip()


def _date_tokens(trading_day: date) -> tuple[str, ...]:
    return (
        trading_day.isoformat(),
        trading_day.strftime("%m/%d/%Y"),
        trading_day.strftime("%B %d, %Y"),
        trading_day.strftime("%b %d, %Y"),
        trading_day.strftime("%B %d %Y"),
        trading_day.strftime("%b %d %Y"),
        trading_day.strftime("%B %-d, %Y") if os.name != "nt" else trading_day.strftime("%B %#d, %Y"),
        trading_day.strftime("%b %-d, %Y") if os.name != "nt" else trading_day.strftime("%b %#d, %Y"),
        trading_day.strftime("%B %-d %Y") if os.name != "nt" else trading_day.strftime("%B %#d %Y"),
        trading_day.strftime("%b %-d %Y") if os.name != "nt" else trading_day.strftime("%b %#d %Y"),
    )


def _text_contains_close(rendered: str, official_close: float) -> bool:
    for token in re.findall(r"(?<![\w.])-?\d[\d,]*(?:\.\d+)?(?![\w.])", rendered):
        try:
            candidate = float(token.replace(",", ""))
        except ValueError:
            continue
        if math.isclose(candidate, official_close, rel_tol=0.0, abs_tol=1e-9):
            return True
    return False


def _validate_spx_rendered_artifact(row: VerifiedCloseInput) -> None:
    if row.source_artifact_path.suffix.lower() not in {".html", ".htm", ".txt"}:
        raise ValueError("SPX official artifact must be retained rendered HTML or text")
    rendered = _artifact_text(row.source_artifact_path)
    if not re.search(r"(?:S\s*&\s*P\s*500|\bSPX\b)", rendered, flags=re.IGNORECASE):
        raise ValueError("SPX official artifact does not identify the S&P 500/SPX index")
    if not any(token in rendered for token in _date_tokens(row.trading_date)):
        raise OfficialArtifactStaleError(
            "SPX official artifact does not contain the requested trading date"
        )
    if not _text_contains_close(rendered, row.official_close):
        raise ValueError("SPX official artifact does not contain the supplied official close")


def _normalized_key(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


def _ndx_record_matches(
    record: dict[str, object],
    row: VerifiedCloseInput,
    *,
    inherited_ndx_identity: bool = False,
) -> bool:
    symbol_keys = {"symbol", "ticker", "indexsymbol", "indexcode", "indexname", "name"}
    date_keys = {"date", "tradingdate", "asofdate", "tradedate"}
    close_keys = {
        "close", "closevalue", "officialclose", "closingvalue", "indexvalue",
        "lastsale", "lastsaleprice",
    }
    symbol_values = [value for key, value in record.items() if _normalized_key(key) in symbol_keys]
    identifies_ndx = inherited_ndx_identity or any(
        re.search(r"\bNDX\b", str(value), flags=re.IGNORECASE)
        or re.search(r"NASDAQ\s*-?\s*100", str(value), flags=re.IGNORECASE)
        for value in symbol_values
    )
    if not identifies_ndx:
        return False
    dates = [str(value).strip() for key, value in record.items() if _normalized_key(key) in date_keys]
    if not any(any(token == candidate or token in candidate for token in _date_tokens(row.trading_date)) for candidate in dates):
        return False
    closes = [value for key, value in record.items() if _normalized_key(key) in close_keys]
    for value in closes:
        try:
            candidate = float(str(value).replace("$", "").replace(",", "").strip())
        except ValueError:
            continue
        if math.isclose(candidate, row.official_close, rel_tol=0.0, abs_tol=1e-9):
            return True
    return False


def _validate_ndx_json_artifact(row: VerifiedCloseInput) -> None:
    if row.source_artifact_path.suffix.lower() != ".json":
        raise ValueError("NDX official artifact must be retained Nasdaq JSON")
    try:
        payload = json.loads(row.source_artifact_path.read_text(encoding="utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("NDX official artifact must contain valid UTF-8 JSON") from exc
    pending = [(payload, False)]
    requested_date_seen = False
    while pending:
        item, inherited_ndx_identity = pending.pop()
        if isinstance(item, dict):
            symbol_values = [
                value
                for key, value in item.items()
                if _normalized_key(key)
                in {"symbol", "ticker", "indexsymbol", "indexcode", "indexname", "name"}
            ]
            identifies_ndx = inherited_ndx_identity or any(
                re.search(r"\bNDX\b", str(value), flags=re.IGNORECASE)
                or re.search(r"NASDAQ\s*-?\s*100", str(value), flags=re.IGNORECASE)
                for value in symbol_values
            )
            if identifies_ndx:
                date_values = [
                    str(value).strip()
                    for key, value in item.items()
                    if _normalized_key(key) in {"date", "tradingdate", "asofdate", "tradedate"}
                ]
                requested_date_seen = requested_date_seen or any(
                    any(token == candidate or token in candidate for token in _date_tokens(row.trading_date))
                    for candidate in date_values
                )
            if _ndx_record_matches(
                item,
                row,
                inherited_ndx_identity=identifies_ndx,
            ):
                return
            pending.extend((value, identifies_ndx) for value in item.values())
        elif isinstance(item, list):
            pending.extend((value, inherited_ndx_identity) for value in item)
    if not requested_date_seen:
        raise OfficialArtifactStaleError(
            "NDX official JSON does not contain the requested trading date"
        )
    raise ValueError("NDX official JSON has no matching symbol/date/close record")


def _validate_vix_csv_artifact(row: VerifiedCloseInput) -> None:
    if row.source_artifact_path.suffix.lower() != ".csv":
        raise ValueError("VIX official artifact must be retained Cboe CSV")
    try:
        with row.source_artifact_path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            normalized_fields = {
                _normalized_key(field): field for field in (reader.fieldnames or ())
            }
            date_field = normalized_fields.get("date")
            close_field = normalized_fields.get("close")
            if date_field is None or close_field is None:
                raise ValueError("VIX official CSV must contain DATE and CLOSE columns")
            requested_date_seen = False
            for record in reader:
                raw_date = str(record.get(date_field) or "").strip()
                try:
                    record_date = datetime.strptime(raw_date, "%m/%d/%Y").date()
                    record_close = float(str(record.get(close_field) or "").strip())
                except ValueError:
                    continue
                if record_date == row.trading_date:
                    requested_date_seen = True
                    if math.isclose(
                        record_close, row.official_close, rel_tol=0.0, abs_tol=1e-9
                    ):
                        return
    except UnicodeDecodeError as exc:
        raise ValueError("VIX official artifact must contain valid UTF-8 CSV") from exc
    if not requested_date_seen:
        raise OfficialArtifactStaleError(
            "VIX official CSV does not contain the requested trading date"
        )
    raise ValueError("VIX official CSV has no matching date/close record")


def _validate_cboe_index_history_artifact(
    row: VerifiedCloseInput, *, expected_symbol: str
) -> None:
    if row.source != "cboe-official":
        raise ValueError(f"{expected_symbol} Cboe history requires source cboe-official")
    if row.source_artifact_path.suffix.lower() != ".csv":
        raise ValueError(f"{expected_symbol} Cboe history must be retained as CSV")
    reference = urlparse(row.source_reference)
    expected_path = (
        f"/api/global/us_indices/daily_prices/{expected_symbol}_History.csv"
    )
    if (
        reference.scheme.lower() != "https"
        or (reference.hostname or "").lower() != "cdn.cboe.com"
        or reference.path != expected_path
        or reference.query
        or reference.fragment
    ):
        raise ValueError(
            f"{expected_symbol} source reference must be the exact Cboe history CSV endpoint"
        )
    try:
        with row.source_artifact_path.open(
            "r", encoding="utf-8-sig", newline=""
        ) as stream:
            reader = csv.DictReader(stream)
            expected_fields = ("DATE", expected_symbol)
            actual_fields = tuple(
                str(field).strip().upper() for field in (reader.fieldnames or ())
            )
            if actual_fields != expected_fields:
                raise ValueError(
                    f"{expected_symbol} Cboe history CSV must contain exactly "
                    f"DATE and {expected_symbol} columns"
                )
            requested_values: list[float] = []
            for record in reader:
                try:
                    record_date = datetime.strptime(
                        str(record.get("DATE") or "").strip(), "%m/%d/%Y"
                    ).date()
                    record_close = float(
                        str(record.get(expected_symbol) or "").strip()
                    )
                except ValueError:
                    continue
                if record_date == row.trading_date:
                    requested_values.append(record_close)
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"{expected_symbol} Cboe history artifact must contain valid UTF-8 CSV"
        ) from exc
    if not requested_values:
        raise OfficialArtifactStaleError(
            f"{expected_symbol} Cboe history CSV does not contain the requested trading date"
        )
    if len(requested_values) != 1:
        raise ValueError(
            f"{expected_symbol} Cboe history CSV contains duplicate requested-date rows"
        )
    if not math.isclose(
        requested_values[0], row.official_close, rel_tol=0.0, abs_tol=1e-9
    ):
        raise ValueError(
            f"{expected_symbol} Cboe history CSV has no matching date/close record"
        )


def _pdf_text(path: Path) -> str:
    try:
        from pypdf import PdfReader
    except ImportError as exc:
        raise ValueError(
            "RUT official PDF validation requires the pypdf runtime dependency"
        ) from exc
    try:
        reader = PdfReader(path)
        return "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception as exc:
        raise ValueError("RUT official artifact must be a readable PDF") from exc


def _validate_rut_pdf_artifact(row: VerifiedCloseInput) -> None:
    if row.source_artifact_path.suffix.lower() != ".pdf":
        raise ValueError("RUT official artifact must be retained FTSE Russell PDF")
    rendered = _pdf_text(row.source_artifact_path)
    if not re.search(r"Russell\s+indexes", rendered, flags=re.IGNORECASE) or not re.search(
        r"Daily\s+Values", rendered, flags=re.IGNORECASE
    ):
        raise ValueError("RUT official PDF does not identify FTSE Russell Daily Values")
    if not any(token in rendered for token in _date_tokens(row.trading_date)):
        raise OfficialArtifactStaleError(
            "RUT official PDF does not contain the requested trading date"
        )
    for line in rendered.splitlines():
        match = re.match(
            r"\s*Russell\s+2000(?:[^\w\s])?\s+Index\s+(.*)$",
            line,
            flags=re.IGNORECASE,
        )
        if match is None:
            continue
        values = []
        for token in re.findall(r"-?\d[\d,]*(?:\.\d+)?", match.group(1)):
            try:
                values.append(float(token.replace(",", "")))
            except ValueError:
                continue
        # The published row begins Open, High, Low, Close. Binding the close
        # to this exact row prevents a different index value elsewhere in the
        # document from satisfying the evidence check.
        if len(values) >= 4 and math.isclose(
            values[3], row.official_close, rel_tol=0.0, abs_tol=1e-9
        ):
            return
    raise ValueError("RUT official PDF has no matching Russell 2000 date/close row")


def _validate_spy_json_artifact(row: VerifiedCloseInput) -> None:
    if row.source_artifact_path.suffix.lower() != ".json":
        raise ValueError("SPY official artifact must be retained NYSE JSON")
    reference = urlparse(row.source_reference)
    query = parse_qs(reference.query, keep_blank_values=True)
    if (
        reference.path.rstrip("/") != "/api/nyseservice/v1/quotes"
        or set(query) != {"symbol"}
        or [value.strip().upper() for value in query["symbol"]] != ["SPY"]
    ):
        raise ValueError("SPY source reference must be the exact NYSE SPY quote JSON endpoint")
    try:
        payload = json.loads(row.source_artifact_path.read_text(encoding="utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("SPY official artifact must contain valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("SPY official JSON must contain a top-level object")
    quote = payload.get("quote")
    history = payload.get("quoteHistory")
    if not isinstance(quote, dict) or str(quote.get("exchg") or "").upper() != "ARCX":
        raise ValueError("SPY official JSON does not identify the NYSE Arca listing")
    if not isinstance(history, dict) or str(history.get("symbol") or "").upper() != "SPY":
        raise ValueError("SPY official JSON does not identify SPY quote history")
    history_rows = history.get("historyList")
    if not isinstance(history_rows, list):
        raise ValueError("SPY official JSON must contain quoteHistory.historyList")
    expected_date = row.trading_date.strftime("%Y/%m/%d")
    requested_date_seen = False
    for record in history_rows:
        if not isinstance(record, dict) or str(record.get("date") or "") != expected_date:
            continue
        requested_date_seen = True
        try:
            record_close = float(str(record.get("close") or "").replace(",", "").strip())
        except ValueError:
            continue
        if math.isclose(record_close, row.official_close, rel_tol=0.0, abs_tol=1e-9):
            return
    if not requested_date_seen:
        raise OfficialArtifactStaleError(
            "SPY official JSON does not contain the requested trading date"
        )
    raise ValueError("SPY official JSON has no matching NYSE Arca date/close record")


def validate_official_artifact_semantics(row: VerifiedCloseInput) -> None:
    """Bind every accepted close value to the retained first-party content."""
    if row.symbol == "SPX":
        if row.source == "cboe-official":
            _validate_cboe_index_history_artifact(row, expected_symbol="SPX")
        else:
            _validate_spx_rendered_artifact(row)
    elif row.symbol == "NDX":
        _validate_ndx_json_artifact(row)
    elif row.symbol == "RUT":
        if row.source == "cboe-official":
            _validate_cboe_index_history_artifact(row, expected_symbol="RUT")
        else:
            _validate_rut_pdf_artifact(row)
    elif row.symbol == "VIX":
        _validate_vix_csv_artifact(row)
    elif row.symbol == "SPY":
        _validate_spy_json_artifact(row)
    else:
        raise ValueError(
            f"{row.symbol} official artifact semantic validation is not implemented; "
            "ingestion fails closed"
        )


def canonical_database_url(project_root: str | Path) -> str:
    database_path = (Path(project_root).resolve() / "data" / "market_data.db").as_posix()
    return f"sqlite:///{database_path}"


def _require_configured_database_bindings(database_path: Path):
    """Return backend.database only when every writer is bound to one target."""
    import backend.database as database_module

    expected = database_path.resolve()
    engine_path = sqlite_database_path_from_url(database_module.engine.url)
    session_bind = database_module.SessionLocal.kw.get("bind")
    if session_bind is None:
        raise RuntimeError("backend.database SessionLocal has no configured engine")
    session_path = sqlite_database_path_from_url(session_bind.url)
    if engine_path != expected or session_path != expected:
        raise RuntimeError(
            "verified-close ingestion database target mismatch: "
            f"configured={expected}, engine={engine_path}, session={session_path}; "
            "restart the process with one DATABASE_URL before importing backend.database"
        )
    return database_module


def parse_verified_close_record(
    raw: dict[str, object], *, source_path: str | Path
) -> VerifiedCloseInput:
    """Validate one candidate without mutating the evidence ledger or database."""
    csv_path = Path(source_path)
    symbol = str(raw.get("symbol") or "").strip().upper()
    source = str(raw.get("source") or "").strip().lower()
    if source not in VERIFIED_CLOSE_SOURCES_BY_SYMBOL.get(symbol, set()):
        raise ValueError(f"source {source!r} is not approved for {symbol!r}")
    reference = validate_official_close_reference(
        symbol, source, str(raw.get("source_reference") or "")
    )
    artifact_hash = validate_source_artifact_sha256(
        str(raw.get("source_artifact_sha256") or "")
    )
    artifact_path = Path(str(raw.get("source_artifact_path") or "").strip())
    if not artifact_path.is_absolute():
        artifact_path = csv_path.parent / artifact_path
    artifact_path = artifact_path.resolve()
    if not artifact_path.is_file():
        raise ValueError("source_artifact_path must identify a retained local file")
    artifact_size = artifact_path.stat().st_size
    if artifact_size <= 0 or artifact_size > 100 * 1024 * 1024:
        raise ValueError("source artifact must be between 1 byte and 100 MiB")
    digest = hashlib.sha256()
    with artifact_path.open("rb") as artifact_stream:
        for chunk in iter(lambda: artifact_stream.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != artifact_hash:
        raise ValueError("source artifact bytes do not match source_artifact_sha256")
    trading_day = date.fromisoformat(str(raw.get("trading_date") or ""))
    observed = validate_verified_close_observed_at(
        trading_day,
        str(raw.get("observed_at_utc") or ""),
    )
    close = float(raw.get("official_close") or 0)
    if close <= 0:
        raise ValueError("official_close must be positive")
    correction_raw = str(raw.get("correction_of_id") or "").strip()
    row = VerifiedCloseInput(
        symbol=symbol,
        trading_date=trading_day,
        official_close=close,
        source=source,
        source_reference=reference,
        source_artifact_sha256=artifact_hash,
        source_artifact_path=artifact_path,
        observed_at_utc=observed,
        correction_of_id=int(correction_raw) if correction_raw else None,
    )
    validate_official_artifact_semantics(row)
    return row


def read_verified_close_csv(path: str | Path) -> list[VerifiedCloseInput]:
    source_path = Path(path)
    rows: list[VerifiedCloseInput] = []
    with source_path.open("r", encoding="utf-8-sig", newline="") as stream:
        for line_number, raw in enumerate(csv.DictReader(stream), start=2):
            try:
                rows.append(parse_verified_close_record(raw, source_path=source_path))
            except Exception as exc:
                raise ValueError(f"invalid verified-close row {line_number}: {exc}") from exc
    if not rows:
        raise ValueError("verified-close CSV contains no rows")
    return rows


def stage_source_artifacts(
    rows: list[VerifiedCloseInput], *, project_root: str | Path
) -> dict[tuple[date, str, str], Path]:
    """Retain exact official bytes in a deterministic content-addressed vault."""
    root = Path(project_root).resolve()
    staged: dict[tuple[date, str, str], Path] = {}
    for row in rows:
        suffix = row.source_artifact_path.suffix.lower()
        if not suffix or len(suffix) > 10 or not suffix[1:].isalnum():
            suffix = ".bin"
        destination = (
            root / "data" / "verified_close_sources" / row.trading_date.isoformat()
            / row.symbol / f"{row.source_artifact_sha256}{suffix}"
        )
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            existing = hashlib.sha256(destination.read_bytes()).hexdigest()
            if existing != row.source_artifact_sha256:
                raise ValueError(f"vault artifact conflicts with its content hash: {destination}")
        else:
            temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
            try:
                shutil.copyfile(row.source_artifact_path, temporary)
                copied = hashlib.sha256(temporary.read_bytes()).hexdigest()
                if copied != row.source_artifact_sha256:
                    raise ValueError("staged artifact changed during copy")
                os.replace(temporary, destination)
            finally:
                if temporary.exists():
                    temporary.unlink()
        staged[(row.trading_date, row.symbol, row.source_artifact_sha256)] = destination
    return staged


def validate_close_bundles(
    rows: list[VerifiedCloseInput],
    *,
    allow_partial_correction: bool = False,
    bundle_profile: str = "full",
) -> None:
    """Require one unambiguous five-family outcome bundle per trading date."""
    keys = [(row.trading_date, row.symbol) for row in rows]
    if len(keys) != len(set(keys)):
        raise ValueError("verified-close CSV contains duplicate symbol/trading-date rows")
    if allow_partial_correction:
        if any(row.correction_of_id is None for row in rows):
            raise ValueError(
                "partial close import is reserved for rows with explicit correction_of_id lineage"
            )
        return
    expected_families = BUNDLE_PROFILES.get(bundle_profile)
    if expected_families is None:
        raise ValueError(f"unknown verified-close bundle profile: {bundle_profile!r}")
    by_date: dict[date, set[str]] = {}
    for row in rows:
        by_date.setdefault(row.trading_date, set()).add(row.symbol)
    incomplete = {
        trading_date.isoformat(): sorted(expected_families - symbols)
        for trading_date, symbols in by_date.items()
        if symbols != expected_families
    }
    if incomplete:
        details = "; ".join(
            f"{trading_date} missing {','.join(missing)}"
            for trading_date, missing in sorted(incomplete.items())
        )
        raise ValueError(f"verified-close session bundle is incomplete: {details}")


def reconcile_governed_prediction_outcomes(
    project_root: str | Path,
    trading_days: set[date],
    *,
    market_db_path: str | Path | None = None,
) -> dict[str, dict[str, object]]:
    """Idempotently score approval-bound forecasts after close-bundle commit."""
    from backend.closing_tape.governance import score_promoted_prediction_outcomes

    root = Path(project_root).resolve()
    market_database = (
        Path(market_db_path).resolve()
        if market_db_path is not None
        else configured_market_database_path(root)
    )
    reports: dict[str, dict[str, object]] = {}
    no_eligible_reasons = {
        "market_database_missing",
        "required_prediction_or_close_evidence_missing",
        "no_governed_promoted_predictions_selected",
    }
    for trading_day in sorted(trading_days):
        try:
            report = score_promoted_prediction_outcomes(
                market_database,
                trading_day=trading_day,
                project_root=root,
            )
        except (KeyError, OSError, RuntimeError, ValueError, sqlite3.DatabaseError) as exc:
            raise RuntimeError(
                f"verified closes committed, but governed forecast scoring failed for "
                f"{trading_day.isoformat()}: {exc}"
            ) from exc
        reason = str(report.get("reason") or "")
        if reason and reason not in no_eligible_reasons:
            raise RuntimeError(
                f"verified closes committed, but governed forecast scoring was incomplete "
                f"for {trading_day.isoformat()}: {reason}"
            )
        reports[trading_day.isoformat()] = report
    return reports


def ingest(
    rows: list[VerifiedCloseInput],
    *,
    project_root: str | Path,
    allow_partial_correction: bool = False,
    bundle_profile: str = "full",
    score_prediction_ids_by_symbol: dict[str, list[int]] | None = None,
) -> list[dict[str, object]]:
    validate_close_bundles(
        rows,
        allow_partial_correction=allow_partial_correction,
        bundle_profile=bundle_profile,
    )
    selected = {
        str(symbol).strip().upper(): sorted({int(value) for value in values})
        for symbol, values in (score_prediction_ids_by_symbol or {}).items()
    }
    row_symbols = {row.symbol for row in rows}
    unknown_symbols = sorted(set(selected) - row_symbols)
    if unknown_symbols:
        raise ValueError(
            "prediction selections reference symbols outside the close bundle: "
            + ",".join(unknown_symbols)
        )
    if any(value <= 0 for values in selected.values() for value in values):
        raise ValueError("selected prediction IDs must be positive integers")
    root = Path(project_root).resolve()
    market_database = configured_market_database_path(root)
    database_module = _require_configured_database_bindings(market_database)
    init_db = database_module.init_db
    score_prediction_snapshots = database_module.score_prediction_snapshots
    upsert_verified_eod_close_bundle = database_module.upsert_verified_eod_close_bundle
    staged = stage_source_artifacts(rows, project_root=root)
    init_db()
    for row in rows:
        staged_path = staged[
            (row.trading_date, row.symbol, row.source_artifact_sha256)
        ]
        staged_hash = hashlib.sha256(staged_path.read_bytes()).hexdigest()
        if staged_hash != row.source_artifact_sha256:
            raise ValueError("staged artifact no longer matches its content hash")
        validate_official_artifact_semantics(
            replace(row, source_artifact_path=staged_path)
        )

    closes = upsert_verified_eod_close_bundle([
        {
            "symbol": row.symbol,
            "trading_date": row.trading_date,
            "official_close": row.official_close,
            "source": row.source,
            "source_reference": row.source_reference,
            "source_artifact_sha256": row.source_artifact_sha256,
            "observed_at_utc": row.observed_at_utc,
            "correction_of_id": row.correction_of_id,
        }
        for row in rows
    ])
    governance_scores = reconcile_governed_prediction_outcomes(
        root,
        {row.trading_date for row in rows},
        market_db_path=market_database,
    )
    results = []
    for row, close in zip(rows, closes, strict=True):
        prediction_ids = selected.get(row.symbol, [])
        score = (
            score_prediction_snapshots(
                row.symbol,
                row.trading_date,
                prediction_ids=prediction_ids,
            )
            if prediction_ids
            else {"scored": 0, "reason": "no_explicit_prediction_selection"}
        )
        results.append(
            {
                "symbol": row.symbol,
                "trading_date": row.trading_date.isoformat(),
                "official_close": close.official_close,
                "source": row.source,
                "source_artifact_path": str(
                    staged[(row.trading_date, row.symbol, row.source_artifact_sha256)]
                ),
                "scored": int(score.get("scored") or 0),
                "governed_forecast_scoring": governance_scores[
                    row.trading_date.isoformat()
                ],
            }
        )
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Ingest explicitly sourced official closes into MarketPin's immutable evidence ledger"
    )
    parser.add_argument("csv_path")
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    parser.add_argument(
        "--bundle-profile",
        choices=sorted(BUNDLE_PROFILES),
        default="full",
        help="required family set: full=SPX,NDX,RUT,VIX,SPY; core=SPX,NDX",
    )
    parser.add_argument(
        "--score-prediction",
        action="append",
        default=[],
        metavar="SYMBOL:ID",
        help="explicit prediction snapshot to score; repeat for additional rows",
    )
    parser.add_argument(
        "--allow-partial-correction",
        action="store_true",
        help="allow a partial bundle only when every row has explicit correction lineage",
    )
    args = parser.parse_args(argv)
    selections: dict[str, list[int]] = {}
    for raw_selection in args.score_prediction:
        try:
            raw_symbol, raw_id = raw_selection.rsplit(":", 1)
            normalized_symbol = raw_symbol.strip().upper()
            prediction_id = int(raw_id)
        except (ValueError, AttributeError) as exc:
            parser.error(f"invalid --score-prediction {raw_selection!r}; expected SYMBOL:ID")
        if not normalized_symbol or prediction_id <= 0:
            parser.error(f"invalid --score-prediction {raw_selection!r}; expected SYMBOL:positive-ID")
        selections.setdefault(normalized_symbol, []).append(prediction_id)
    results = ingest(
        read_verified_close_csv(args.csv_path),
        project_root=args.project_root,
        allow_partial_correction=args.allow_partial_correction,
        bundle_profile=args.bundle_profile,
        score_prediction_ids_by_symbol=selections,
    )
    for result in results:
        print(
            f"{result['symbol']} {result['trading_date']} close={result['official_close']} "
            f"source={result['source']} scored={result['scored']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
