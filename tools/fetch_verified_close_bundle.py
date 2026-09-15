from __future__ import annotations

import argparse
import csv
import html
import io
import json
import math
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Callable
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.audit_verified_close_bundle import audit_close_bundle_csv
from tools.fetch_verified_close_artifact import fetch_official_artifact
from tools.ingest_verified_closes import (
    OfficialArtifactStaleError,
    VerifiedCloseInput,
    _pdf_text,
    validate_official_artifact_semantics,
)


UTC = timezone.utc
NEW_YORK = ZoneInfo("America/New_York")
PUBLICATION_NOT_BEFORE_ET = time(18, 0)
CSV_FIELDS = (
    "symbol",
    "trading_date",
    "official_close",
    "source",
    "source_reference",
    "source_artifact_sha256",
    "source_artifact_path",
    "observed_at_utc",
    "correction_of_id",
)


@dataclass(frozen=True)
class OfficialCloseEndpoint:
    symbol: str
    source: str
    url: Callable[[date], str]


OFFICIAL_CLOSE_ENDPOINTS = (
    OfficialCloseEndpoint(
        "SPX",
        "cboe-official",
        lambda _day: (
            "https://cdn.cboe.com/api/global/us_indices/daily_prices/SPX_History.csv"
        ),
    ),
    OfficialCloseEndpoint(
        "RUT",
        "cboe-official",
        lambda _day: (
            "https://cdn.cboe.com/api/global/us_indices/daily_prices/RUT_History.csv"
        ),
    ),
    OfficialCloseEndpoint(
        "NDX",
        "nasdaq-official",
        lambda day: (
            "https://api.nasdaq.com/api/quote/NDX/historical?assetclass=index"
            f"&fromdate={day.isoformat()}"
            f"&todate={(day + timedelta(days=1)).isoformat()}&limit=10"
        ),
    ),
    OfficialCloseEndpoint(
        "VIX",
        "cboe-official",
        lambda _day: (
            "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
        ),
    ),
    OfficialCloseEndpoint(
        "SPY",
        "nyse-arca-official",
        lambda _day: "https://www.nyse.com/api/nyseservice/v1/quotes?symbol=SPY",
    ),
)


def _date_tokens(trading_day: date) -> tuple[str, ...]:
    return (
        trading_day.isoformat(),
        trading_day.strftime("%m/%d/%Y"),
        trading_day.strftime("%Y/%m/%d"),
        trading_day.strftime("%B %d, %Y"),
        trading_day.strftime("%b %d, %Y"),
        f"{trading_day.strftime('%B')} {trading_day.day}, {trading_day.year}",
        f"{trading_day.strftime('%b')} {trading_day.day}, {trading_day.year}",
        f"{trading_day.strftime('%B')} {trading_day.day} {trading_day.year}",
    )


def _positive_close(value: object, *, symbol: str) -> float:
    try:
        close = float(str(value).replace("$", "").replace(",", "").strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{symbol} official artifact has an invalid close") from exc
    if not math.isfinite(close) or close <= 0:
        raise ValueError(f"{symbol} official artifact has an invalid close")
    return close


def _extract_cboe_index_history(
    path: Path, trading_day: date, *, symbol: str
) -> float:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            expected_fields = ("DATE", symbol)
            actual_fields = tuple(
                str(field).strip().upper() for field in (reader.fieldnames or ())
            )
            if actual_fields != expected_fields:
                raise ValueError(
                    f"{symbol} Cboe history CSV must contain exactly DATE and {symbol} columns"
                )
            matches: list[float] = []
            for record in reader:
                try:
                    row_date = datetime.strptime(
                        str(record.get("DATE") or "").strip(), "%m/%d/%Y"
                    ).date()
                except ValueError:
                    continue
                if row_date == trading_day:
                    matches.append(_positive_close(record.get(symbol), symbol=symbol))
    except UnicodeDecodeError as exc:
        raise ValueError(f"{symbol} Cboe history artifact must contain UTF-8 CSV") from exc
    if not matches:
        raise OfficialArtifactStaleError(
            f"{symbol} Cboe history artifact does not contain the requested trading date"
        )
    if len(matches) != 1:
        raise ValueError(f"{symbol} Cboe history CSV contains duplicate requested-date rows")
    return matches[0]


def _extract_spx(path: Path, trading_day: date) -> float:
    if path.suffix.lower() == ".csv":
        return _extract_cboe_index_history(path, trading_day, symbol="SPX")
    try:
        raw = path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("SPX official artifact must contain UTF-8 HTML or text") from exc
    rendered = html.unescape(re.sub(r"<[^>]+>", " ", raw))
    rendered = re.sub(r"\s+", " ", rendered)
    if not any(token in rendered for token in _date_tokens(trading_day)):
        raise OfficialArtifactStaleError(
            "SPX official artifact does not contain the requested trading date"
        )
    match = re.search(
        r"(?:S(?:&amp;|&)\s*P\s*500|\bSPX\b).{0,4000}?"
        r"indices-price-value[^>]*>\s*([\d,]+(?:\.\d+)?)",
        raw,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match is None:
        match = re.search(
            r"(?:S\s*&\s*P\s*500|\bSPX\b)\D{0,200}"
            r"([\d,]+(?:\.\d+)?)",
            rendered,
            flags=re.IGNORECASE,
        )
    if match is None:
        raise ValueError("SPX official artifact has no structured S&P 500 price value")
    return _positive_close(match.group(1), symbol="SPX")


def _extract_ndx(path: Path, trading_day: date) -> float:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        rows = payload["data"]["tradesTable"]["rows"]
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise ValueError("NDX official artifact has no Nasdaq historical rows") from exc
    requested_date_seen = False
    for row in rows if isinstance(rows, list) else ():
        if not isinstance(row, dict):
            continue
        raw_date = str(row.get("date") or "").strip()
        try:
            row_date = datetime.strptime(raw_date, "%m/%d/%Y").date()
        except ValueError:
            continue
        if row_date == trading_day:
            requested_date_seen = True
            return _positive_close(row.get("close"), symbol="NDX")
    if not requested_date_seen:
        raise OfficialArtifactStaleError(
            "NDX official artifact does not contain the requested trading date"
        )
    raise ValueError("NDX official artifact has no close for the requested trading date")


def _extract_rut(path: Path, trading_day: date) -> float:
    if path.suffix.lower() == ".csv":
        return _extract_cboe_index_history(path, trading_day, symbol="RUT")
    rendered = _pdf_text(path)
    if not any(token in rendered for token in _date_tokens(trading_day)):
        raise OfficialArtifactStaleError(
            "RUT official artifact does not contain the requested trading date"
        )
    for line in rendered.splitlines():
        match = re.match(
            r"\s*Russell\s+2000(?:[^\w\s])?\s+Index\s+(.*)$",
            line,
            flags=re.IGNORECASE,
        )
        if match is None:
            continue
        values = re.findall(r"-?\d[\d,]*(?:\.\d+)?", match.group(1))
        if len(values) >= 4:
            return _positive_close(values[3], symbol="RUT")
    raise ValueError("RUT official artifact has no Russell 2000 close row")


def _extract_vix(path: Path, trading_day: date) -> float:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            fields = {str(field).strip().upper(): field for field in reader.fieldnames or ()}
            if "DATE" not in fields or "CLOSE" not in fields:
                raise ValueError("VIX official CSV must contain DATE and CLOSE columns")
            for row in reader:
                try:
                    row_date = datetime.strptime(
                        str(row.get(fields["DATE"]) or "").strip(), "%m/%d/%Y"
                    ).date()
                except ValueError:
                    continue
                if row_date == trading_day:
                    return _positive_close(row.get(fields["CLOSE"]), symbol="VIX")
    except UnicodeDecodeError as exc:
        raise ValueError("VIX official artifact must contain UTF-8 CSV") from exc
    raise OfficialArtifactStaleError(
        "VIX official artifact does not contain the requested trading date"
    )


def _extract_spy(path: Path, trading_day: date) -> float:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        history = payload["quoteHistory"]
        rows = history["historyList"]
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise ValueError("SPY official artifact has no NYSE quote history") from exc
    if str(history.get("symbol") or "").upper() != "SPY":
        raise ValueError("SPY official artifact does not identify SPY quote history")
    expected = trading_day.strftime("%Y/%m/%d")
    for row in rows if isinstance(rows, list) else ():
        if isinstance(row, dict) and str(row.get("date") or "") == expected:
            return _positive_close(row.get("close"), symbol="SPY")
    raise OfficialArtifactStaleError(
        "SPY official artifact does not contain the requested trading date"
    )


EXTRACTORS = {
    "SPX": _extract_spx,
    "NDX": _extract_ndx,
    "RUT": _extract_rut,
    "VIX": _extract_vix,
    "SPY": _extract_spy,
}


def extract_official_close(path: str | Path, *, symbol: str, trading_day: date) -> float:
    normalized = str(symbol).strip().upper()
    extractor = EXTRACTORS.get(normalized)
    if extractor is None:
        raise ValueError(f"unsupported official close family {normalized!r}")
    return extractor(Path(path), trading_day)


def _publication_cutoff_utc(trading_day: date) -> datetime:
    return datetime.combine(
        trading_day, PUBLICATION_NOT_BEFORE_ET, tzinfo=NEW_YORK
    ).astimezone(UTC)


def _serialize_candidate_rows(rows: list[dict[str, object]]) -> str:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=CSV_FIELDS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue()


def fetch_verified_close_bundle(
    *,
    project_root: str | Path,
    trading_day: date,
    now_utc: datetime | None = None,
    allow_early_fetch: bool = False,
    allow_output_replace: bool = False,
) -> dict[str, object]:
    """Capture and audit one five-family official-close bundle without ingesting it."""
    root = Path(project_root).resolve()
    observed_now = now_utc or datetime.now(UTC)
    if observed_now.tzinfo is None:
        raise ValueError("now_utc must be timezone-aware")
    observed_now = observed_now.astimezone(UTC)
    cutoff = _publication_cutoff_utc(trading_day)
    if observed_now < cutoff and not allow_early_fetch:
        raise RuntimeError(
            "official-close bundle fetch is blocked before "
            f"{cutoff.isoformat()} (18:00 America/New_York)"
        )

    candidate_dir = root / "data" / "verified_close_sources" / trading_day.isoformat()
    destination = candidate_dir / "candidate_bundle.csv"
    if destination.exists() and not allow_output_replace:
        raise FileExistsError(
            "a candidate bundle already exists; explicit replacement is required"
        )
    candidate_rows: list[dict[str, object]] = []
    fetched: list[dict[str, object]] = []
    for endpoint in OFFICIAL_CLOSE_ENDPOINTS:
        result = fetch_official_artifact(
            symbol=endpoint.symbol,
            source=endpoint.source,
            source_reference=endpoint.url(trading_day),
            trading_date=trading_day,
            project_root=root,
        )
        artifact_path = Path(str(result["source_artifact_path"])).resolve()
        close = extract_official_close(
            artifact_path, symbol=endpoint.symbol, trading_day=trading_day
        )
        observed_at = datetime.fromisoformat(
            str(result["retrieved_at_utc"]).replace("Z", "+00:00")
        )
        row = VerifiedCloseInput(
            symbol=endpoint.symbol,
            trading_date=trading_day,
            official_close=close,
            source=endpoint.source,
            source_reference=str(result["source_reference"]),
            source_artifact_sha256=str(result["source_artifact_sha256"]),
            source_artifact_path=artifact_path,
            observed_at_utc=observed_at,
            correction_of_id=None,
        )
        validate_official_artifact_semantics(row)
        relative_artifact = artifact_path.relative_to(candidate_dir.resolve())
        candidate_rows.append(
            {
                "symbol": row.symbol,
                "trading_date": row.trading_date.isoformat(),
                "official_close": format(row.official_close, ".15g"),
                "source": row.source,
                "source_reference": row.source_reference,
                "source_artifact_sha256": row.source_artifact_sha256,
                "source_artifact_path": relative_artifact.as_posix(),
                "observed_at_utc": row.observed_at_utc.astimezone(UTC).isoformat(),
                "correction_of_id": "",
            }
        )
        fetched.append(result)

    candidate_dir.mkdir(parents=True, exist_ok=True)
    serialized = _serialize_candidate_rows(candidate_rows)
    if destination.exists():
        current = destination.read_text(encoding="utf-8")
        if current != serialized and not allow_output_replace:
            raise FileExistsError(
                "a different candidate bundle exists; explicit replacement is required"
            )
        if current != serialized:
            temporary = destination.with_name(
                f".{destination.name}.{uuid.uuid4().hex}.tmp"
            )
            try:
                with temporary.open("x", encoding="utf-8", newline="") as stream:
                    stream.write(serialized)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, destination)
            finally:
                if temporary.exists():
                    temporary.unlink()
    else:
        temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
        try:
            with temporary.open("x", encoding="utf-8", newline="") as stream:
                stream.write(serialized)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, destination)
        finally:
            if temporary.exists():
                temporary.unlink()

    audit = audit_close_bundle_csv(
        destination, trading_date=trading_day, bundle_profile="full"
    )
    if not audit["ready_for_ingestion"]:
        raise ValueError("fetched official-close bundle failed semantic audit")
    return {
        "trading_date": trading_day.isoformat(),
        "candidate_csv": str(destination.resolve()),
        "publication_cutoff_utc": cutoff.isoformat(),
        "fetched_artifacts": fetched,
        "audit": audit,
        "database_ingested": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch and audit one five-family official-close bundle without database ingestion"
        )
    )
    parser.add_argument("--trading-date", required=True)
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    parser.add_argument("--allow-early-fetch", action="store_true")
    parser.add_argument("--allow-output-replace", action="store_true")
    args = parser.parse_args(argv)
    try:
        payload = fetch_verified_close_bundle(
            project_root=args.project_root,
            trading_day=date.fromisoformat(args.trading_date),
            allow_early_fetch=args.allow_early_fetch,
            allow_output_replace=args.allow_output_replace,
        )
    except (FileExistsError, RuntimeError) as exc:
        parser.error(str(exc))
    print(json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
