"""Freeze, collect, ingest, and score verified closes through durable receipts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.database_target import configured_market_database_path
from backend.postclose_scoring import (
    load_prediction_score_selection,
    prepare_prediction_score_selection,
)

UTC = timezone.utc
RESULT_SCHEMA = "marketpin-verified-close-postclose-result.v1"


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        + "\n"
    ).encode("utf-8")


def _result_directory(root: Path, trading_day: date) -> Path:
    return root / "data" / "postclose_pipeline" / trading_day.isoformat()


def _append_result(root: Path, trading_day: date, payload: dict[str, Any]) -> dict[str, str]:
    directory = _result_directory(root, trading_day)
    directory.mkdir(parents=True, exist_ok=True)
    rendered = _canonical_bytes(payload)
    digest = hashlib.sha256(rendered).hexdigest()
    path = directory / f"{digest}.json"
    if path.exists():
        if path.read_bytes() != rendered:
            raise RuntimeError("post-close result receipt hash collision")
        return {"result_path": str(path), "result_sha256": digest}
    temporary = directory / f".{digest}.{uuid.uuid4().hex}.tmp"
    try:
        with temporary.open("xb") as stream:
            stream.write(rendered)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()
    return {"result_path": str(path), "result_sha256": digest}


def run_settlement(
    project_root: str | Path,
    trading_day: date,
    *,
    observed_at_utc: datetime | None = None,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    database_path = configured_market_database_path(root, require_canonical=True)
    selection = load_prediction_score_selection(root, trading_day)
    selection_receipt = selection["receipt"]

    from backend.closing_tape.config import _market_times
    from tools.audit_verified_close_bundle import audit_close_bundle_csv
    from tools.fetch_verified_close_bundle import (
        _publication_cutoff_utc,
        fetch_verified_close_bundle,
    )
    from tools.ingest_verified_closes import ingest, read_verified_close_csv

    observed = (observed_at_utc or datetime.now(UTC)).astimezone(UTC)
    cutoff = _publication_cutoff_utc(trading_day)
    if observed < cutoff:
        raise RuntimeError(
            "verified-close settlement is blocked until the official publication cutoff"
        )
    selected_at = datetime.fromisoformat(
        str(selection_receipt["selected_at_utc"]).replace("Z", "+00:00")
    ).astimezone(UTC)
    _open, _analysis, cash_close, _stop = _market_times(trading_day)
    if selected_at < cash_close or selected_at >= cutoff:
        raise RuntimeError(
            "prediction-selection receipt must be created after cash close and before "
            "official close publication"
        )

    candidate = (
        root
        / "data"
        / "verified_close_sources"
        / trading_day.isoformat()
        / "candidate_bundle.csv"
    )
    if candidate.exists():
        audit = audit_close_bundle_csv(
            candidate, trading_date=trading_day, bundle_profile="full"
        )
        if not audit["ready_for_ingestion"]:
            raise RuntimeError("retained official-close candidate bundle is not ready")
        fetch_result: dict[str, Any] = {
            "trading_date": trading_day.isoformat(),
            "candidate_csv": str(candidate.resolve()),
            "publication_cutoff_utc": cutoff.isoformat(),
            "audit": audit,
            "database_ingested": False,
            "reused_retained_candidate": True,
        }
    else:
        fetch_result = fetch_verified_close_bundle(
            project_root=root,
            trading_day=trading_day,
            now_utc=observed,
        )

    selections = {
        str(item["symbol"]): [int(item["prediction_id"])]
        for item in selection_receipt.get("selections", [])
    }
    selection_hashes = {
        symbol: str(selection["receipt_sha256"]) for symbol in selections
    }
    ingest_results = ingest(
        read_verified_close_csv(fetch_result["candidate_csv"]),
        project_root=root,
        bundle_profile="full",
        score_prediction_ids_by_symbol=selections,
        selection_evidence_sha256_by_symbol=selection_hashes,
        # The running app already owns schema initialization. Catch-up during a
        # session must not run global DDL or reconcile unrelated ORB decisions.
        initialize_schema=False,
    )
    payload: dict[str, Any] = {
        "schema_version": RESULT_SCHEMA,
        "trading_date": trading_day.isoformat(),
        "started_at_utc": observed.isoformat(),
        "completed_at_utc": datetime.now(UTC).isoformat(),
        "database_path": str(database_path),
        "selection_receipt_path": selection["receipt_path"],
        "selection_receipt_sha256": selection["receipt_sha256"],
        "candidate_csv": str(Path(fetch_result["candidate_csv"]).resolve()),
        "bundle_audit": fetch_result["audit"],
        "ingest_results": ingest_results,
        "research_only": True,
        "training_eligible": False,
        "performance_claim_eligible": False,
    }
    payload.update(_append_result(root, trading_day, payload))
    return payload


def latest_pipeline_status(project_root: str | Path, trading_day: date) -> dict[str, Any]:
    root = Path(project_root).resolve()
    try:
        selection = load_prediction_score_selection(root, trading_day)
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        selection_status: dict[str, Any] = {"status": "MISSING_OR_INVALID", "detail": str(exc)}
    else:
        selection_status = {
            "status": "RECEIPT_VALID",
            "path": selection["receipt_path"],
            "sha256": selection["receipt_sha256"],
            "symbols": [
                {"symbol": row["symbol"], "status": row["status"], "reason": row["reason"]}
                for row in selection["receipt"].get("statuses", [])
            ],
        }
    result_dir = _result_directory(root, trading_day)
    result_paths = (
        sorted(
            result_dir.glob("*.json"),
            key=lambda item: item.stat().st_mtime_ns,
            reverse=True,
        )
        if result_dir.is_dir()
        else []
    )
    latest_result = None
    if result_paths:
        path = result_paths[0]
        try:
            raw = path.read_bytes()
            digest = hashlib.sha256(raw).hexdigest()
            payload = json.loads(raw.decode("utf-8"))
            if path.stem != digest or payload.get("schema_version") != RESULT_SCHEMA:
                raise ValueError("result receipt identity mismatch")
            latest_result = {
                "status": "COMPLETE",
                "path": str(path),
                "sha256": digest,
                "completed_at_utc": payload["completed_at_utc"],
                "ingest_results": payload["ingest_results"],
            }
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
            latest_result = {"status": "INVALID", "path": str(path), "detail": str(exc)}
    return {
        "schema_version": "marketpin-verified-close-postclose-status.v1",
        "trading_date": trading_day.isoformat(),
        "selection": selection_status,
        "settlement": latest_result or {"status": "PENDING"},
        "dji_close_scoring": {
            "status": "UNSUPPORTED",
            "reason": "no approved DJI official-close adapter is configured",
        },
    }


def run_pending_settlement(project_root, anchor_day, *, observed_at_utc=None):
    """Retry at most one frozen, published-date selection from the last seven days.

    No selection is created after the fact. Prefer the newest pending date so
    an older missing source cannot starve this evening's settlement. Completed
    content-addressed receipts are skipped only when bound to that selection.
    """
    from tools.fetch_verified_close_bundle import _publication_cutoff_utc
    root = Path(project_root).resolve()
    observed = observed_at_utc or datetime.now(UTC)
    checked = []
    for offset in range(7):
        day = anchor_day - timedelta(days=offset)
        if observed < _publication_cutoff_utc(day):
            continue
        directory = root / 'data' / 'prediction_score_selections' / day.isoformat()
        if not directory.is_dir():
            continue
        selection = load_prediction_score_selection(root, day)
        status = latest_pipeline_status(root, day)['settlement']
        if status['status'] == 'INVALID':
            raise RuntimeError(f'Invalid existing settlement receipt for {day}')
        if status['status'] == 'COMPLETE':
            payload = json.loads(Path(status['path']).read_text(encoding='utf-8'))
            if (payload.get('trading_date') != day.isoformat()
                    or payload.get('selection_receipt_sha256') != selection['receipt_sha256']):
                raise RuntimeError(f'Settlement selection binding mismatch for {day}')
            checked.append(day.isoformat())
            continue
        return run_settlement(root, day, observed_at_utc=observed)
    return {'status': 'NO_PENDING_PUBLISHED_SELECTION', 'lookback_days': 7,
            'completed_dates_skipped': checked, 'observed_at_utc': observed.isoformat()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("select", "settle", "status", "catchup"))
    parser.add_argument("--trading-date", required=True)
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    args = parser.parse_args(argv)
    trading_day = date.fromisoformat(args.trading_date)
    if args.mode == "status":
        result = latest_pipeline_status(args.project_root, trading_day)
    else:
        from backend.postclose_run_guard import PostcloseAlreadyRunning, postclose_run_guard
        try:
            with postclose_run_guard(args.project_root, args.mode):
                result = run_with_attempt_receipt(args.project_root, trading_day, args.mode)
        except PostcloseAlreadyRunning:
            result = {"status": "BUSY", "reason": "POSTCLOSE_OPERATION_ALREADY_RUNNING", "mode": args.mode,
                      "trading_date": trading_day.isoformat(), "observed_at_utc": datetime.now(UTC).isoformat()}
    print(json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False))
    return 0


def run_with_attempt_receipt(project_root, trading_day, mode):
    """Record actual start/completion and failures without rewriting outcomes."""
    started = datetime.now(UTC)
    attempt = {"schema_version": "marketpin-postclose-attempt.v1", "mode": mode,
               "requested_trading_date": trading_day.isoformat(), "started_at_utc": started.isoformat()}
    try:
        if mode == "select":
            result = prepare_prediction_score_selection(project_root, trading_day)
        elif mode == "settle":
            result = run_settlement(project_root, trading_day)
        elif mode == "catchup":
            result = run_pending_settlement(project_root, trading_day)
        else:
            raise ValueError("unsupported post-close mutation")
        attempt.update(status="SUCCEEDED", settled_trading_date=result.get("trading_date"),
                       result_path=result.get("result_path"), result_status=result.get("status"))
        return result
    except Exception as exc:
        attempt.update(status="FAILED", error_type=type(exc).__name__, detail=str(exc)[:2000])
        raise
    finally:
        attempt["completed_at_utc"] = datetime.now(UTC).isoformat()
        rendered = _canonical_bytes(attempt)
        digest = hashlib.sha256(rendered).hexdigest()
        directory = Path(project_root).resolve() / "data" / "postclose_attempts" / trading_day.isoformat()
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / (digest + ".json")
        with path.open("xb") as handle:
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())


if __name__ == "__main__":
    raise SystemExit(main())
