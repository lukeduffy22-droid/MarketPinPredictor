from __future__ import annotations

import hashlib
import sqlite3
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from .dataset import attach_point_in_time_reference_prices, load_marketpin_reference_prices
from .integrity import TapeIntegrityReport, inspect_dbn
from .governance import finish_forecast_attempt, start_forecast_attempt
from .oi_replay import replay_open_interest_state_from_prefix
from .paper_shadow import PaperShadowResult, record_paper_shadow_predictions
from .paper_evidence import LIVE_PREFIX_RECEIPT_CONTRACT_VERSION
from .production import (
    load_promoted_model_runtime,
    predict_promoted_close,
    record_promoted_close_predictions,
)
from .parity import estimate_tcbbo_parity_reference_prices
from .replay import build_minute_rows
from .surface import build_contract_surface_features


CONTRACT_KEYS = [
    "session_id", "feed_name", "family_root", "raw_symbol", "expiration",
    "option_type", "strike", "minute_utc",
]


def _validate_live_prefix_integrity(
    report: TapeIntegrityReport,
    *,
    expected_sha256: str,
    expected_subscription_acks: int | None = None,
    event_cutoff_utc: datetime | None = None,
) -> None:
    """Require research-grade TCBBO evidence before deriving live features."""
    reasons: list[str] = []
    if report.sha256.lower() != expected_sha256.lower():
        reasons.append("integrity SHA-256 does not match the cutoff ledger")
    if not report.local_file_intact:
        reasons.append("local DBN framing/provider-message integrity failed")
    if report.tcbbo_records <= 0:
        reasons.append("no TCBBO records were decoded")
    elif report.tcbbo_timestamped_records != report.tcbbo_records:
        reasons.append("not every TCBBO record has valid event and receive timestamps")
    if report.tcbbo_records > 0:
        nbbo_ratio = report.tcbbo_valid_nbbo_records / report.tcbbo_records
        if nbbo_ratio < 0.95:
            reasons.append(f"valid pre-trade NBBO coverage is {nbbo_ratio:.3%}, below 95%")
    if report.provider_errors:
        reasons.append(f"provider errors present: {len(report.provider_errors)}")
    if report.slow_reader_warnings:
        reasons.append(f"slow-reader warnings present: {report.slow_reader_warnings}")
    if expected_subscription_acks is not None:
        if expected_subscription_acks < 1:
            reasons.append("expected subscription identity is invalid")
        elif int(report.subscription_acks) != expected_subscription_acks:
            reasons.append(
                "subscription acknowledgements do not match the cutoff catalog"
            )
        if int(report.replay_completed) != expected_subscription_acks:
            reasons.append("subscription replay was incomplete at the analysis cutoff")
    if event_cutoff_utc is not None:
        if event_cutoff_utc.tzinfo is None:
            reasons.append("analysis event cutoff is timezone-naive")
        else:
            last_trade_event_ns = int(report.last_trade_event_ns or 0)
            cutoff_ns = int(event_cutoff_utc.timestamp() * 1_000_000_000)
            if last_trade_event_ns > cutoff_ns:
                reasons.append("decoded prefix contains a post-horizon trade event")
    if reasons:
        raise ValueError("live analysis prefix failed integrity gates: " + "; ".join(reasons))


def copy_verified_prefix(
    source: str | Path,
    destination: str | Path,
    *,
    cutoff_bytes: int,
    expected_sha256: str,
) -> Path:
    if cutoff_bytes <= 0:
        raise ValueError("prefix cutoff must be positive")
    source_path = Path(source)
    destination_path = Path(destination)
    digest = hashlib.sha256()
    remaining = cutoff_bytes
    with source_path.open("rb") as reader, destination_path.open("xb") as writer:
        while remaining:
            block = reader.read(min(4 * 1024 * 1024, remaining))
            if not block:
                raise OSError("raw DBN ended before the recorded analysis cutoff")
            writer.write(block)
            digest.update(block)
            remaining -= len(block)
    if digest.hexdigest() != expected_sha256.lower():
        destination_path.unlink(missing_ok=True)
        raise ValueError("copied analysis prefix SHA-256 does not match the cutoff ledger")
    return destination_path


def _live_open_interest(
    catalog_path: Path,
    session_id: str,
    feed_name: str,
    *,
    available_before_utc: datetime,
) -> pd.DataFrame:
    with sqlite3.connect(f"file:{catalog_path.resolve()}?mode=ro", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(tape_open_interest)")
        }
        if "available_at_utc" not in columns:
            return pd.DataFrame()
        rows = connection.execute(
            """
            SELECT raw_symbol, open_interest,
                   asof_utc AS open_interest_asof_utc,
                   available_at_utc AS open_interest_available_at_utc
            FROM tape_open_interest
            WHERE session_id=? AND feed_name=? AND raw_symbol IS NOT NULL
              AND available_at_utc IS NOT NULL
              AND available_at_utc<=?
            """,
            (session_id, feed_name, pd.Timestamp(available_before_utc).isoformat()),
        ).fetchall()
    return pd.DataFrame([dict(row) for row in rows])


def _replay_open_interest(
    prefix_path: Path,
    *,
    available_before_utc: datetime,
) -> pd.DataFrame:
    """Reconstruct horizon OI from the exact copied bytes and in-prefix definitions."""

    rows = replay_open_interest_state_from_prefix(
        prefix_path, available_before_utc=available_before_utc
    )
    return pd.DataFrame(
        rows,
        columns=[
            "raw_symbol", "open_interest", "open_interest_asof_utc",
            "open_interest_available_at_utc",
        ],
    )


def build_surface_from_live_prefix(
    prefix_path: str | Path,
    *,
    prefix_sha256: str,
    catalog_path: str | Path,
    market_db_path: str | Path,
    session_id: str,
    feed_name: str,
    trading_day: date,
    cash_open_utc: datetime,
    cash_close_utc: datetime,
    feature_available_at_utc: datetime,
    derive_open_interest_from_prefix: bool = False,
    expected_subscription_acks: int | None = None,
    verified_integrity_report: TapeIntegrityReport | None = None,
) -> pd.DataFrame:
    integrity = verified_integrity_report or inspect_dbn(
        prefix_path, require_tcbbo=True,
        expected_subscription_acks=expected_subscription_acks or 1,
    )
    _validate_live_prefix_integrity(
        integrity,
        expected_sha256=prefix_sha256,
        expected_subscription_acks=expected_subscription_acks,
        event_cutoff_utc=feature_available_at_utc,
    )
    (
        _observed, _inferred, contract_observed, contract_inferred, _feature_hash,
    ) = build_minute_rows(
        prefix_path, session_id=session_id, feed_name=feed_name,
        verified_source_sha256=prefix_sha256,
        verified_integrity_report=integrity,
        include_contract_rows=True,
    )
    observed = pd.DataFrame(contract_observed)
    inferred = pd.DataFrame(contract_inferred)
    if observed.empty or inferred.empty:
        return pd.DataFrame()
    contracts = observed.merge(inferred, on=CONTRACT_KEYS, how="inner", validate="one_to_one")
    target_minute = pd.Timestamp(feature_available_at_utc).tz_convert("UTC") - pd.Timedelta(minutes=1)
    contracts["minute_utc"] = pd.to_datetime(contracts["minute_utc"], utc=True)
    contracts = contracts[contracts["minute_utc"] == target_minute].copy()
    if contracts.empty:
        return pd.DataFrame()
    if derive_open_interest_from_prefix:
        oi = _replay_open_interest(
            Path(prefix_path),
            available_before_utc=feature_available_at_utc,
        )
    else:
        oi = _live_open_interest(
            Path(catalog_path), session_id, feed_name,
            available_before_utc=feature_available_at_utc,
        )
    if oi.empty:
        contracts["open_interest"] = float("nan")
        contracts["open_interest_asof_utc"] = pd.NaT
        contracts["open_interest_available_at_utc"] = pd.NaT
    else:
        contracts = contracts.merge(oi, on="raw_symbol", how="left", validate="many_to_one")
    contracts["trading_date"] = trading_day.isoformat()
    contracts["cash_open_utc"] = cash_open_utc
    contracts["cash_close_utc"] = cash_close_utc
    contracts["capture_integrity_verified"] = True
    prices = load_marketpin_reference_prices(market_db_path)
    parity = estimate_tcbbo_parity_reference_prices(contracts)
    joined = attach_point_in_time_reference_prices(contracts, prices, parity)
    return build_contract_surface_features(joined) if not joined.empty else pd.DataFrame()


def run_live_prefix_paper_shadow(
    *,
    project_root: str | Path,
    source_path: str | Path,
    cutoff_bytes: int,
    prefix_sha256: str,
    catalog_path: str | Path,
    market_db_path: str | Path,
    session_id: str,
    feed_name: str,
    trading_day: date,
    cash_open_utc: datetime,
    cash_close_utc: datetime,
    feature_available_at_utc: datetime,
) -> PaperShadowResult:
    root = Path(project_root).resolve()
    paper_configured = (root / "models" / "closing_tape_paper_candidate.json").is_file()
    production_configured = (root / "models" / "closing_tape_model.json").is_file()
    if not paper_configured and not production_configured:
        return PaperShadowResult(False, 0, None, None, (), ("no paper candidate descriptor",))
    temporary_path: Path | None = None
    production_attempt_key: str | None = None
    production_terminal = False
    production_reasons: tuple[str, ...] = ()
    if production_configured:
        try:
            production_attempt_key = start_forecast_attempt(
                market_db_path,
                trading_day=trading_day,
                session_id=session_id,
                prediction_mode="tcbbo_promoted",
                decision_horizon_minutes=15,
                feature_available_at_utc=feature_available_at_utc,
                source_sha256=prefix_sha256,
            )
        except Exception as exc:
            production_reasons = (
                f"GOVERNANCE_ATTEMPT_START_FAILED:{type(exc).__name__}: "
                f"{str(exc)[:400]}",
            )
    try:
        handle, raw_name = tempfile.mkstemp(
            prefix="closing-tape-analysis-", suffix=".dbn", dir=Path(source_path).parent
        )
        __import__("os").close(handle)
        temporary_path = Path(raw_name)
        temporary_path.unlink()
        copy_verified_prefix(
            source_path, temporary_path, cutoff_bytes=cutoff_bytes,
            expected_sha256=prefix_sha256,
        )
        surface = build_surface_from_live_prefix(
            temporary_path, prefix_sha256=prefix_sha256,
            catalog_path=catalog_path, market_db_path=market_db_path,
            session_id=session_id, feed_name=feed_name, trading_day=trading_day,
            cash_open_utc=cash_open_utc, cash_close_utc=cash_close_utc,
            feature_available_at_utc=feature_available_at_utc,
            derive_open_interest_from_prefix=paper_configured,
        )
        now = datetime.now(feature_available_at_utc.tzinfo)
        live_prefix_receipt = {
            "contract_version": LIVE_PREFIX_RECEIPT_CONTRACT_VERSION,
            "prefix_sha256": prefix_sha256.lower(),
            "cutoff_bytes": int(cutoff_bytes),
            "session_id": session_id,
            "feed_name": feed_name,
            "horizon_id": "cash-close-minus-15m-v1",
            "trading_date": trading_day.isoformat(),
            "feature_available_at_utc": feature_available_at_utc.astimezone(
                timezone.utc
            ).isoformat(),
            "catalog_path": str(Path(catalog_path).resolve()),
            "source_path": str(Path(source_path).resolve()),
        }
        paper_result = (
            record_paper_shadow_predictions(
                surface, project_root=root, market_db_path=market_db_path,
                trading_day=trading_day, session_id=session_id,
                live_prefix_receipt=live_prefix_receipt,
            )
            if paper_configured
            else PaperShadowResult(False, 0, None, None, (), ("no paper candidate descriptor",))
        )
        production_keys: tuple[str, ...] = ()
        if production_configured and production_attempt_key is not None:
            try:
                runtime = load_promoted_model_runtime(root)
                predictions = predict_promoted_close(surface, runtime)
                production_keys = record_promoted_close_predictions(
                    market_db_path, predictions, runtime=runtime,
                    trading_day=trading_day,
                    session_id=session_id, recorded_at_utc=now,
                )
                finish_forecast_attempt(
                    market_db_path,
                    production_attempt_key,
                    event_type="PREDICTED",
                    prediction_keys=production_keys,
                    recorded_at_utc=now,
                    prediction_batch=predictions,
                )
                production_terminal = True
            except Exception as exc:
                production_reasons = (f"{type(exc).__name__}: {str(exc)[:500]}",)
                try:
                    finish_forecast_attempt(
                        market_db_path,
                        production_attempt_key,
                        event_type="ABSTAIN",
                        reasons=production_reasons,
                    )
                    production_terminal = True
                except Exception as terminal_exc:
                    production_reasons = (
                        *production_reasons,
                        f"GOVERNANCE_TERMINAL_WRITE_FAILED:{type(terminal_exc).__name__}: "
                        f"{str(terminal_exc)[:400]}",
                    )
                production_keys = ()
        return PaperShadowResult(
            configured=paper_result.configured,
            recorded=paper_result.recorded,
            model_version=paper_result.model_version,
            artifact_sha256=paper_result.artifact_sha256,
            forecast_keys=paper_result.forecast_keys,
            reasons=paper_result.reasons,
            production_recorded=len(production_keys),
            production_keys=production_keys,
            production_reasons=production_reasons,
        )
    except Exception as exc:
        if production_attempt_key is not None and not production_terminal:
            try:
                finish_forecast_attempt(
                    market_db_path,
                    production_attempt_key,
                    event_type="ABSTAIN",
                    reasons=(f"PREFIX_REPLAY_FAILED:{type(exc).__name__}: {str(exc)[:400]}",),
                )
            except Exception:
                # Preserve the original replay failure; the unresolved start
                # remains visible in the denominator for operator repair.
                pass
        raise
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
