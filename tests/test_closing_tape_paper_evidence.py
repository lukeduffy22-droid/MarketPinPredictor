import hashlib
import json
import shutil
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.closing_tape import paper_evidence
from backend.closing_tape.catalog import TapeCatalog
from backend.closing_tape.contracts import EVIDENCE_CONTRACT_VERSION, LIVE_SOURCE_KIND
from backend.closing_tape.dataset import PRODUCTION_FAMILIES
from backend.closing_tape.paper_evidence import (
    LIVE_PREFIX_RECEIPT_CONTRACT_VERSION,
    audit_paper_campaign_coverage,
    build_paper_feature_payload,
    parse_paper_feature_evidence,
    replay_verify_paper_feature_groups,
)
from backend.closing_tape.surface import (
    MODEL_FEATURE_COLUMNS,
    MODEL_FEATURE_CONTRACT_HASH,
    SURFACE_FEATURE_VERSION,
)


UTC = timezone.utc
FAMILIES = tuple(sorted(PRODUCTION_FAMILIES))


def _catalog_fixture(root: Path, trading_day: date, *, session_id: str):
    day_dir = root / "data" / "closing_tape" / trading_day.isoformat()
    day_dir.mkdir(parents=True)
    source = day_dir / "opra_options.dbn"
    prefix = (f"retained-prefix-{trading_day.isoformat()}".encode("ascii")) * 4
    source.write_bytes(prefix + b"-finalized-tail")
    prefix_hash = hashlib.sha256(prefix).hexdigest()
    final_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    cash_open = datetime.combine(
        trading_day, datetime.min.time(), tzinfo=UTC
    ) + timedelta(hours=13, minutes=30)
    feature_at = datetime.combine(
        trading_day, datetime.min.time(), tzinfo=UTC
    ) + timedelta(hours=19, minutes=45)
    cash_close = feature_at + timedelta(minutes=15)
    captured_at = feature_at + timedelta(seconds=1)
    finalized_at = cash_close + timedelta(minutes=1)
    last_trade_event_ns = int(feature_at.timestamp() * 1_000_000_000) - 1_000
    catalog = TapeCatalog(day_dir / "closing_tape.sqlite")
    with catalog.connect() as connection:
        connection.execute(
            """
            INSERT INTO tape_sessions (
                session_id, trading_date, created_at_utc, cash_open_utc,
                analysis_due_utc, cash_close_utc, stop_due_utc, status,
                config_json, completed_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'complete', '{}', ?)
            """,
            (
                session_id,
                trading_day.isoformat(),
                cash_open.isoformat(),
                cash_open.isoformat(),
                feature_at.isoformat(),
                cash_close.isoformat(),
                (cash_close + timedelta(minutes=5)).isoformat(),
                finalized_at.isoformat(),
            ),
        )
        connection.execute(
            """
            INSERT INTO tape_feed_status (
                session_id, feed_name, dataset, schemas_json, symbols_json,
                dbn_path, source_kind, evidence_contract_version,
                operational_counters_applicable, source_components_json,
                status, expected_subscription_acks, subscription_acks,
                replay_completed, reconnect_count, slow_reader_warnings,
                provider_error_count, unmapped_trade_records, complete,
                file_bytes, last_trade_event_ns, sha256
            ) VALUES (
                ?, 'opra_options', 'OPRA.PILLAR', ?, '[]', ?, ?, ?, 1,
                '{}', 'complete', 1, 1, 1, 0, 0, 0, 0, 1, ?, ?, ?
            )
            """,
            (
                session_id,
                json.dumps(["tcbbo", "definition", "statistics"]),
                str(source.resolve()),
                LIVE_SOURCE_KIND,
                EVIDENCE_CONTRACT_VERSION,
                source.stat().st_size,
                last_trade_event_ns,
                final_hash,
            ),
        )
        connection.execute(
            """
            INSERT INTO tape_analysis_cutoffs (
                session_id, feed_name, horizon_id, captured_at_utc,
                event_cutoff_utc, cutoff_bytes, record_sequence,
                processed_sequence, prefix_sha256, last_trade_event_ns,
                final_file_sha256, finalized_at_utc
            ) VALUES (
                ?, 'opra_options', 'cash-close-minus-15m-v1', ?, ?, ?,
                17, 17, ?, ?, ?, ?
            )
            """,
            (
                session_id,
                captured_at.isoformat(),
                feature_at.isoformat(),
                len(prefix),
                prefix_hash,
                last_trade_event_ns,
                final_hash,
                finalized_at.isoformat(),
            ),
        )
    return {
        "catalog": catalog.path.resolve(),
        "source": source.resolve(),
        "prefix": prefix,
        "prefix_sha256": prefix_hash,
        "final_sha256": final_hash,
        "session_id": session_id,
        "trading_day": trading_day,
        "cash_open": cash_open,
        "feature_at": feature_at,
        "cash_close": cash_close,
        "captured_at": captured_at,
        "last_trade_event_ns": last_trade_event_ns,
    }


def _features(family: str) -> dict[str, float]:
    values = {column: 0.0 for column in MODEL_FEATURE_COLUMNS}
    values["minutes_to_cash_close"] = 15.0
    for root in PRODUCTION_FAMILIES:
        values[f"family_is_{root.lower()}"] = float(root == family)
    return values


def _payload_bundle(entry):
    receipt = {
        "contract_version": LIVE_PREFIX_RECEIPT_CONTRACT_VERSION,
        "prefix_sha256": entry["prefix_sha256"],
        "cutoff_bytes": len(entry["prefix"]),
        "session_id": entry["session_id"],
        "feed_name": "opra_options",
        "horizon_id": "cash-close-minus-15m-v1",
        "trading_date": entry["trading_day"].isoformat(),
        "feature_available_at_utc": entry["feature_at"].isoformat(),
        "catalog_path": str(entry["catalog"]),
        "source_path": str(entry["source"]),
    }
    payloads = []
    rows = []
    recorded = {}
    for index, family in enumerate(FAMILIES):
        reference = 100.0 + index
        identity = {
            "forecast_key": (
                f"{entry['trading_day'].isoformat()}:{entry['session_id']}:{family}"
            ),
            "model_version": "candidate-1",
            "artifact_sha256": "a" * 64,
            "source_sha256": entry["prefix_sha256"],
            "session_id": entry["session_id"],
            "family_root": family,
            "trading_date": entry["trading_day"].isoformat(),
            "decision_horizon_minutes": 15,
            "feature_available_at_utc": entry["feature_at"].isoformat(),
            "reference_price": reference,
            "incumbent_predicted_close": reference + 2.0,
            "feature_contract_hash": MODEL_FEATURE_CONTRACT_HASH,
        }
        serialized, payload_hash = build_paper_feature_payload(
            _features(family),
            forecast_identity=identity,
            live_prefix_receipt=receipt,
        )
        parsed = parse_paper_feature_evidence(
            serialized,
            expected_sha256=payload_hash,
            expected_forecast_identity=identity,
        )
        recorded_at = (entry["captured_at"] + timedelta(seconds=1)).isoformat()
        payloads.append(parsed)
        recorded[payload_hash] = recorded_at
        rows.append(
            {
                **identity,
                "feature_payload_json": serialized,
                "feature_payload_sha256": payload_hash,
                "recorded_at_utc": recorded_at,
            }
        )
    return payloads, rows, recorded


def _surface(entry, *, mutate_feature: bool = False) -> pd.DataFrame:
    rows = []
    for index, family in enumerate(FAMILIES):
        values = _features(family)
        if mutate_feature and family == "SPX":
            values["observed_log1p_volume"] = 1.0
        rows.append(
            {
                **values,
                "session_id": entry["session_id"],
                "feed_name": "opra_options",
                "family_root": family,
                "trading_date": entry["trading_day"].isoformat(),
                "feature_available_at_utc": entry["feature_at"],
                "cash_close_utc": entry["cash_close"],
                "surface_feature_version": SURFACE_FEATURE_VERSION,
                "feature_schema_hash": MODEL_FEATURE_CONTRACT_HASH,
                "source_sha256": entry["prefix_sha256"],
                "capture_integrity_verified": True,
                "reference_price": 100.0 + index,
                "predicted_close": 102.0 + index,
            }
        )
    return pd.DataFrame(rows)


def _patch_catalog_verifier(monkeypatch, entries):
    by_catalog = {entry["catalog"]: entry for entry in entries}

    def verify(catalog_path, *, source_sha256, **_kwargs):
        entry = by_catalog[Path(catalog_path).resolve()]
        raw = entry["source"].read_bytes()
        actual = hashlib.sha256(raw).hexdigest()
        if actual != source_sha256:
            raise ValueError("retained finalized DBN SHA-256 changed")
        return {
            "source_path": str(entry["source"]),
            "source_sha256": actual,
            "source_bytes": len(raw),
        }

    monkeypatch.setattr(
        paper_evidence, "verify_retained_live_catalog_source", verify
    )


def _patch_replay(monkeypatch, entries, *, mutate_feature=False):
    _patch_catalog_verifier(monkeypatch, entries)
    by_prefix = {entry["prefix_sha256"]: entry for entry in entries}

    def inspect(path, **_kwargs):
        prefix_hash = hashlib.sha256(Path(path).read_bytes()).hexdigest()
        entry = by_prefix[prefix_hash]
        return SimpleNamespace(
            sha256=prefix_hash,
            local_file_intact=True,
            tcbbo_records=10,
            tcbbo_timestamped_records=10,
            tcbbo_valid_nbbo_records=10,
            provider_errors=(),
            slow_reader_warnings=0,
            subscription_acks=1,
            replay_completed=1,
            last_trade_event_ns=entry["last_trade_event_ns"],
        )

    calls = []

    def build(_prefix_path, **kwargs):
        calls.append(kwargs["session_id"])
        entry = by_prefix[kwargs["prefix_sha256"]]
        return _surface(entry, mutate_feature=mutate_feature)

    monkeypatch.setattr(paper_evidence, "inspect_dbn", inspect)
    monkeypatch.setattr(
        "backend.closing_tape.live_shadow.build_surface_from_live_prefix", build
    )
    return calls


def test_paper_prefix_replay_copies_and_rebuilds_once_for_five_families(
    tmp_path, monkeypatch
):
    entry = _catalog_fixture(
        tmp_path, date(2026, 8, 25), session_id="session-2026-08-25"
    )
    payloads, _rows, recorded = _payload_bundle(entry)
    calls = _patch_replay(monkeypatch, [entry])

    result = replay_verify_paper_feature_groups(
        payloads,
        project_root=tmp_path,
        market_db_path=tmp_path / "market.db",
        recorded_at_by_payload=recorded,
    )

    assert calls == [entry["session_id"]]
    assert len(result.receipt_sha256s) == 1
    assert result.final_source_sha256s == (entry["final_sha256"],)
    assert set(result.payload_receipt_sha256s) == {
        payload.feature_payload_sha256 for payload in payloads
    }
    assert set(result.payload_receipt_sha256s.values()) == set(
        result.receipt_sha256s
    )


@pytest.mark.parametrize("mutation", ["missing", "tampered"])
def test_paper_prefix_replay_fails_closed_for_missing_or_tampered_raw_dbn(
    tmp_path, monkeypatch, mutation
):
    entry = _catalog_fixture(
        tmp_path, date(2026, 8, 25), session_id="session-2026-08-25"
    )
    payloads, _rows, recorded = _payload_bundle(entry)
    _patch_replay(monkeypatch, [entry])
    if mutation == "missing":
        entry["source"].unlink()
    else:
        entry["source"].write_bytes(b"tampered" + entry["source"].read_bytes())

    with pytest.raises(ValueError, match="retained .*DBN|missing|shorter"):
        replay_verify_paper_feature_groups(
            payloads,
            project_root=tmp_path,
            market_db_path=tmp_path / "market.db",
            recorded_at_by_payload=recorded,
        )


def test_paper_prefix_replay_rejects_ambiguous_catalog_identity(
    tmp_path, monkeypatch
):
    entry = _catalog_fixture(
        tmp_path, date(2026, 8, 25), session_id="session-2026-08-25"
    )
    payloads, _rows, recorded = _payload_bundle(entry)
    _patch_replay(monkeypatch, [entry])
    second_root = tmp_path / "second-root"
    duplicate_dir = second_root / entry["trading_day"].isoformat()
    duplicate_dir.mkdir(parents=True)
    shutil.copy2(entry["catalog"], duplicate_dir / "closing_tape.sqlite")
    monkeypatch.setenv("CLOSING_TAPE_CATALOG_ROOTS", str(second_root))

    with pytest.raises(ValueError, match="exactly one retained"):
        replay_verify_paper_feature_groups(
            payloads,
            project_root=tmp_path,
            market_db_path=tmp_path / "market.db",
            recorded_at_by_payload=recorded,
        )


def test_paper_prefix_replay_requires_exact_feature_reference_and_source_semantics(
    tmp_path, monkeypatch
):
    entry = _catalog_fixture(
        tmp_path, date(2026, 8, 25), session_id="session-2026-08-25"
    )
    payloads, _rows, recorded = _payload_bundle(entry)
    _patch_replay(monkeypatch, [entry], mutate_feature=True)

    with pytest.raises(ValueError, match="semantics do not match"):
        replay_verify_paper_feature_groups(
            payloads,
            project_root=tmp_path,
            market_db_path=tmp_path / "market.db",
            recorded_at_by_payload=recorded,
        )


def test_campaign_audit_rejects_omitted_clean_finalized_session(
    tmp_path, monkeypatch
):
    first = _catalog_fixture(
        tmp_path, date(2026, 8, 25), session_id="session-2026-08-25"
    )
    second = _catalog_fixture(
        tmp_path, date(2026, 8, 26), session_id="session-2026-08-26"
    )
    _first_payloads, first_rows, _first_recorded = _payload_bundle(first)
    _second_payloads, second_rows, _second_recorded = _payload_bundle(second)
    _patch_catalog_verifier(monkeypatch, [first, second])
    arguments = {
        "project_root": tmp_path,
        "activated_at_utc": "2026-08-25T19:44:00+00:00",
        "latest_counted_trading_date": "2026-08-26",
    }

    with pytest.raises(ValueError, match="omitted a complete five-family"):
        audit_paper_campaign_coverage(first_rows, **arguments)

    result = audit_paper_campaign_coverage(
        first_rows + second_rows, **arguments
    )

    assert result.opportunity_count == 2
    assert len(result.payloads) == 10
    assert len(result.receipt_sha256) == 64
    assert result.receipt_sha256 == audit_paper_campaign_coverage(
        first_rows + second_rows, **arguments
    ).receipt_sha256
