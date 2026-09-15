"""Explicit, append-only lineage repairs; original close observations stay immutable.

A repair can link a later corroborating root to an earlier equal-price root.
It cannot change a price, source, timestamp, or existing correction link.
Unreconciled duplicate roots still fail the ordinary chain validators.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path

from .close_evidence import (
    resolve_verified_close_artifact,
    validate_official_close_reference,
    validate_verified_close_observed_at,
)

TABLE = "eod_close_lineage_reconciliations"


def _execute(connection, sql, parameters=()):
    method = getattr(connection, "exec_driver_sql", None)
    return method(sql, parameters) if method else connection.execute(sql, parameters)


def _mapping(row):
    return dict(row._mapping if hasattr(row, "_mapping") else row)


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False, default=str).encode()).hexdigest()


def _observed(row):
    value = datetime.fromisoformat(str(row["observed_at_utc"]).replace("Z", "+00:00"))
    # SQLite stores the existing UTC DATETIME columns without a zone suffix.
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _check_pair(child, parent):
    if child["correction_of_id"] is not None or parent["correction_of_id"] is not None:
        raise ValueError("reconciliation requires two original root observations")
    if int(child["id"]) <= int(parent["id"]):
        raise ValueError("reconciliation parent must precede the corroborating root")
    if any(child[k] != parent[k] for k in ("symbol", "trading_date", "official_close")):
        raise ValueError("reconciliation requires the same symbol, date and exact close")
    if not all(int(r["source_verified"]) == 1 and
               math.isfinite(float(r["official_close"])) and float(r["official_close"]) > 0
               for r in (child, parent)):
        raise ValueError("reconciliation requires verified finite positive closes")
    if _observed(child) < _observed(parent):
        raise ValueError("reconciliation cannot reverse observation chronology")
    for row in (child, parent):
        validate_verified_close_observed_at(str(row["trading_date"]), _observed(row))
        validate_official_close_reference(row["symbol"], row["source"], row["source_reference"])


def load_verified_close_parent_overrides(connection):
    """Validate retained receipts against exact original rows; never write on read."""
    exists = _execute(connection,
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)).fetchone()
    if not exists:
        return {}
    overrides = {}
    for raw in _execute(connection, f"SELECT * FROM {TABLE} ORDER BY observation_id").fetchall():
        receipt = _mapping(raw)
        document = json.loads(receipt["receipt_json"])
        required = {"version", "observation_id", "parent_observation_id", "observation_sha256",
                    "parent_observation_sha256", "recorded_at_utc", "reason"}
        if not isinstance(document, dict) or set(document) != required:
            raise ValueError("close lineage reconciliation receipt structure is invalid")
        if document.get("version") != 1 or _digest(document) != receipt["receipt_sha256"]:
            raise ValueError("close lineage reconciliation receipt hash or version is invalid")
        child_id, parent_id = document["observation_id"], document["parent_observation_id"]
        if (type(child_id) is not int or type(parent_id) is not int or
                child_id <= 0 or parent_id <= 0 or
                child_id != receipt["observation_id"] or parent_id != receipt["parent_observation_id"]):
            raise ValueError("close lineage reconciliation identity is invalid")
        selected = _execute(connection,
            "SELECT * FROM eod_close_observations WHERE id IN (?,?)", (child_id, parent_id)).fetchall()
        rows = {int(r["id"]): r for r in map(_mapping, selected)}
        if set(rows) != {child_id, parent_id}:
            raise ValueError("close lineage reconciliation references missing observations")
        child, parent = rows[child_id], rows[parent_id]
        _check_pair(child, parent)
        if (_digest(child) != document["observation_sha256"] or
                _digest(parent) != document["parent_observation_sha256"]):
            raise ValueError("close lineage reconciliation observation fingerprint changed")
        if child_id in overrides:
            raise ValueError("duplicate close lineage reconciliation")
        overrides[child_id] = parent_id
    return overrides


def _validate_graph(rows, overrides):
    by_id = {int(r["id"]): r for r in rows}
    children, roots = {}, []
    for ident, row in by_id.items():
        parent = overrides.get(ident, row["correction_of_id"])
        if parent is None:
            roots.append(ident)
        elif parent not in by_id or parent >= ident or parent in children:
            raise ValueError("reconciled close lineage is missing, non-causal or forked")
        else:
            children[parent] = ident
    if len(roots) != 1:
        raise ValueError("reconciled close lineage must have exactly one causal root")
    visited, current = set(), roots[0]
    while current not in visited:
        visited.add(current)
        if current not in children:
            break
        current = children[current]
    if visited != set(by_id):
        raise ValueError("reconciled close lineage is disconnected")


def reconcile_duplicate_close_roots(connection, pairs, *, project_root, reason):
    """Append verified receipts inside a caller-owned transaction; never edit the ledger.

    pairs contains (later_root_id, earlier_root_id). The caller must retain a
    before-image and verify the source contents before explicitly requesting this.
    """
    if not str(reason).strip():
        raise ValueError("a reconciliation reason is required")
    overrides = load_verified_close_parent_overrides(connection)
    proposed, groups = [], {}
    for child_id, parent_id in pairs:
        if type(child_id) is not int or type(parent_id) is not int or child_id <= 0 or parent_id <= 0:
            raise ValueError("reconciliation identities must be positive integers")
        selected = _execute(connection,
            "SELECT * FROM eod_close_observations WHERE id IN (?,?)", (child_id, parent_id)).fetchall()
        rows = {int(r["id"]): r for r in map(_mapping, selected)}
        if set(rows) != {child_id, parent_id}:
            raise ValueError("reconciliation references missing observations")
        child, parent = rows[child_id], rows[parent_id]
        _check_pair(child, parent)
        for row in (child, parent):
            resolve_verified_close_artifact(Path(project_root) / "data" / "verified_close_sources",
                trading_date=str(row["trading_date"]), symbol=row["symbol"],
                source_artifact_sha256=row["source_artifact_sha256"])
        if child_id in overrides:
            if overrides[child_id] != parent_id:
                raise ValueError("reconciliation conflicts with an existing receipt")
            continue
        overrides[child_id] = parent_id
        groups[(child["symbol"], str(child["trading_date"]))] = True
        document = {"version": 1, "observation_id": child_id, "parent_observation_id": parent_id,
            "observation_sha256": _digest(child), "parent_observation_sha256": _digest(parent),
            "recorded_at_utc": datetime.now(timezone.utc).isoformat(), "reason": str(reason).strip()}
        proposed.append((child_id, parent_id, _digest(document), json.dumps(document, sort_keys=True)))
    for symbol, day in groups:
        rows = list(map(_mapping, _execute(connection,
            "SELECT * FROM eod_close_observations WHERE symbol=? AND trading_date=? AND source_verified=1",
            (symbol, day)).fetchall()))
        _validate_graph(rows, overrides)
    if not proposed:
        return 0
    _execute(connection, f"CREATE TABLE IF NOT EXISTS {TABLE} ("
        "observation_id INTEGER PRIMARY KEY REFERENCES eod_close_observations(id), "
        "parent_observation_id INTEGER NOT NULL REFERENCES eod_close_observations(id), "
        "receipt_sha256 TEXT NOT NULL UNIQUE, receipt_json TEXT NOT NULL)")
    for action in ("UPDATE", "DELETE"):
        _execute(connection, f"CREATE TRIGGER IF NOT EXISTS {TABLE}_no_{action.lower()} "
            f"BEFORE {action} ON {TABLE} BEGIN SELECT RAISE(ABORT, 'close reconciliation receipts are immutable'); END")
    _execute(connection, f"CREATE TRIGGER IF NOT EXISTS {TABLE}_no_replace BEFORE INSERT ON {TABLE} "
        f"WHEN EXISTS(SELECT 1 FROM {TABLE} WHERE observation_id=NEW.observation_id OR receipt_sha256=NEW.receipt_sha256) "
        "BEGIN SELECT RAISE(ABORT, 'close reconciliation receipts are immutable'); END")
    for values in proposed:
        _execute(connection, f"INSERT INTO {TABLE} VALUES (?,?,?,?)", values)
    return len(proposed)
