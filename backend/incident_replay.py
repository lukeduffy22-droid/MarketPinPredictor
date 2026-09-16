"""Read-only packaging and deterministic replay of sanitized incident evidence."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Mapping, Sequence


INCIDENT_EVIDENCE_SCHEMA = "marketpin-incident-evidence.v1"
INCIDENT_PACKAGE_SCHEMA = "marketpin-incident-replay-package.v1"
INCIDENT_CATEGORIES = frozenset(
    {"opening", "closing", "handoff", "queue_overload", "persistence"}
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SECRET_KEY = re.compile(
    r"(^|_)(api_?key|authorization|bearer|credential|password|private_?key|secret|token)($|_)",
    re.IGNORECASE,
)


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _assert_no_credentials(value: object, *, path: str = "$") -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            name = str(key)
            if _SECRET_KEY.search(name):
                raise ValueError(f"incident evidence contains credential-like field: {path}.{name}")
            _assert_no_credentials(nested, path=f"{path}.{name}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _assert_no_credentials(nested, path=f"{path}[{index}]")


def rejection_reasons(record: Mapping[str, object]) -> tuple[str, ...]:
    """Derive fail-closed reasons without consulting live or mutable state."""

    checks = record.get("checks")
    if not isinstance(checks, list) or not checks:
        raise ValueError("incident evidence checks must be a non-empty list")
    reasons: list[str] = []
    for check in checks:
        if not isinstance(check, Mapping) or set(check) != {"name", "passed", "reason"}:
            raise ValueError("incident evidence check fields are invalid")
        name = str(check.get("name") or "").strip()
        reason = str(check.get("reason") or "").strip()
        passed = check.get("passed")
        if not name or type(passed) is not bool or (not passed and not reason):
            raise ValueError("incident evidence check is invalid")
        if not passed:
            reasons.append(reason)
    missing = record.get("missing_evidence_reasons")
    if not isinstance(missing, list) or any(not str(item).strip() for item in missing):
        raise ValueError("incident missing-evidence reasons are invalid")
    reasons.extend(f"MISSING_EVIDENCE:{str(item).strip()}" for item in missing)
    return tuple(dict.fromkeys(reasons))


def validate_incident_record(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("incident evidence root must be an object")
    expected = {
        "schema_version",
        "incident_id",
        "category",
        "observed_at_utc",
        "symbol",
        "subscription_epoch_id",
        "subscription_generation",
        "validation_method",
        "missing_evidence_reasons",
        "checks",
        "evidence",
    }
    if set(value) != expected or value.get("schema_version") != INCIDENT_EVIDENCE_SCHEMA:
        raise ValueError("incident evidence fields or schema are invalid")
    category = str(value.get("category") or "")
    symbol = str(value.get("symbol") or "").upper().strip()
    incident_id = str(value.get("incident_id") or "").strip()
    validation_method = str(value.get("validation_method") or "").strip()
    try:
        observed = datetime.fromisoformat(
            str(value.get("observed_at_utc") or "").replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ValueError("incident observed timestamp is invalid") from exc
    epoch = value.get("subscription_epoch_id")
    generation = value.get("subscription_generation")
    if (
        category not in INCIDENT_CATEGORIES
        or not incident_id
        or not symbol
        or not validation_method
        or observed.tzinfo is None
        or (epoch is not None and _SHA256.fullmatch(str(epoch)) is None)
        or (generation is not None and (type(generation) is not int or generation <= 0))
        or not isinstance(value.get("evidence"), dict)
    ):
        raise ValueError("incident evidence identity is invalid")
    _assert_no_credentials(value)
    rejection_reasons(value)
    if _canonical_bytes(value) != json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8") + b"\n":
        raise AssertionError("canonical encoder is inconsistent")
    return dict(value)


def load_incident_record(path: str | Path) -> tuple[dict[str, object], bytes]:
    source = Path(path).resolve()
    raw = source.read_bytes()
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"incident evidence is not valid UTF-8 JSON: {source.name}") from exc
    return validate_incident_record(value), raw


def export_incident_package(
    sources: Sequence[str | Path],
    *,
    destination: str | Path,
    project_root: str | Path,
) -> dict[str, object]:
    """Copy selected evidence unchanged into a content-bound package outside Git."""

    root = Path(project_root).resolve()
    target = Path(destination).resolve()
    if target == root or target.is_relative_to(root):
        raise ValueError("incident replay packages must be written outside the repository")
    if target.exists():
        raise FileExistsError(target)
    loaded = [(Path(source).resolve(), *load_incident_record(source)) for source in sources]
    if not loaded:
        raise ValueError("at least one incident evidence file is required")
    loaded.sort(key=lambda item: str(item[1]["incident_id"]))
    target.mkdir(parents=True)
    originals = target / "originals"
    originals.mkdir()
    entries: list[dict[str, object]] = []
    try:
        for index, (source, record, raw) in enumerate(loaded, start=1):
            relative = Path("originals") / f"{index:03d}-{source.name}"
            copied = target / relative
            copied.write_bytes(raw)
            entries.append(
                {
                    "incident_id": record["incident_id"],
                    "category": record["category"],
                    "observed_at_utc": record["observed_at_utc"],
                    "symbol": record["symbol"],
                    "subscription_epoch_id": record["subscription_epoch_id"],
                    "subscription_generation": record["subscription_generation"],
                    "validation_method": record["validation_method"],
                    "missing_evidence_reasons": record["missing_evidence_reasons"],
                    "rejection_reasons": list(rejection_reasons(record)),
                    "original_path": relative.as_posix(),
                    "original_sha256": _sha256_bytes(raw),
                    "original_bytes": len(raw),
                }
            )
        manifest = {
            "schema_version": INCIDENT_PACKAGE_SCHEMA,
            "incidents": sorted(entries, key=lambda item: str(item["incident_id"])),
        }
        manifest_bytes = _canonical_bytes(manifest)
        (target / "manifest.json").write_bytes(manifest_bytes)
        return {
            **manifest,
            "manifest_sha256": _sha256_bytes(manifest_bytes),
            "package_path": str(target),
        }
    except Exception:
        shutil.rmtree(target, ignore_errors=True)
        raise


def replay_incident_package(package_path: str | Path) -> dict[str, object]:
    """Verify package bytes and reproduce every rejection reason offline."""

    root = Path(package_path).resolve()
    manifest_path = root / "manifest.json"
    manifest_bytes = manifest_path.read_bytes()
    try:
        manifest = json.loads(manifest_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("incident package manifest is invalid") from exc
    if (
        not isinstance(manifest, dict)
        or set(manifest) != {"schema_version", "incidents"}
        or manifest.get("schema_version") != INCIDENT_PACKAGE_SCHEMA
        or not isinstance(manifest.get("incidents"), list)
        or manifest_bytes != _canonical_bytes(manifest)
    ):
        raise ValueError("incident package manifest contract is invalid")
    reproduced: dict[str, list[str]] = {}
    entry_fields = {
        "incident_id",
        "category",
        "observed_at_utc",
        "symbol",
        "subscription_epoch_id",
        "subscription_generation",
        "validation_method",
        "missing_evidence_reasons",
        "rejection_reasons",
        "original_path",
        "original_sha256",
        "original_bytes",
    }
    for entry in manifest["incidents"]:
        if not isinstance(entry, dict) or set(entry) != entry_fields:
            raise ValueError("incident package entry is invalid")
        relative = Path(str(entry.get("original_path") or ""))
        source = (root / relative).resolve()
        if not source.is_relative_to(root) or relative.is_absolute():
            raise ValueError("incident package source path escapes package")
        raw = source.read_bytes()
        if (
            _sha256_bytes(raw) != entry.get("original_sha256")
            or len(raw) != entry.get("original_bytes")
        ):
            raise ValueError("incident package source hash or size does not match")
        record, loaded_raw = load_incident_record(source)
        reasons = list(rejection_reasons(record))
        identity_matches = all(
            record[field] == entry[field]
            for field in (
                "incident_id",
                "category",
                "observed_at_utc",
                "symbol",
                "subscription_epoch_id",
                "subscription_generation",
                "validation_method",
                "missing_evidence_reasons",
            )
        )
        if (
            loaded_raw != raw
            or not identity_matches
            or reasons != entry.get("rejection_reasons")
            or str(record["incident_id"]) in reproduced
        ):
            raise ValueError("incident rejection reasons do not reproduce")
        reproduced[str(record["incident_id"])] = reasons
    return {
        "status": "reproduced",
        "manifest_sha256": _sha256_bytes(manifest_bytes),
        "incidents": reproduced,
    }
