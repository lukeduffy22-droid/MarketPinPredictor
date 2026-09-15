from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, Mapping


CATALOG_ROOTS_ENV = "CLOSING_TAPE_CATALOG_ROOTS"
CATALOG_FILENAME = "closing_tape.sqlite"


@dataclass(frozen=True)
class CatalogDiscovery:
    """Deterministic, read-only inventory of retained closing-tape catalogs."""

    catalog_roots: tuple[str, ...]
    catalog_paths: tuple[Path, ...]
    issues: tuple[str, ...]
    resolution_id: str

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["catalog_paths"] = [str(path) for path in self.catalog_paths]
        return payload


def _configured_root_values(
    configured_roots: Iterable[str | Path] | None,
    environment: Mapping[str, str],
) -> tuple[str, ...]:
    if configured_roots is not None:
        return tuple(
            str(value).strip()
            for value in configured_roots
            if str(value).strip()
        )
    raw = str(environment.get(CATALOG_ROOTS_ENV, "") or "")
    return tuple(
        value.strip().strip('"')
        for value in raw.split(os.pathsep)
        if value.strip().strip('"')
    )


def _path_key(path: Path) -> str:
    return os.path.normcase(str(path))


def _resolution_id(labels: Iterable[str]) -> str:
    normalized = tuple(os.path.normcase(str(label)) for label in labels)
    return hashlib.sha256(
        json.dumps(
            normalized,
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def catalog_configuration_resolution_id(
    project_root: str | Path,
    *,
    environment: Mapping[str, str] | None = None,
) -> str:
    """Hash configured root identity without scanning catalog contents."""

    root = Path(project_root).resolve()
    env = os.environ if environment is None else environment
    requested = _configured_root_values(None, env)
    labels = [str((root / "data" / "closing_tape").resolve())]
    keys = {_path_key(Path(labels[0]))}
    for raw_value in requested:
        try:
            expanded = Path(os.path.expandvars(raw_value)).expanduser()
            candidate = expanded if expanded.is_absolute() else root / expanded
            label = str(candidate.resolve())
        except (OSError, RuntimeError):
            label = raw_value
        key = os.path.normcase(label)
        if key not in keys:
            labels.append(label)
            keys.add(key)
    return _resolution_id(labels)


def discover_closing_tape_catalogs(
    project_root: str | Path,
    *,
    configured_roots: Iterable[str | Path] | None = None,
    environment: Mapping[str, str] | None = None,
) -> CatalogDiscovery:
    """Discover local plus explicitly configured date-partitioned catalogs.

    ``CLOSING_TAPE_CATALOG_ROOTS`` is an ``os.pathsep``-delimited list of
    additional roots. Relative entries are resolved against ``project_root``.
    The default ``data/closing_tape`` root remains included for live captures.
    Missing default storage is an empty state; a missing or inaccessible
    configured root is an issue so readiness can fail closed.
    """

    root = Path(project_root).resolve()
    env = os.environ if environment is None else environment
    requested = _configured_root_values(configured_roots, env)
    issues: list[str] = []
    root_records: dict[str, tuple[Path, bool]] = {}
    root_labels: list[str] = []

    def add_root(raw_value: str | Path, *, configured: bool) -> None:
        raw_text = str(raw_value).strip()
        try:
            expanded = Path(os.path.expandvars(raw_text)).expanduser()
            candidate = expanded if expanded.is_absolute() else root / expanded
            resolved = candidate.resolve()
        except (OSError, RuntimeError) as exc:
            label = raw_text or "<empty>"
            root_labels.append(label)
            root_kind = "configured catalog root" if configured else "catalog root"
            issues.append(
                f"{root_kind} is unavailable "
                f"({label}): {type(exc).__name__}: {exc}"
            )
            return
        key = _path_key(resolved)
        existing = root_records.get(key)
        root_records[key] = (resolved, configured or bool(existing and existing[1]))
        if existing is None:
            root_labels.append(str(resolved))

    add_root(root / "data" / "closing_tape", configured=False)
    for value in requested:
        add_root(value, configured=True)

    catalogs: dict[str, Path] = {}
    physical_catalogs: dict[tuple[int, int], Path] = {}
    for catalog_root, configured in root_records.values():
        try:
            metadata = catalog_root.stat()
        except FileNotFoundError:
            if configured:
                issues.append(f"configured catalog root is missing: {catalog_root}")
            continue
        except OSError as exc:
            issues.append(
                "catalog root cannot be inspected "
                f"({catalog_root}): {type(exc).__name__}: {exc}"
            )
            continue
        if not stat.S_ISDIR(metadata.st_mode):
            root_kind = "configured catalog root" if configured else "catalog root"
            issues.append(f"{root_kind} is not a directory: {catalog_root}")
            continue
        try:
            candidates = tuple(
                sorted(
                    catalog_root.glob(f"*/{CATALOG_FILENAME}"),
                    key=lambda path: str(path).casefold(),
                )
            )
        except OSError as exc:
            issues.append(
                "catalog root scan failed "
                f"({catalog_root}): {type(exc).__name__}: {exc}"
            )
            continue
        for candidate in candidates:
            partition = candidate.parent.name
            try:
                parsed_partition = date.fromisoformat(partition)
            except ValueError:
                issues.append(
                    f"catalog partition is not an ISO trading date: {candidate.parent}"
                )
                continue
            if parsed_partition.isoformat() != partition:
                issues.append(
                    f"catalog partition is not canonical ISO format: {candidate.parent}"
                )
                continue
            try:
                resolved_catalog = candidate.resolve(strict=True)
                catalog_metadata = resolved_catalog.stat()
            except (OSError, RuntimeError) as exc:
                issues.append(
                    "catalog path cannot be inspected "
                    f"({candidate}): {type(exc).__name__}: {exc}"
                )
                continue
            if not stat.S_ISREG(catalog_metadata.st_mode):
                issues.append(f"catalog path is not a regular file: {candidate}")
                continue
            link_count = int(getattr(catalog_metadata, "st_nlink", 1) or 1)
            if link_count > 1:
                issues.append(
                    "catalog file has multiple hard links and cannot be "
                    f"physically contained ({candidate}; links={link_count})"
                )
                continue
            if not resolved_catalog.is_relative_to(catalog_root):
                issues.append(
                    "catalog path escapes its configured root: "
                    f"{candidate} -> {resolved_catalog}"
                )
                continue
            inode = int(catalog_metadata.st_ino)
            if inode:
                physical_identity = (int(catalog_metadata.st_dev), inode)
                existing_physical = physical_catalogs.get(physical_identity)
                if (
                    existing_physical is not None
                    and existing_physical != resolved_catalog
                ):
                    issues.append(
                        "catalog physical alias is duplicated: "
                        f"{existing_physical} and {resolved_catalog}"
                    )
                    continue
                physical_catalogs[physical_identity] = resolved_catalog
            catalogs.setdefault(_path_key(resolved_catalog), resolved_catalog)

    ordered_catalogs = tuple(
        sorted(catalogs.values(), key=lambda path: str(path).casefold())
    )
    resolution_id = _resolution_id(root_labels)
    return CatalogDiscovery(
        catalog_roots=tuple(root_labels),
        catalog_paths=ordered_catalogs,
        issues=tuple(dict.fromkeys(issues)),
        resolution_id=resolution_id,
    )


def select_closing_tape_catalogs(
    project_root: str | Path,
    *,
    explicit_catalogs: Iterable[str | Path] | None = None,
    environment: Mapping[str, str] | None = None,
) -> CatalogDiscovery:
    """Resolve explicit catalog files, or use configured-root discovery.

    A nonempty explicit list remains a complete override of default discovery,
    matching the established research CLI contract. Invalid explicit inputs
    are reported and never fall back to local catalogs.
    """

    explicit = tuple(
        str(value).strip()
        for value in (explicit_catalogs or ())
        if str(value).strip()
    )
    if not explicit:
        return discover_closing_tape_catalogs(
            project_root,
            environment=environment,
        )

    issues: list[str] = []
    catalogs: dict[str, Path] = {}
    identity_labels: list[str] = []
    for raw_value in explicit:
        try:
            path = Path(os.path.expandvars(raw_value)).expanduser().resolve()
            metadata = path.stat()
        except FileNotFoundError:
            issues.append(f"explicit catalog is missing: {raw_value}")
            identity_labels.append(os.path.normcase(raw_value))
            continue
        except (OSError, RuntimeError) as exc:
            issues.append(
                "explicit catalog cannot be inspected "
                f"({raw_value}): {type(exc).__name__}: {exc}"
            )
            identity_labels.append(os.path.normcase(raw_value))
            continue
        identity_labels.append(_path_key(path))
        if not stat.S_ISREG(metadata.st_mode):
            issues.append(f"explicit catalog is not a regular file: {path}")
            continue
        link_count = int(getattr(metadata, "st_nlink", 1) or 1)
        if link_count > 1:
            issues.append(
                "explicit catalog has multiple hard links and cannot be "
                f"physically contained ({path}; links={link_count})"
            )
            continue
        catalogs.setdefault(_path_key(path), path)

    ordered_catalogs = tuple(
        sorted(catalogs.values(), key=lambda path: str(path).casefold())
    )
    resolution_id = hashlib.sha256(
        json.dumps(
            ("explicit", *sorted(set(identity_labels))),
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return CatalogDiscovery(
        catalog_roots=(),
        catalog_paths=ordered_catalogs,
        issues=tuple(dict.fromkeys(issues)),
        resolution_id=resolution_id,
    )
