import json
import os

import pytest

from backend.closing_tape.catalog_discovery import (
    CATALOG_ROOTS_ENV,
    catalog_configuration_resolution_id,
    discover_closing_tape_catalogs,
    select_closing_tape_catalogs,
)


def _write_catalog(root, trading_date):
    path = root / trading_date / "closing_tape.sqlite"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"sqlite fixture")
    return path.resolve()


def test_default_discovery_preserves_local_one_level_contract(tmp_path):
    local_root = tmp_path / "data" / "closing_tape"
    later = _write_catalog(local_root, "2026-08-27")
    earlier = _write_catalog(local_root, "2026-08-25")
    _write_catalog(local_root / "nested", "2026-08-26")

    discovery = discover_closing_tape_catalogs(tmp_path, environment={})

    assert discovery.catalog_roots == (str(local_root.resolve()),)
    assert discovery.catalog_paths == (earlier, later)
    assert discovery.issues == ()
    assert len(discovery.resolution_id) == 64
    json.dumps(discovery.to_dict(), sort_keys=True)


def test_configured_roots_extend_local_discovery_and_dedupe_paths(tmp_path):
    local_root = tmp_path / "data" / "closing_tape"
    external_root = tmp_path / "external"
    local = _write_catalog(local_root, "2026-08-25")
    external = _write_catalog(external_root, "2026-08-26")
    environment = {
        CATALOG_ROOTS_ENV: os.pathsep.join(
            (str(external_root), str(external_root), str(local_root))
        )
    }

    discovery = discover_closing_tape_catalogs(
        tmp_path,
        environment=environment,
    )

    assert discovery.catalog_paths == (local, external)
    assert discovery.catalog_roots == (
        str(local_root.resolve()),
        str(external_root.resolve()),
    )
    assert discovery.issues == ()


def test_relative_configured_root_is_project_relative(tmp_path):
    external = _write_catalog(tmp_path / "archive", "2026-08-25")

    discovery = discover_closing_tape_catalogs(
        tmp_path,
        environment={CATALOG_ROOTS_ENV: "archive"},
    )

    assert discovery.catalog_paths == (external,)
    assert discovery.catalog_roots[-1] == str((tmp_path / "archive").resolve())


def test_missing_configured_root_is_an_issue_without_creating_it(tmp_path):
    missing = tmp_path / "missing-archive"

    discovery = discover_closing_tape_catalogs(
        tmp_path,
        environment={CATALOG_ROOTS_ENV: str(missing)},
    )

    assert discovery.catalog_paths == ()
    assert discovery.issues == (
        f"configured catalog root is missing: {missing.resolve()}",
    )
    assert not missing.exists()


def test_non_date_catalog_partition_is_reported_and_excluded(tmp_path):
    invalid = _write_catalog(
        tmp_path / "data" / "closing_tape",
        "archive-copy",
    )

    discovery = discover_closing_tape_catalogs(tmp_path, environment={})

    assert discovery.catalog_paths == ()
    assert discovery.issues == (
        f"catalog partition is not an ISO trading date: {invalid.parent}",
    )


def test_explicit_catalogs_replace_defaults_and_invalid_input_never_falls_back(
    tmp_path,
):
    local = _write_catalog(
        tmp_path / "data" / "closing_tape",
        "2026-08-25",
    )
    external = _write_catalog(tmp_path / "external", "2026-08-26")

    selected = select_closing_tape_catalogs(
        tmp_path,
        explicit_catalogs=(external, external),
        environment={},
    )
    assert selected.catalog_paths == (external,)
    assert local not in selected.catalog_paths
    assert selected.issues == ()

    missing = tmp_path / "missing.sqlite"
    invalid = select_closing_tape_catalogs(
        tmp_path,
        explicit_catalogs=(missing,),
        environment={},
    )
    assert invalid.catalog_paths == ()
    assert invalid.issues == (f"explicit catalog is missing: {missing}",)
    assert local not in invalid.catalog_paths


def test_configuration_resolution_changes_with_root_configuration(tmp_path):
    default_id = catalog_configuration_resolution_id(
        tmp_path,
        environment={},
    )
    external_id = catalog_configuration_resolution_id(
        tmp_path,
        environment={CATALOG_ROOTS_ENV: str(tmp_path / "external")},
    )

    assert len(default_id) == 64
    assert len(external_id) == 64
    assert default_id != external_id


def test_configuration_resolution_matches_discovery_receipt(tmp_path):
    environment = {
        CATALOG_ROOTS_ENV: os.pathsep.join(
            (str(tmp_path / "missing-external"), "relative-archive")
        )
    }

    configuration_id = catalog_configuration_resolution_id(
        tmp_path,
        environment=environment,
    )
    discovery = discover_closing_tape_catalogs(
        tmp_path,
        environment=environment,
    )

    assert configuration_id == discovery.resolution_id


def test_catalog_with_out_of_root_hardlink_fails_closed(tmp_path):
    catalog = _write_catalog(
        tmp_path / "data" / "closing_tape",
        "2026-08-25",
    )
    outside_alias = tmp_path / "outside-alias.sqlite"
    try:
        os.link(catalog, outside_alias)
    except OSError as exc:
        pytest.skip(f"hard links are unavailable on this test filesystem: {exc}")

    discovery = discover_closing_tape_catalogs(tmp_path, environment={})
    explicit = select_closing_tape_catalogs(
        tmp_path,
        explicit_catalogs=(catalog,),
        environment={},
    )

    assert discovery.catalog_paths == ()
    assert len(discovery.issues) == 1
    assert "multiple hard links" in discovery.issues[0]
    assert explicit.catalog_paths == ()
    assert len(explicit.issues) == 1
    assert "multiple hard links" in explicit.issues[0]
