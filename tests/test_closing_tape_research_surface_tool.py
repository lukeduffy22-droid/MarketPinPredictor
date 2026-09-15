from types import SimpleNamespace

import pandas as pd
import pytest

from tools import build_closing_tape_research_surface as tool


def test_surface_tool_default_discovers_configured_external_catalog(
    tmp_path,
    monkeypatch,
    capsys,
):
    catalog = tmp_path / "external" / "2026-08-25" / "closing_tape.sqlite"
    catalog.parent.mkdir(parents=True)
    catalog.write_bytes(b"catalog fixture")
    monkeypatch.setenv("CLOSING_TAPE_CATALOG_ROOTS", str(catalog.parents[1]))
    captured = {}
    report = SimpleNamespace(
        surface_rows=1,
        to_dict=lambda: {"surface_rows": 1},
    )

    def build(catalogs, _market_db, **_kwargs):
        captured["catalogs"] = tuple(catalogs)
        return pd.DataFrame({"x": [1]}), report

    monkeypatch.setattr(tool, "build_research_surface_dataset", build)
    monkeypatch.setattr(
        tool,
        "require_compute_window",
        lambda *_args, **_kwargs: (),
    )
    monkeypatch.setattr(
        tool,
        "load_scored_marketpin_closes",
        lambda *_args, **_kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(
        tool,
        "prepare_close_training_dataset",
        lambda *_args, **_kwargs: (
            pd.DataFrame(),
            SimpleNamespace(to_dict=lambda: {"ready": False}),
        ),
    )

    code = tool.main(["--project-root", str(tmp_path)])

    assert code == 0
    assert captured["catalogs"] == (catalog.resolve(),)
    payload = capsys.readouterr().out
    assert '"mode": "configured_roots"' in payload


def test_surface_tool_rejects_missing_explicit_catalog_before_build(
    tmp_path,
    monkeypatch,
):
    called = False

    def forbidden(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("surface build must not run with invalid catalog input")

    monkeypatch.setattr(tool, "build_research_surface_dataset", forbidden)
    monkeypatch.setattr(
        tool,
        "require_compute_window",
        lambda *_args, **_kwargs: (),
    )

    with pytest.raises(SystemExit):
        tool.main(
            [
                "--project-root",
                str(tmp_path),
                "--catalog",
                str(tmp_path / "missing.sqlite"),
            ]
        )

    assert called is False


def test_surface_tool_explicit_catalog_ignores_offline_archive_setting(
    tmp_path,
    monkeypatch,
):
    catalog = tmp_path / "explicit" / "2026-08-25" / "closing_tape.sqlite"
    catalog.parent.mkdir(parents=True)
    catalog.write_bytes(b"catalog fixture")
    missing_archive = tmp_path / "offline-archive"
    monkeypatch.setenv("CLOSING_TAPE_CATALOG_ROOTS", str(missing_archive))
    captured = {}
    report = SimpleNamespace(
        surface_rows=1,
        to_dict=lambda: {"surface_rows": 1},
    )

    def build(catalogs, _market_db, **_kwargs):
        captured["catalogs"] = tuple(catalogs)
        return pd.DataFrame({"x": [1]}), report

    monkeypatch.setattr(tool, "build_research_surface_dataset", build)
    monkeypatch.setattr(
        tool,
        "load_scored_marketpin_closes",
        lambda *_args, **_kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(
        tool,
        "prepare_close_training_dataset",
        lambda *_args, **_kwargs: (
            pd.DataFrame(),
            SimpleNamespace(to_dict=lambda: {"ready": False}),
        ),
    )

    code = tool.main(
        [
            "--project-root",
            str(tmp_path),
            "--catalog",
            str(catalog),
        ]
    )

    assert code == 0
    assert captured["catalogs"] == (catalog.resolve(),)
    assert not missing_archive.exists()
