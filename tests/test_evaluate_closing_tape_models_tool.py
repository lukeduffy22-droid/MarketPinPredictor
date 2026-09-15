from types import SimpleNamespace

import pandas as pd
import pytest

from backend.closing_tape.catalog_discovery import CatalogDiscovery
from tools import evaluate_closing_tape_models as tool


def _ready_label_preflight():
    return SimpleNamespace(
        ready=True,
        reasons=(),
        to_dict=lambda: {"ready": True, "complete_bundle_sessions": 60},
    )


def _replay_verified_surface(tmp_path, frame=None):
    selection = CatalogDiscovery(
        catalog_roots=(),
        catalog_paths=(),
        issues=(),
        resolution_id="c" * 64,
    )
    return SimpleNamespace(
        frame=frame if frame is not None else pd.DataFrame({"x": [1]}),
        artifact_sha256="a" * 64,
        parquet_path=tmp_path / "surface.parquet",
        manifest_path=tmp_path / "surface.manifest.json",
        manifest={"row_count": 1, "source_sha256s": ["b" * 64]},
        catalog_selection=selection,
        replay_verification={"semantic_equality_verified": True},
        replay_receipt_sha256="d" * 64,
    )


def test_evaluation_default_discovers_configured_external_catalog(
    tmp_path,
    monkeypatch,
    capsys,
):
    catalog = tmp_path / "external" / "2026-08-25" / "closing_tape.sqlite"
    catalog.parent.mkdir(parents=True)
    catalog.write_bytes(b"catalog fixture")
    monkeypatch.setenv("CLOSING_TAPE_CATALOG_ROOTS", str(catalog.parents[1]))
    captured = {}
    surface_report = SimpleNamespace(to_dict=lambda: {"surface_rows": 1})
    evaluation = SimpleNamespace(
        trained=False,
        device_effective="cpu",
        evaluation_report=None,
        to_dict=lambda: {"trained": False, "reasons": ["fixture gate"]},
    )

    def build(catalogs, _market_db):
        captured["catalogs"] = tuple(catalogs)
        return pd.DataFrame({"x": [1]}), surface_report

    monkeypatch.setattr(tool, "build_research_surface_dataset", build)
    monkeypatch.setattr(
        tool,
        "load_scored_marketpin_closes",
        lambda _path, **_kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(
        tool,
        "audit_close_label_preflight",
        lambda _closes: _ready_label_preflight(),
    )
    monkeypatch.setattr(
        tool,
        "require_compute_window",
        lambda *_args, **_kwargs: (),
    )
    def evaluate(*_args, **kwargs):
        captured["evaluation_device"] = kwargs["device"]
        return evaluation

    monkeypatch.setattr(tool, "evaluate_close_models_if_ready", evaluate)

    code = tool.main(["--project-root", str(tmp_path), "--device", "cpu"])

    assert code == 2
    assert captured["catalogs"] == (catalog.resolve(),)
    assert captured["evaluation_device"] == "cpu"
    payload = capsys.readouterr().out
    assert '"mode": "configured_roots"' in payload
    assert str(catalog.resolve()).replace("\\", "\\\\") in payload


def test_invalid_explicit_catalog_never_falls_back_or_scans_labels(
    tmp_path,
    monkeypatch,
):
    local = (
        tmp_path
        / "data"
        / "closing_tape"
        / "2026-08-25"
        / "closing_tape.sqlite"
    )
    local.parent.mkdir(parents=True)
    local.write_bytes(b"local fixture")
    called = False

    def forbidden(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("label scan must not run for invalid catalog input")

    monkeypatch.setattr(tool, "load_scored_marketpin_closes", forbidden)

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


def test_evaluation_tool_can_package_without_enabling_production(tmp_path, monkeypatch, capsys):
    evaluation_report = SimpleNamespace(device="cpu")
    evaluation = SimpleNamespace(
        trained=True,
        device_effective="cpu",
        evaluation_report=evaluation_report,
        to_dict=lambda: {"trained": True, "evaluation": {"promoted": True}},
    )
    captured = {}

    frozen = _replay_verified_surface(tmp_path)
    monkeypatch.setattr(
        tool, "load_replay_verified_research_surface_artifact",
        lambda *_args, **_kwargs: frozen,
    )
    monkeypatch.setattr(
        tool, "load_scored_marketpin_closes", lambda _path, **_kwargs: pd.DataFrame()
    )
    monkeypatch.setattr(
        tool, "audit_close_label_preflight", lambda _closes: _ready_label_preflight()
    )
    monkeypatch.setattr(tool, "evaluate_close_models_if_ready", lambda *_args, **_kwargs: evaluation)

    def package(result, **kwargs):
        captured["result"] = result
        captured.update(kwargs)
        return SimpleNamespace(to_dict=lambda: {"version": kwargs["version"]})

    monkeypatch.setattr(tool, "package_promoted_candidate", package)
    artifact = tmp_path / "models" / "candidate-v1.json"
    output = tmp_path / "evaluation.json"

    code = tool.main(
        [
            "--project-root", str(tmp_path),
            "--surface-manifest", str(frozen.manifest_path),
            "--package-version", "candidate-v1",
            "--candidate-artifact", str(artifact),
            "--calibration-asof-utc", "2026-08-25T20:01:00+00:00",
            "--output-json", str(output),
        ]
    )

    assert code == 0
    assert captured["result"] is evaluation
    assert captured["version"] == "candidate-v1"
    assert captured["artifact_path"] == artifact.resolve()
    assert captured["device"] == "cpu"
    assert '"candidate_package"' in output.read_text(encoding="utf-8")
    assert '"candidate_package"' in capsys.readouterr().out
    assert not (tmp_path / "models" / "closing_tape_model.json").exists()
    assert not (tmp_path / "models" / "closing_tape_paper_candidate.json").exists()


def test_evaluation_tool_requires_explicit_paper_activation(tmp_path, monkeypatch):
    evaluation = SimpleNamespace(
        trained=True,
        device_effective="cpu",
        evaluation_report=SimpleNamespace(device="cpu"),
        to_dict=lambda: {"trained": True, "evaluation": {"promoted": True}},
    )
    package = SimpleNamespace(to_dict=lambda: {"version": "candidate-v1"})
    frozen = _replay_verified_surface(tmp_path)
    monkeypatch.setattr(
        tool, "load_replay_verified_research_surface_artifact",
        lambda *_args, **_kwargs: frozen,
    )
    monkeypatch.setattr(
        tool, "load_scored_marketpin_closes", lambda _path, **_kwargs: pd.DataFrame()
    )
    monkeypatch.setattr(
        tool, "audit_close_label_preflight", lambda _closes: _ready_label_preflight()
    )
    monkeypatch.setattr(
        tool, "evaluate_close_models_if_ready", lambda *_args, **_kwargs: evaluation
    )
    monkeypatch.setattr(tool, "package_promoted_candidate", lambda *_args, **_kwargs: package)
    captured = {}

    def activate(project_root, received_package, **kwargs):
        captured["project_root"] = project_root
        captured["package"] = received_package
        captured.update(kwargs)
        return tmp_path / "models" / "closing_tape_paper_candidate.json"

    monkeypatch.setattr(tool, "write_paper_candidate_descriptor", activate)
    output = tmp_path / "evaluation.json"
    artifact = tmp_path / "models" / "candidate-v1.json"

    code = tool.main(
        [
            "--project-root", str(tmp_path),
            "--surface-manifest", str(frozen.manifest_path),
            "--package-version", "candidate-v1",
            "--candidate-artifact", str(artifact),
            "--enable-paper-candidate",
            "--output-json", str(output),
        ]
    )

    assert code == 0
    assert captured == {
        "project_root": tmp_path.resolve(),
        "package": package,
        "allow_replace": False,
    }
    assert "paper_candidate_descriptor" in output.read_text(encoding="utf-8")


def test_packaging_requires_new_durable_evidence_output(tmp_path, monkeypatch):
    artifact = tmp_path / "candidate.json"
    with pytest.raises(SystemExit):
        tool.main(
            [
                "--project-root", str(tmp_path),
                "--package-version", "candidate-v1",
                "--candidate-artifact", str(artifact),
            ]
        )
    assert not artifact.exists()

    output = tmp_path / "existing.json"
    output.write_text("preserve me", encoding="utf-8")
    called = False

    def forbidden(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("evaluation must not run before an output conflict is resolved")

    monkeypatch.setattr(tool, "build_research_surface_dataset", forbidden)
    with pytest.raises(SystemExit):
        tool.main(
            [
                "--project-root", str(tmp_path),
                "--surface-manifest", str(tmp_path / "surface.manifest.json"),
                "--package-version", "candidate-v1",
                "--candidate-artifact", str(artifact),
                "--output-json", str(output),
            ]
        )
    assert not called
    assert output.read_text(encoding="utf-8") == "preserve me"
    assert not artifact.exists()


def test_nonfinite_evaluation_evidence_is_rejected_before_packaging(tmp_path, monkeypatch):
    evaluation = SimpleNamespace(
        trained=True,
        device_effective="cpu",
        evaluation_report=SimpleNamespace(device="cpu"),
        to_dict=lambda: {"trained": True, "evaluation": {"mae": float("nan")}},
    )
    frozen = _replay_verified_surface(tmp_path)
    monkeypatch.setattr(
        tool, "load_replay_verified_research_surface_artifact",
        lambda *_args, **_kwargs: frozen,
    )
    monkeypatch.setattr(
        tool, "load_scored_marketpin_closes", lambda _path, **_kwargs: pd.DataFrame()
    )
    monkeypatch.setattr(
        tool, "audit_close_label_preflight", lambda _closes: _ready_label_preflight()
    )
    monkeypatch.setattr(tool, "evaluate_close_models_if_ready", lambda *_args, **_kwargs: evaluation)
    called = False

    def forbidden(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("non-finite evidence must fail before artifact packaging")

    monkeypatch.setattr(tool, "package_promoted_candidate", forbidden)
    artifact = tmp_path / "candidate.json"
    output = tmp_path / "evidence.json"

    with pytest.raises(ValueError, match="not strict finite JSON"):
        tool.main(
            [
                    "--project-root", str(tmp_path),
                    "--surface-manifest", str(frozen.manifest_path),
                    "--package-version", "candidate-v1",
                "--candidate-artifact", str(artifact),
                "--output-json", str(output),
            ]
        )

    assert not called
    assert not artifact.exists()
    assert not output.exists()


def test_label_preflight_skips_expensive_surface_scan(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(
        tool, "load_scored_marketpin_closes", lambda _path, **_kwargs: pd.DataFrame()
    )

    def forbidden(*_args, **_kwargs):
        raise AssertionError("surface construction must not run without enough close bundles")

    monkeypatch.setattr(tool, "build_research_surface_dataset", forbidden)

    code = tool.main(
        [
            "--project-root", str(tmp_path),
            "--surface-manifest", str(tmp_path / "surface.manifest.json"),
            "--device", "cuda",
        ]
    )

    assert code == 2
    payload = capsys.readouterr().out
    assert '"trained": false' in payload
    assert '"skipped": true' in payload
    assert '"complete_bundle_sessions": 0' in payload
    assert '"device_requested": "cuda"' in payload


def test_evaluator_reuses_replay_verified_surface_artifact(tmp_path, monkeypatch, capsys):
    frame = pd.DataFrame({"feature": [1.0]})
    frozen = _replay_verified_surface(tmp_path, frame)
    frozen.manifest["replay_verified"] = False
    frozen.manifest["replay_verification"] = {"semantic_equality_verified": False}
    evaluation = SimpleNamespace(
        trained=False,
        device_effective="cpu",
        evaluation_report=None,
        to_dict=lambda: {"trained": False, "reasons": ["test gate"]},
    )
    monkeypatch.setattr(
        tool, "load_scored_marketpin_closes", lambda _path, **_kwargs: pd.DataFrame()
    )
    monkeypatch.setattr(
        tool, "audit_close_label_preflight", lambda _closes: _ready_label_preflight()
    )
    captured_load = {}

    def load(*args, **kwargs):
        captured_load["args"] = args
        captured_load["kwargs"] = kwargs
        return frozen

    monkeypatch.setattr(tool, "load_replay_verified_research_surface_artifact", load)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("catalog surface construction must not run for a frozen artifact")

    monkeypatch.setattr(tool, "build_research_surface_dataset", forbidden)
    captured = {}

    def evaluate(received, *_args, **_kwargs):
        captured["surface"] = received
        return evaluation

    monkeypatch.setattr(tool, "evaluate_close_models_if_ready", evaluate)

    code = tool.main(
        ["--project-root", str(tmp_path), "--surface-manifest", "surface.manifest.json"]
    )

    assert code == 2
    assert captured["surface"] is frozen
    assert captured_load["kwargs"]["project_root"] == tmp_path.resolve()
    assert captured_load["kwargs"]["market_db_path"] == (
        tmp_path / "data" / "market_data.db"
    )
    output = capsys.readouterr().out
    assert '"loaded_from_artifact": true' in output
    assert '"replay_verified": true' in output
    assert '"semantic_equality_verified": true' in output
    expected_hash = "a" * 64
    assert f'"artifact_sha256": "{expected_hash}"' in output


def test_packaging_rejects_catalog_built_surface_before_any_label_or_model_scan(
    tmp_path,
    monkeypatch,
):
    called = False

    def forbidden(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("packaging must stop before scanning mutable inputs")

    monkeypatch.setattr(tool, "load_scored_marketpin_closes", forbidden)
    with pytest.raises(SystemExit, match="2"):
        tool.main(
            [
                "--project-root", str(tmp_path),
                "--package-version", "candidate-v1",
                "--candidate-artifact", str(tmp_path / "candidate.json"),
                "--output-json", str(tmp_path / "evaluation.json"),
            ]
        )
    assert called is False


def test_cuda_rejects_catalog_built_surface_before_any_label_or_model_scan(
    tmp_path,
    monkeypatch,
):
    called = False

    def forbidden(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("CUDA must stop before scanning mutable inputs")

    monkeypatch.setattr(tool, "load_scored_marketpin_closes", forbidden)
    with pytest.raises(SystemExit, match="2"):
        tool.main(["--project-root", str(tmp_path), "--device", "cuda"])
    assert called is False
