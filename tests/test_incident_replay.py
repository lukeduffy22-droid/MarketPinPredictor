import json
from pathlib import Path

import pytest

from backend.incident_replay import export_incident_package, replay_incident_package


FIXTURE = Path(__file__).parent / "fixtures" / "incident_replay" / "selected_failures.json"


def _fixture_sources(tmp_path: Path) -> list[Path]:
    records = json.loads(FIXTURE.read_text(encoding="utf-8"))
    tmp_path.mkdir(parents=True)
    sources = []
    for record in records:
        path = tmp_path / f"{record['incident_id']}.json"
        path.write_text(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
        sources.append(path)
    return sources


def test_selected_incidents_replay_same_rejection_reasons_offline(tmp_path):
    project_root = tmp_path / "repository"
    project_root.mkdir()
    sources = _fixture_sources(tmp_path / "source")
    destination = tmp_path / "runtime" / "incident-package"

    exported = export_incident_package(
        sources, destination=destination, project_root=project_root
    )
    replayed = replay_incident_package(destination)

    assert replayed["manifest_sha256"] == exported["manifest_sha256"]
    assert replayed["incidents"] == {
        "closing-1": ["COMPLETE_PAIR_MINIMUM_NOT_MET"],
        "handoff-1": ["HANDOFF_NOT_ACTIVE"],
        "opening-1": ["OPENING_BUCKET_MISSING", "MISSING_EVIDENCE:prestage_receipt"],
        "persistence-1": [
            "CALCULATION_INPUT_PERSISTENCE_FAILED",
            "MISSING_EVIDENCE:calculation_input_sha256",
        ],
        "queue-1": ["QUEUE_FULL_DELTA_FREEZES_PROMOTION"],
    }
    entries = {entry["incident_id"]: entry for entry in exported["incidents"]}
    for source in sources:
        incident_id = json.loads(source.read_text(encoding="utf-8"))["incident_id"]
        copied = destination / entries[incident_id]["original_path"]
        assert source.read_bytes() == copied.read_bytes()


def test_export_rejects_credentials_and_repository_destination(tmp_path):
    project_root = tmp_path / "repository"
    project_root.mkdir()
    source = _fixture_sources(tmp_path / "source")[0]
    record = json.loads(source.read_text(encoding="utf-8"))
    record["evidence"]["api_key"] = "not-exportable"
    source.write_text(json.dumps(record), encoding="utf-8")

    with pytest.raises(ValueError, match="credential-like"):
        export_incident_package(
            [source], destination=tmp_path / "outside", project_root=project_root
        )

    source = _fixture_sources(tmp_path / "clean-source")[0]
    with pytest.raises(ValueError, match="outside the repository"):
        export_incident_package(
            [source], destination=project_root / "data", project_root=project_root
        )


def test_replay_rejects_modified_original(tmp_path):
    project_root = tmp_path / "repository"
    project_root.mkdir()
    destination = tmp_path / "runtime" / "incident-package"
    exported = export_incident_package(
        _fixture_sources(tmp_path / "source"),
        destination=destination,
        project_root=project_root,
    )
    original = destination / exported["incidents"][0]["original_path"]
    original.write_bytes(original.read_bytes() + b" ")

    with pytest.raises(ValueError, match="hash or size"):
        replay_incident_package(destination)


def test_export_is_deterministic_across_source_argument_order(tmp_path):
    project_root = tmp_path / "repository"
    project_root.mkdir()
    sources = _fixture_sources(tmp_path / "source")
    first = export_incident_package(
        sources,
        destination=tmp_path / "runtime" / "first",
        project_root=project_root,
    )
    second = export_incident_package(
        list(reversed(sources)),
        destination=tmp_path / "runtime" / "second",
        project_root=project_root,
    )

    assert first["manifest_sha256"] == second["manifest_sha256"]
