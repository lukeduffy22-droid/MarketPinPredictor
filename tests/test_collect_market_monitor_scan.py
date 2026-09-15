from __future__ import annotations

import json
from pathlib import Path

import tools.collect_market_monitor_scan as cli


def test_cli_emits_exact_request_and_uses_fixed_five_second_sample(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    root = tmp_path / "project"
    database_path = root / "data" / "market_data.db"
    database_path.parent.mkdir(parents=True)
    database_path.touch()
    expected = {"scan": {"event_type": "substantive_scan"}, "policy_inputs": {}}
    calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        cli,
        "configured_market_database_path",
        lambda project_root: database_path,
    )

    def fake_collect(**kwargs):
        calls.append(kwargs)
        return expected

    monkeypatch.setattr(cli, "collect_commit_request", fake_collect)

    assert cli.main(["--project-root", str(root)]) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == expected
    assert captured.err == ""
    assert calls == [
        {
            "project_root": root.resolve(),
            "backend_url": "http://127.0.0.1:8000",
            "database_path": database_path,
            "state_path": (root / "exports" / "market_monitor" / "state.json").resolve(),
            "journal_dir": (root / "exports" / "market_monitor").resolve(),
            "sample_seconds": 5.0,
            "timeout_seconds": 5.0,
        }
    ]


def test_cli_failure_has_no_stdout_commit_payload(tmp_path: Path, monkeypatch, capsys) -> None:
    root = tmp_path / "project"
    monkeypatch.setattr(
        cli,
        "configured_market_database_path",
        lambda project_root: root / "market.db",
    )

    def fail_collect(**kwargs):
        raise cli.MonitorScanCollectorError("source_alignment_failed")

    monkeypatch.setattr(cli, "collect_commit_request", fail_collect)

    assert cli.main(["--project-root", str(root)]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    error = json.loads(captured.err)
    assert error == {
        "schema_version": cli.ERROR_SCHEMA,
        "accepted": False,
        "action": "abstain",
        "issues": [
            "collection_error:MonitorScanCollectorError:source_alignment_failed"
        ],
    }


def test_cli_abstains_when_implicit_database_conflicts_with_existing_canonical(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    root = tmp_path / "project"
    canonical_path = root / "data" / "market_data.db"
    canonical_path.parent.mkdir(parents=True)
    canonical_path.touch()
    selected_path = root / "market.db"
    collector_called = False

    monkeypatch.setattr(
        cli,
        "configured_market_database_path",
        lambda project_root: selected_path,
    )

    def fake_collect(**kwargs):
        nonlocal collector_called
        collector_called = True
        return {"unexpected": True}

    monkeypatch.setattr(cli, "collect_commit_request", fake_collect)

    assert cli.main(["--project-root", str(root)]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    error = json.loads(captured.err)
    assert error["accepted"] is False
    assert error["action"] == "abstain"
    assert len(error["issues"]) == 1
    assert "implicit_database_target_conflict" in error["issues"][0]
    assert "--database-path" in error["issues"][0]
    assert collector_called is False


def test_cli_explicit_database_path_bypasses_implicit_target_conflict(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    root = tmp_path / "project"
    canonical_path = root / "data" / "market_data.db"
    canonical_path.parent.mkdir(parents=True)
    canonical_path.touch()
    explicit_path = root / "market.db"
    expected = {"scan": {"event_type": "substantive_scan"}, "policy_inputs": {}}
    calls: list[dict[str, object]] = []

    def unexpected_implicit_resolution(project_root):
        raise AssertionError("explicit database path must bypass implicit resolution")

    monkeypatch.setattr(
        cli,
        "configured_market_database_path",
        unexpected_implicit_resolution,
    )

    def fake_collect(**kwargs):
        calls.append(kwargs)
        return expected

    monkeypatch.setattr(cli, "collect_commit_request", fake_collect)

    assert (
        cli.main(
            [
                "--project-root",
                str(root),
                "--database-path",
                str(explicit_path),
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    assert json.loads(captured.out) == expected
    assert captured.err == ""
    assert len(calls) == 1
    assert calls[0]["database_path"] == explicit_path.resolve()
