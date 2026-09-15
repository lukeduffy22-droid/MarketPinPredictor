from datetime import date, datetime
from types import SimpleNamespace

import gamma_viz
import pandas as pd
from app.utils.display_time import resolve_display_timezone


def _snapshot(**overrides):
    values = {
        "interval_timestamp": datetime(2026, 9, 1, 15, 30),
        "pin_strike": 6500.0,
        "spot_price": 6495.0,
        "total_gex": 10.0,
        "net_gex": 2.0,
        "pull_strength": 0.5,
        "is_mock_data": False,
        "is_valid": True,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_gamma_chart_interprets_naive_sqlite_timestamp_as_utc_then_displays_central(
    monkeypatch,
):
    snapshot = _snapshot()
    monkeypatch.setattr(
        gamma_viz,
        "get_gamma_snapshots_for_day",
        lambda *_args: [snapshot],
    )

    figure = gamma_viz.create_gamma_evolution_chart(
        "SPX",
        date(2026, 9, 1),
        resolve_display_timezone("America/Chicago"),
    )

    assert figure is not None
    plotted_time = pd.Timestamp(figure.data[0].x[0])
    assert plotted_time.hour == 10
    assert figure.layout.xaxis.title.text == "Time (America/Chicago)"


def test_explicit_valid_history_uses_persisted_snapshot_labels_not_real_or_live(
    monkeypatch,
):
    monkeypatch.setattr(
        gamma_viz,
        "get_gamma_snapshots_for_day",
        lambda *_args: [_snapshot()],
    )

    figure = gamma_viz.create_gamma_evolution_chart(
        "SPX",
        date(2026, 9, 1),
        resolve_display_timezone("America/Chicago"),
    )

    trace_names = [trace.name for trace in figure.data]
    assert trace_names == [
        "Gamma Pin — Persisted valid snapshot",
        "Spot — Persisted valid snapshot",
    ]
    assert "Captured History" in figure.layout.title.text
    assert all("Real" not in name and "Live" not in name for name in trace_names)


def test_history_caption_surfaces_last_persisted_observation_and_live_boundary():
    viewer_timezone = resolve_display_timezone("America/Chicago")
    snapshots = [
        _snapshot(interval_timestamp=datetime(2026, 9, 1, 15, 15)),
        _snapshot(interval_timestamp=datetime(2026, 9, 1, 15, 30)),
    ]

    caption = gamma_viz._history_as_of_caption(snapshots, viewer_timezone)

    assert "Captured history only" in caption
    assert "current live state is unavailable" in caption
    assert "Last persisted observation: Sep 01, 2026 at 10:30:00 AM CDT" in caption
    assert "explicit persisted-valid provenance" in caption


def test_legacy_row_without_validity_is_visible_only_as_unverified_history():
    legacy = _snapshot()
    del legacy.is_valid

    figure = gamma_viz.create_gamma_evolution_chart(
        "SPX",
        date(2026, 9, 1),
        resolve_display_timezone("America/Chicago"),
        snapshots=[legacy],
    )

    assert gamma_viz._snapshot_provenance(legacy) == gamma_viz.LEGACY_UNVERIFIED
    assert [trace.name for trace in figure.data] == [
        "Gamma Pin — Legacy unverified history",
        "Spot — Legacy unverified history",
    ]
    caption = gamma_viz._history_as_of_caption(
        [legacy],
        resolve_display_timezone("America/Chicago"),
    )
    assert "without explicit validity provenance" in caption
    assert "persisted-valid provenance" not in caption


def test_explicit_invalid_snapshot_is_excluded_from_history():
    invalid = _snapshot(is_valid=False)

    assert gamma_viz.create_gamma_evolution_chart(
        "SPX",
        date(2026, 9, 1),
        resolve_display_timezone("America/Chicago"),
        snapshots=[invalid],
    ) is None
