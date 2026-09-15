from contextlib import nullcontext

from app.services import retained_parity_view
from app.utils.display_time import resolve_display_timezone
from app.utils.opra_parity_history import build_opra_parity_gamma_history


def _record(timestamp: str, *, epoch: str, generation: int, spot: float) -> dict:
    return {
        "generated_at_utc": timestamp,
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "spot_source": "databento_opra_put_call_parity",
        "spot_last": spot,
        "primary_gamma_pin_strike": spot + 5,
        "subscription_epoch_id": epoch,
        "subscription_generation": generation,
        "calculation_id": f"calc-{timestamp}",
    }


class _RecordingStreamlit:
    def __init__(self):
        self.errors = []
        self.markdowns = []
        self.captions = []
        self.figures = []

    def expander(self, *_args, **_kwargs):
        return nullcontext()

    def error(self, value):
        self.errors.append(value)

    def markdown(self, value):
        self.markdowns.append(value)

    def caption(self, value):
        self.captions.append(value)

    def plotly_chart(self, figure, **kwargs):
        self.figures.append((figure, kwargs))


def test_renderer_labels_latest_segment_envelope_as_non_predictive(monkeypatch):
    epoch_a = "a" * 64
    epoch_b = "b" * 64
    history = build_opra_parity_gamma_history(
        [
            _record("2026-09-09T13:30:00Z", epoch=epoch_a, generation=1, spot=90),
            _record("2026-09-09T13:31:00Z", epoch=epoch_b, generation=3, spot=101),
            _record("2026-09-09T13:32:00Z", epoch=epoch_b, generation=3, spot=103),
        ]
    )
    ui = _RecordingStreamlit()
    monkeypatch.setattr(retained_parity_view, "st", ui)

    rendered = retained_parity_view.render_retained_opra_parity_history(
        history,
        symbol="SPX",
        local_date="2026-09-09",
        display_timezone=resolve_display_timezone("UTC"),
        key_prefix="test",
        expanded=True,
    )

    assert rendered is True
    assert "NON-PREDICTIVE RETAINED CONTEXT ONLY" in ui.errors[0]
    assert "not an end-of-day close estimate" in ui.errors[0]
    assert "$101.00 to $103.00" in ui.markdowns[0]
    lineage = next(value for value in ui.captions if "N=2" in value)
    assert "first UTC=2026-09-09T13:31:00+00:00" in lineage
    assert "last UTC=2026-09-09T13:32:00+00:00" in lineage
    assert f"epoch={epoch_b}" in lineage
    assert "generation=3" in lineage
    assert "not merged" in lineage

    figure = ui.figures[0][0]
    assert len(figure.data) == 4
    assert all(trace.connectgaps is False for trace in figure.data)


def test_renderer_calls_single_point_a_reference_and_not_a_range(monkeypatch):
    history = build_opra_parity_gamma_history(
        [_record("2026-09-09T13:30:00Z", epoch="c" * 64, generation=4, spot=88)]
    )
    ui = _RecordingStreamlit()
    monkeypatch.setattr(retained_parity_view, "st", ui)

    retained_parity_view.render_retained_opra_parity_history(
        history,
        symbol="NDX",
        local_date="2026-09-09",
        display_timezone=resolve_display_timezone("UTC"),
        key_prefix="test",
    )

    assert "reference only" in ui.markdowns[0].lower()
    assert "one point; no range" in ui.markdowns[0].lower()
    assert "$88.00" in ui.markdowns[0]


def test_context_symbol_selection_preserves_partial_forecast_availability():
    selected = retained_parity_view.symbols_requiring_retained_context(
        ["SPX", "NDX"],
        forecast_symbols={"SPX"},
        state_symbols={"SPX", "NDX"},
        lifecycle_check_completed=True,
    )

    assert selected == ("NDX",)
