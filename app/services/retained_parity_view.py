"""Strictly diagnostic rendering for retained OPRA parity and gamma-pin history."""

from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from app.utils.display_time import DisplayTimezone, display_timezone_label
from app.utils.opra_parity_history import (
    OPRA_PARITY_HISTORY_NOTICE,
    OpraParityGammaHistory,
    latest_contiguous_parity_reference,
)


def symbols_requiring_retained_context(
    selected_symbols: list[str],
    *,
    forecast_symbols: set[str],
    state_symbols: set[str],
    lifecycle_check_completed: bool,
) -> tuple[str, ...]:
    """Keep eligible forecasts while selecting only their unavailable peers."""

    return tuple(
        symbol
        for symbol in selected_symbols
        if symbol not in forecast_symbols
        and (symbol in state_symbols or lifecycle_check_completed)
    )


def build_retained_opra_parity_figure(
    history: OpraParityGammaHistory,
    display_timezone: DisplayTimezone,
) -> go.Figure:
    """Build separate traces for every contiguous epoch/generation segment."""

    figure = go.Figure()
    for segment_index, segment in enumerate(history.segments):
        timestamps = [
            point.timestamp_utc.astimezone(display_timezone.tzinfo)
            for point in segment.points
        ]
        identity = [
            [
                point.subscription_epoch_id,
                point.subscription_generation,
                point.calculation_id or "N/A",
            ]
            for point in segment.points
        ]
        figure.add_trace(
            go.Scatter(
                x=timestamps,
                y=[point.parity_spot for point in segment.points],
                mode="lines+markers",
                name="OPRA parity spot",
                legendgroup="parity_spot",
                showlegend=segment_index == 0,
                connectgaps=False,
                line={"color": "#38bdf8", "width": 2},
                marker={"size": 5},
                customdata=identity,
                hovertemplate=(
                    "Parity spot %{y:,.2f}<br>"
                    "Epoch %{customdata[0]} · generation %{customdata[1]}<br>"
                    "Calculation %{customdata[2]}<extra></extra>"
                ),
            )
        )
        figure.add_trace(
            go.Scatter(
                x=timestamps,
                y=[point.gamma_pin for point in segment.points],
                mode="lines+markers",
                name="Primary gamma pin",
                legendgroup="gamma_pin",
                showlegend=segment_index == 0,
                connectgaps=False,
                line={"color": "#f59e0b", "width": 2},
                marker={"size": 5},
                customdata=identity,
                hovertemplate=(
                    "Gamma pin %{y:,.2f}<br>"
                    "Epoch %{customdata[0]} · generation %{customdata[1]}<br>"
                    "Calculation %{customdata[2]}<extra></extra>"
                ),
            )
        )
    figure.update_layout(
        height=360,
        hovermode="x unified",
        margin={"l": 20, "r": 20, "t": 25, "b": 20},
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "left",
            "x": 0,
        },
        xaxis_title=f"Time ({display_timezone_label(display_timezone)})",
        yaxis_title="Index level / strike",
    )
    return figure


def render_retained_opra_parity_history(
    history: OpraParityGammaHistory,
    *,
    symbol: str,
    local_date: str,
    display_timezone: DisplayTimezone,
    key_prefix: str,
    expanded: bool = False,
) -> bool:
    """Render retained research context without creating a forecast surface."""

    if not history.segments:
        st.caption(
            "No valid identity-bearing stored OPRA parity snapshots are available "
            "for this date."
        )
        return False

    with st.expander(
        f"📈 OPRA Parity Spot / Gamma Pin ({history.included_records} samples)",
        expanded=expanded,
    ):
        st.error(
            "NON-PREDICTIVE RETAINED CONTEXT ONLY — this is not an end-of-day "
            "close estimate, target, forecast, confidence interval, or expected "
            "trading range."
        )
        st.caption(OPRA_PARITY_HISTORY_NOTICE)

        reference = latest_contiguous_parity_reference(history)
        if reference is not None:
            if reference.sample_count >= 2:
                st.markdown(
                    "**Latest contiguous OPRA parity observation envelope "
                    "(not a forecast):** "
                    f"${reference.range_low:,.2f} to ${reference.range_high:,.2f}"
                )
            else:
                st.markdown(
                    "**Latest retained OPRA parity reference only "
                    "(one point; no range):** "
                    f"${reference.reference_value:,.2f}"
                )
            st.caption(
                "Latest contiguous segment identity — "
                f"first UTC={reference.first_timestamp_utc.isoformat()} | "
                f"last UTC={reference.last_timestamp_utc.isoformat()} | "
                f"N={reference.sample_count} | "
                f"epoch={reference.subscription_epoch_id} | "
                f"generation={reference.subscription_generation}. "
                "Matching identities in older non-contiguous segments are not merged."
            )

        st.plotly_chart(
            build_retained_opra_parity_figure(history, display_timezone),
            width="stretch",
            key=f"{key_prefix}_{symbol}_{local_date}",
        )
        st.caption(
            f"{history.excluded_records} stored records were omitted because they "
            "were diagnostic, non-OPRA-parity, malformed, non-numeric, or lacked "
            "canonical epoch/generation identity."
        )
    return True
