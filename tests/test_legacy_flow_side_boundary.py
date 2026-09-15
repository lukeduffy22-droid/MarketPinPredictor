import time
from datetime import date

import pytest

from app.features.calculators import calc_flow_urgency
from app.ingest.websocket_aggregator import parse_options_trade
from app.state.ring_buffers import FLOW_RINGS, OptTrade


def test_polygon_trade_without_side_evidence_remains_unknown():
    parsed = parse_options_trade(
        {
            "ev": "T",
            "sym": "O:SPX260825C06500000",
            "p": 10.0,
            "s": 2,
            "t": 1_787_653_800_000,
        }
    )

    assert parsed is not None
    _root, trade = parsed
    assert trade.aggressor == 0


def test_unknown_side_flow_cannot_create_directional_urgency():
    ring = FLOW_RINGS["SPX"]
    ring.q.clear()
    try:
        now = int(time.time())
        ring.add(
            now,
            OptTrade(
                ts=now, root="SPX", K=6500, is_call=True,
                exp=date(2026, 8, 25), notional=50_000_000.0, aggressor=0,
            ),
        )

        assert calc_flow_urgency("SPX") == pytest.approx(0.35)
    finally:
        ring.q.clear()
