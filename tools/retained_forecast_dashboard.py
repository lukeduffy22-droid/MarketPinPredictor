"""Read-only companion view for saved same-session EOD research forecasts."""

from pathlib import Path
import os
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# This companion reads this checkout's known market ledger by default. Set the
# absolute default before backend configuration can load a legacy relative .env.
os.environ.setdefault("DATABASE_URL", f"sqlite:///{ROOT / 'data' / 'market_data.db'}")

import requests
import streamlit as st

from app.services.retained_forecasts import render_retained_forecasts

st.set_page_config(page_title="Saved EOD Forecasts", layout="centered")
st.title("Saved EOD Forecasts")
st.caption("SPX and NDX · Main GEX research estimates · Read-only history")
if st.button("Refresh forecasts and age", type="primary"):
    st.rerun()
try:
    response = requests.get("http://127.0.0.1:8000/health/live", timeout=1.5)
    response.raise_for_status()
    pipeline = response.json()
except (requests.RequestException, ValueError):
    pipeline = {}
render_retained_forecasts(pipeline)
st.caption("Automatic refresh is off. This view reads saved predictions and does not generate forecasts or change live validation.")
