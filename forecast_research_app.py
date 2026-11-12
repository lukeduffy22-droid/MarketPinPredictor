"""MarketPin research companion; shares the production dashboard's controls."""
import streamlit as st

from app.services.forecast_research_view import render_forecast_research
from app.services.live_api_demo import render_live_api_demo

st.set_page_config(page_title="MarketPin Forecast Research", layout="wide")
st.title("MarketPin Forecast Research")
st.write("Compare EOD candidates, select future trading sessions, and inspect directional-shift evidence.")
st.caption("Live capture continues in the main MarketPin application. Research estimates normally share the managed research service; each symbol has its own current eligibility check.")
render_live_api_demo()
render_forecast_research()
