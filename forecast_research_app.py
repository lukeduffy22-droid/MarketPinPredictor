"""MarketPin research companion; shares the production dashboard's controls."""
import streamlit as st

from app.services.forecast_research_view import render_forecast_research

st.set_page_config(page_title="MarketPin Forecast Research", layout="wide")
st.title("MarketPin Forecast Research")
st.write("Compare EOD candidates, select future trading sessions, and inspect directional-shift evidence.")
st.caption("Live capture continues in the main MarketPin application. This page reads a separate research service.")
render_forecast_research()
