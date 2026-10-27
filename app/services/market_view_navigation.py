"""Explicit view selection; inactive Streamlit panels perform no reads."""

MARKET_VIEWS = ("Live Streaming", "Advisor", "Shadow Research", "Frozen Gamma (Audit)", "Debug Snapshot")


def select_market_view():
    import streamlit as st
    return st.radio("Market data view", MARKET_VIEWS, key="market_data_view",
                    help="Only the selected panel loads its detail. The shared session status is checked on each page update.")
