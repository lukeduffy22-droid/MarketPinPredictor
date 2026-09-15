"""Loopback-only research API, runnable without restarting live capture."""
from contextlib import asynccontextmanager
from datetime import date, timedelta

from fastapi import FastAPI, HTTPException, Query


def create_research_app(service, *, manage_lifecycle=True):
    @asynccontextmanager
    async def lifespan(app):
        if manage_lifecycle:
            service.start()
        try:
            yield
        finally:
            if manage_lifecycle:
                service.stop()

    app = FastAPI(title="MarketPin Forecast Research", lifespan=lifespan)

    @app.get("/health")
    def health():
        state = service.snapshot()
        return {key: state.get(key) for key in
                ("schema_version", "status", "as_of_utc", "capture_age_seconds", "reasons")}

    @app.get("/v1/research/forecasts")
    def forecasts(symbols: str | None = None, horizon_sessions: int = Query(0, ge=0, le=10)):
        from backend.market_universe import normalize_market_symbol
        requested = tuple(dict.fromkeys(normalize_market_symbol(s) for s in symbols.split(","))) if symbols else service.symbols
        if not requested or len(requested) > 40 or any(s not in service.symbols for s in requested):
            raise HTTPException(422, "Request tracked symbols only (maximum 40)")
        state = service.snapshot()
        selected = {}
        for symbol in requested:
            item = state.get("symbols", {}).get(symbol, {})
            selected[symbol] = {k: v for k, v in item.items() if k != "forecasts"}
            selected[symbol]["forecast"] = item.get("forecasts", {}).get(str(horizon_sessions),
                {"status": "ABSTAIN", "predicted_close": None, "reasons": ["RESEARCH_WARMING"]})
        return {**state, "horizon_sessions": horizon_sessions, "requested_symbols": requested, "symbols": selected}

    @app.get("/v1/research/universe")
    def universe():
        from backend.market_universe import market_universe_catalog
        return {"tracked_symbols": service.symbols, "catalog": market_universe_catalog(),
                "equity_context": service.snapshot().get("equity_context")}

    # --- Trend research ---------------------------------------------------
    # Serves app/services/trend_research_view.py (and any other loopback
    # consumer). Reads official closes straight from the source database —
    # loopback-only, no Databento dependency, same contract style as /forecasts.

    @app.get("/v1/research/trend")
    def trend(symbol: str, lookback_days: int = Query(30, ge=1, le=250)):
        """Direction/slope/momentum for one tracked symbol over the lookback window.

        Reads official closes from ``eod_closes`` (append-only close evidence)
        in the service's source database. Response shape: ``trend.prices`` as
        ``[{ts, close}, ...]`` plus a precomputed ``trend.metrics`` block
        (trend_direction, slope_pct, momentum_score) so consumers don't have
        to recompute them.
        """
        from backend.market_universe import normalize_market_symbol

        normalized = normalize_market_symbol(symbol)
        if normalized not in service.symbols:
            raise HTTPException(422, "Request tracked symbols only")

        source = getattr(service, "source_database", None)
        if not source:
            raise HTTPException(503, "Source database not available")

        cutoff = date.today() - timedelta(days=lookback_days)
        try:
            import sqlite3
            con = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
            try:
                rows = con.execute(
                    "SELECT trading_date, official_close FROM eod_closes "
                    "WHERE symbol = ? AND trading_date >= ? "
                    "AND source NOT LIKE '%unit-test' "
                    "ORDER BY trading_date",
                    (normalized, cutoff.isoformat()),
                ).fetchall()
            finally:
                con.close()
        except sqlite3.Error:
            raise HTTPException(503, "Close evidence unavailable")

        if len(rows) < 2:
            raise HTTPException(422, f"Insufficient close evidence for {normalized} "
                                     f"({len(rows)} session(s) in window)")

        prices = [{"ts": r[0].isoformat() if hasattr(r[0], "isoformat") else str(r[0]),
                   "close": float(r[1])} for r in rows]
        first_close, last_close = prices[0]["close"], prices[-1]["close"]
        slope_pct = (last_close - first_close) / first_close * 100.0

        # Momentum: mean of session-over-session pct changes, normalized to [-1, 1].
        changes = [(prices[i]["close"] - prices[i - 1]["close"]) / prices[i - 1]["close"]
                   for i in range(1, len(prices))]
        momentum_score = max(-1.0, min(1.0, (sum(changes) / len(changes)) * 100.0 / 2.0))

        return {
            "schema_version": "marketpin-market-research.v1",
            "symbol": normalized,
            "lookback_days": lookback_days,
            "sessions": len(prices),
            "window": {"from": prices[0]["ts"], "to": prices[-1]["ts"]},
            "trend": {
                "prices": prices,
                "metrics": {
                    "trend_direction": "up" if slope_pct > 5 else "down" if slope_pct < -5 else "sideways",
                    "slope_pct": round(slope_pct, 4),
                    "momentum_score": round(momentum_score, 4),
                },
            },
        }

    return app