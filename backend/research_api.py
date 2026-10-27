"""Loopback-only research API, runnable without restarting live capture."""
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone

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
                ("schema_version", "health_schema_version", "component", "status", "as_of_utc",
                 "capture_age_seconds", "capture_interval_seconds", "reasons", "process_alive",
                 "process_id", "instance_id", "started_at_utc", "worker", "loaded_source_sha256",
                 "source_identity_sha256", "source_database_access", "indicator_capture")}

    @app.get("/v1/research/forecasts")
    def forecasts(symbols: str | None = Query(None, max_length=400), horizon_sessions: int = Query(0, ge=0, le=10)):
        from backend.market_universe import normalize_market_symbol
        from backend.forecast_horizons import evaluate_forecast
        from backend.forecast_publication_gate import evaluate_publication_gate, project_publication_item
        requested = tuple(dict.fromkeys(normalize_market_symbol(s) for s in symbols.split(","))) if symbols is not None else service.symbols
        if not requested or len(requested) > 40 or any(s not in service.symbols for s in requested):
            raise HTTPException(422, "Request tracked symbols only (maximum 40)")
        state = service.snapshot()
        selected = {}
        for symbol in requested:
            item = state.get("symbols", {}).get(symbol, {})
            selected[symbol] = {k: v for k, v in item.items() if k not in ("forecasts", "retained_forecasts", "history_by_horizon")}
            forecast = item.get("forecasts", {}).get(str(horizon_sessions))
            if forecast is None:
                forecast = evaluate_forecast(symbol, [], as_of_utc=datetime.now(timezone.utc),
                                             horizon_sessions=horizon_sessions)
                forecast["reasons"] = list(dict.fromkeys([*state.get("reasons", []), "RESEARCH_WARMING"]))
            selected[symbol]["forecast"] = forecast
            selected[symbol]["retained_forecast"] = item.get("retained_forecasts", {}).get(str(horizon_sessions), forecast)
            if horizon_sessions != 0:
                selected[symbol].pop("eod_learning", None)
            selected[symbol]["history_availability"] = item.get("history_by_horizon", {}).get(str(horizon_sessions),
                {"status": "UNKNOWN", "qualified_return_samples": None,
                 "required_return_samples": 60 if horizon_sessions else None, "reasons": ["RESEARCH_WARMING"]})
        # Exactly one bounded loopback health read for the whole request. GET
        # never collects forecasts, writes journals, or contacts a market provider.
        try:
            current = service.context_reader()
        except Exception:
            current = {}
        gate = evaluate_publication_gate({s: item["forecast"] for s, item in selected.items()}, current,
                                         checked_at_utc=datetime.now(timezone.utc))
        selected = {s: project_publication_item(item, item["forecast"], item["retained_forecast"], gate["symbols"][s])
                    for s, item in selected.items()}
        return {**state, "horizon_sessions": horizon_sessions, "requested_symbols": requested,
                "symbols": selected, "publication_gate": gate}

    @app.get("/v1/research/attempts")
    def attempts(limit: int = Query(20, ge=1, le=50)):
        from backend.forecast_attempt_journal import read_attempts
        return read_attempts(service.journal_database, limit=limit)

    @app.get("/v1/research/universe")
    def universe():
        from backend.market_universe import market_universe_catalog
        return {"tracked_symbols": service.symbols, "catalog": market_universe_catalog(),
                "equity_context": service.snapshot().get("equity_context")}

    @app.get("/v1/research/indicator-bars")
    def indicator_bars(symbol: str = Query(..., max_length=12)):
        from backend.indicator_bars import read_indicator_bars
        from backend.market_universe import normalize_market_symbol
        from pathlib import Path
        normalized = normalize_market_symbol(symbol)
        if normalized not in service.symbols:
            raise HTTPException(422, "Request tracked symbols only")
        # Read the existing collector's retained store. A GET never creates or
        # starts a collector, contacts a provider, or changes a subscription.
        result = read_indicator_bars(Path(service.source_database).parent / "equity_context.db", normalized)
        tracker = getattr(service, "indicator_tracker", None)
        if tracker is not None:
            capture = tracker.snapshot()
            result["capture"] = {k: capture.get(k) for k in
                ("enabled", "running", "status", "last_attempt_utc", "retry_after_utc", "accepted_bar_count")}
            if result["status"] != "available" and capture.get("status") in (
                    "ENTITLEMENT_UNAVAILABLE", "EQUITY_CONTEXT_FETCH_FAILED",
                    "INDICATOR_CAPTURE_WRITER_UNAVAILABLE", "INDICATOR_CAPTURE_START_FAILED"):
                result["bar_reason_code"] = result["reason_code"]
                result["reason_code"] = capture["status"]
        return result

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
