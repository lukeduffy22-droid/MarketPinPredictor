"""MCP server exposing the MarketPinPredictor research API to coding agents.

Bridges AI coding agents (Codex CLI, Claude Code, Hermes, any MCP client) to the
loopback research backend at http://127.0.0.1:8001 (tools/run_forecast_research.py).
Lets an agent ask for forecasts, trend metrics, and the tracked universe while it
works on the codebase, without handing it direct database access.

Run standalone (any MCP client can also spawn it over stdio):
    .venv\\Scripts\\python.exe tools/marketpin_mcp_server.py

Register with Codex CLI:
    codex mcp add marketpin -- C:/Cprojectsgpu_app/MarketPinPredictor/.venv/Scripts/python.exe C:/Cprojectsgpu_app/MarketPinPredictor/tools/marketpin_mcp_server.py

Environment:
    MARKETPIN_RESEARCH_URL   base URL of the research API (default http://127.0.0.1:8001)
    MARKETPIN_MCP_TIMEOUT    HTTP timeout seconds (default 8.0)
"""
from __future__ import annotations

import functools
import json
import os
import sys
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from mcp.server.mcpserver import MCPServer

RESEARCH_URL = os.environ.get("MARKETPIN_RESEARCH_URL", "http://127.0.0.1:8001").rstrip("/")
REQUEST_TIMEOUT = float(os.environ.get("MARKETPIN_MCP_TIMEOUT", "8.0"))

server = MCPServer("marketpin", instructions=(
    "MarketPinPredictor research backend. Tools return JSON from the loopback "
    "research API (schema marketpin-market-research.v1). If a call reports the API "
    "unreachable, tell the user to start it with "
    "`.venv\\Scripts\\python.exe tools/run_forecast_research.py`."
))


def _http_get(path: str, params: dict | None = None) -> str:
    """GET {RESEARCH_URL}{path}; raises RuntimeError with a clean message on failure."""
    from urllib.parse import urlencode
    url = f"{RESEARCH_URL}{path}"
    if params:
        url = f"{url}?{urlencode(params)}"
    try:
        with urlopen(url, timeout=REQUEST_TIMEOUT) as resp:
            return resp.read().decode("utf-8")
    except HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")[:300]
        raise RuntimeError(f"Research API {e.code} at {path}: {detail}") from e
    except URLError as e:
        raise RuntimeError(
            f"Research API unreachable at {RESEARCH_URL} — is "
            f"`python tools/run_forecast_research.py` running? ({e.reason})"
        ) from e


def _schema_guard(payload: dict) -> dict:
    """Enforce the schema contract the research API declares."""
    if payload.get("schema_version") != "marketpin-market-research.v1":
        raise RuntimeError(f"Unexpected schema_version: {payload.get('schema_version')!r}")
    return payload


def _safe(func):
    """Return readable error text instead of raising (keeps failures in-band)."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except RuntimeError as e:
            return f"Error: {e}"
        except Exception as e:  # noqa: BLE001 - boundary: never crash the stdio server
            return f"Error: unexpected failure: {e}"
    return wrapper


@server.tool()
@_safe
def get_forecasts(symbols: str = "", horizon_sessions: int = 0) -> str:
    """Get forecast research for tracked market symbols (SPX, NDX, RUT, VIX, SPY,
    QQQ, sector ETFs): per-symbol EOD research forecasts, directional shift state,
    and health reasons. Use horizon_sessions=0 for today's EOD, up to 10 farther out.

    Args:
        symbols: Comma-separated symbols (e.g. 'SPX,SPY'). Empty = all tracked.
        horizon_sessions: Forecast horizon in sessions, 0-10 (0 = EOD).
    """
    params = {}
    if symbols:
        params["symbols"] = symbols
    params["horizon_sessions"] = max(0, min(10, int(horizon_sessions)))
    payload = _schema_guard(json.loads(_http_get("/v1/research/forecasts", params)))
    return json.dumps(payload, indent=2)


@server.tool()
@_safe
def get_trend(symbol: str, lookback_days: int = 30) -> str:
    """Get trend metrics for one tracked symbol over a lookback window: direction
    (up/down/sideways), slope %, momentum score, and the official daily closes
    behind them. This is the authoritative trend contract — read the output before
    writing code that consumes it.

    Args:
        symbol: One tracked symbol, e.g. 'SPX'.
        lookback_days: Lookback window in calendar days, 1-250 (default 30).
    """
    symbol = (symbol or "").strip()
    if not symbol:
        return "Error: get_trend requires a symbol"
    payload = _schema_guard(json.loads(_http_get(
        "/v1/research/trend",
        {"symbol": symbol, "lookback_days": max(1, min(250, int(lookback_days)))})))
    return json.dumps(payload, indent=2)


@server.tool()
@_safe
def get_universe() -> str:
    """Get the tracked symbol universe and metadata: which symbols the research
    engine covers, plus equity context. Use this to discover valid symbol names
    before calling get_forecasts/get_trend."""
    return json.dumps(json.loads(_http_get("/v1/research/universe")), indent=2)


@server.tool()
@_safe
def get_research_health() -> str:
    """Check the research API's health: status (RESEARCH_ONLY/STALE/UNAVAILABLE),
    capture age, and per-symbol evidence reasons. Call this first if other tools
    fail or return abstentions."""
    return json.dumps(json.loads(_http_get("/health")), indent=2)


def main() -> None:
    import asyncio
    print(f"marketpin MCP server -> {RESEARCH_URL}", file=sys.stderr)
    asyncio.run(server.run_stdio_async())


if __name__ == "__main__":
    main()