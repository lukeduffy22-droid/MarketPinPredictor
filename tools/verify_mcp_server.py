"""Verify tools/marketpin_mcp_server.py speaks correct MCP over stdio.

Spawns the research API on a spare port and the MCP server, performs the full
JSON-RPC handshake, calls every tool, checks error paths, then shuts both down.
Run: .venv/Scripts/python.exe tools/verify_mcp_server.py
"""
import json
import os
import subprocess
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VENV_PY = os.path.join(ROOT, ".venv", "Scripts", "python.exe")
PORT = "8511"


def wait_http(url, timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2):
                return True
        except Exception:
            time.sleep(0.5)
    return False


def main():
    failures = []

    # 1. research API
    api = subprocess.Popen([VENV_PY, "tools/run_forecast_research.py", "--port", PORT],
                            cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        if not wait_http(f"http://127.0.0.1:{PORT}/health"):
            print("FAIL: research API did not come up")
            sys.exit(1)
        print("research API: up on", PORT)

        env = {**os.environ, "MARKETPIN_RESEARCH_URL": f"http://127.0.0.1:{PORT}"}
        mcp = subprocess.Popen([VENV_PY, "tools/marketpin_mcp_server.py"], cwd=ROOT, env=env,
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True, encoding="utf-8")

        def send(obj):
            mcp.stdin.write(json.dumps(obj) + "\n")
            mcp.stdin.flush()

        def recv(timeout=30):
            # readline on the pipe with a watchdog; mcp stdout is line-delimited JSON-RPC
            import threading
            holder = {}
            def rd():
                holder["line"] = mcp.stdout.readline()
            t = threading.Thread(target=rd, daemon=True)
            t.start()
            t.join(timeout)
            if "line" not in holder:
                return None
            line = holder["line"]
            return json.loads(line) if line.strip() else None

        try:
            # 2. handshake — echo the protocol version the SDK advertises
            send({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                  "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                             "clientInfo": {"name": "verify", "version": "0.1"}}})
            init = recv()
            if not init or "result" not in init:
                print("FAIL: no initialize result:", init)
                sys.exit(1)
            negotiated = init["result"].get("protocolVersion")
            print("initialize: ok, server", init["result"]["serverInfo"]["name"],
                  "| protocol", negotiated)
            send({"jsonrpc": "2.0", "method": "notifications/initialized"})

            # 3. tools/list
            send({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
            tools = recv()
            names = [t["name"] for t in tools["result"]["tools"]]
            expected = {"get_forecasts", "get_trend", "get_universe", "get_research_health"}
            print("tools/list:", names)
            if expected != set(names):
                failures.append(f"tool set mismatch: {names}")

            def call(name, args, msg_id):
                send({"jsonrpc": "2.0", "id": msg_id, "method": "tools/call",
                      "params": {"name": name, "arguments": args}})
                return recv()

            # 4. get_trend happy path
            r = call("get_trend", {"symbol": "SPX", "lookback_days": 30}, 3)
            data = json.loads(r["result"]["content"][0]["text"])
            print("get_trend SPX:", data["trend"]["metrics"])
            if data["schema_version"] != "marketpin-market-research.v1":
                failures.append("get_trend: wrong schema_version")

            # 5. get_forecasts
            r = call("get_forecasts", {"symbols": "SPX", "horizon_sessions": 0}, 4)
            fc = json.loads(r["result"]["content"][0]["text"])
            status = fc["symbols"]["SPX"]["forecast"]["status"]
            print("get_forecasts SPX status:", status)

            # 6. get_universe
            r = call("get_universe", {}, 5)
            uni = json.loads(r["result"]["content"][0]["text"])
            print("get_universe:", len(uni["tracked_symbols"]), "tracked symbols")

            # 7. get_research_health
            r = call("get_research_health", {}, 6)
            print("get_research_health:", json.loads(r["result"]["content"][0]["text"])["status"])

            # 8. error path: untracked symbol
            r = call("get_trend", {"symbol": "ZZZZ"}, 7)
            txt = r["result"]["content"][0]["text"]
            ok = txt.startswith("Error:") and "422" in txt
            print("untracked symbol -> clean error:", ok)
            if not ok:
                failures.append(f"bad-symbol error path: {txt[:100]}")

            # 9. error path: API down — kill research API, expect unreachable message
            api.terminate()
            api.wait(timeout=15)
            r = call("get_trend", {"symbol": "SPX"}, 8)
            txt = r["result"]["content"][0]["text"]
            ok = txt.startswith("Error:") and "unreachable" in txt
            print("API-down -> clean error:", ok)
            if not ok:
                failures.append(f"API-down error path: {txt[:100]}")

        finally:
            try:
                mcp.stdin.close()
                mcp.wait(timeout=10)
            except Exception:
                mcp.kill()
    finally:
        if api.poll() is None:
            api.terminate()
            try:
                api.wait(timeout=15)
            except Exception:
                api.kill()

    print()
    if failures:
        print("FAILURES:")
        for f in failures:
            print(" -", f)
        sys.exit(1)
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    main()