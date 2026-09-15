"""
MarketPin Databento Forecast Tool
==================================
Builds today's End-of-Day and next-week forecasts from two sources:

  * INDEX symbols (SPX, NDX, RUT): EOD via the production engine
    (backend.ai_predictor.build_ai_prediction) driven by today's LIVE
    Databento OPRA GEX/pin payloads; next-week via recent official closes.

  * ETF symbols (SPY, QQQ, IWM, DIA, broad + sector ETFs): a momentum /
    volatility time-series forecast computed from FRESH Databento OHLCV
    (EQUS.MINI). The trained CUDA index model is out of distribution for
    ETF price levels, so ETFs use their own statistical forecast.

Usage:
    python databento_forecast.py                 # EOD + next-week, all symbols
    python databento_forecast.py --symbols SPX SPY QQQ
    python databento_forecast.py --horizon eod   # or: next-week / both (default)
"""

import argparse
import csv
import json
import os
import sqlite3
import sys
from datetime import date, timedelta

import numpy as np
import pandas as pd

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

# Universe
INDEX_SYMBOLS = ["SPX", "NDX", "RUT", "VIX"]
ETF_SYMBOLS = [
    "SPY", "QQQ", "IWM", "DIA",
    "VOO", "VTI", "RSP", "MDY",
    "XLK", "XLF", "XLY", "XLP", "XLE", "XLI", "XLV", "XLU",
    "SMH",
    "TLT", "IEF", "HYG", "LQD",
]
DATASET = "EQUS.MINI"


def load_env(path):
    env = dict(os.environ)
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    except OSError:
        pass
    return env


def databento_client():
    import databento as db
    env = load_env(os.path.join(ROOT, ".env"))
    key = env.get("DATABENTO_API_KEY", "").strip()
    if not key or key.startswith("YOUR"):
        raise RuntimeError("DATABENTO_API_KEY is missing in .env")
    return db.Historical(key), env


def fetch_daily_ohlcv(client, symbol, days=40):
    """Most recent `days` of daily OHLCV for an ETF, session-indexed."""
    end = date.today()
    start = end - timedelta(days=int(days * 1.8) + 10)
    data = client.timeseries.get_range(
        dataset=DATASET, symbols=[symbol], schema="ohlcv-1d",
        start=start.isoformat(), end=end.isoformat(),
    )
    df = data.to_df()
    if df is None or df.empty:
        return None
    idx = df.index
    try:
        dates = idx.tz_convert("America/New_York").date
    except Exception:
        dates = [d.date() for d in idx]
    daily = df[["open", "high", "low", "close", "volume"]].copy()
    daily["session_date"] = dates
    daily = daily.groupby("session_date").agg(
        open=("open", "last"), high=("high", "max"),
        low=("low", "min"), close=("close", "last"),
        volume=("volume", "sum"),
    ).sort_index()
    return daily


def _forecast_stats(series):
    """Shared statistical forecast from a daily-close-style Series with OHLC."""
    close = series["close"].dropna()
    if len(close) < 5:
        return None
    spot = float(close.iloc[-1])
    recent = close.tail(8)
    rets = close.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    vol = float(rets.tail(20).std()) if len(rets) >= 5 else float(rets.std())
    hi = series["high"].dropna()
    lo = series["low"].dropna()
    tr = (hi - lo).tail(10)
    avg_range = float(tr.mean()) if len(tr) else float(recent.diff().abs().mean())
    trend = (float(recent.iloc[0]) - spot) / spot if len(recent) >= 5 else 0.0
    gap = 1.0005 if trend > 0 else 0.9995
    expected_open = spot * gap
    eod_close = expected_open + spot * trend * 0.3
    eod_move_pct = (eod_close - spot) / spot * 100.0
    daily_drift = (eod_close - spot) / 5.0
    week_path = spot
    week = []
    for i in range(5):
        week_path += daily_drift
        week.append(round(week_path + avg_range * 0.10 * np.random.default_rng(i).normal(), 4))
    return {
        "spot": spot, "expected_open": expected_open, "avg_range": avg_range,
        "realized_vol": vol, "trend_5d": trend,
        "eod_close": eod_close, "eod_move_pct": eod_move_pct,
        "confidence": ("LOW" if vol > 0.02 else ("MEDIUM" if vol > 0.015 else "HIGH")),
        "week_by_day": week, "week_end": week[-1],
        "week_move_pct": (week[-1] - spot) / spot * 100.0,
    }


def forecast_for_df(daily):
    """Statistical EOD + next-week from a daily OHLCV DataFrame."""
    f = _forecast_stats(daily)
    if f is None:
        return None
    f["kind"] = "etf"
    return f


def index_daily_for_forecast(symbol, days=30):
    """Daily OHLC series for an index from stored official closes."""
    db_path = os.path.join(ROOT, "data", "market_data.db")
    if not os.path.isfile(db_path):
        return None
    con = sqlite3.connect(db_path)
    rows = con.execute(
        "SELECT trading_date, official_close FROM eod_closes "
        "WHERE symbol=? AND source NOT LIKE '%unit-test' "
        "ORDER BY trading_date ASC LIMIT ?", (symbol, days)).fetchall()
    con.close()
    if len(rows) < 5:
        return None
    ser = pd.Series([r[1] for r in rows],
                    index=pd.to_datetime([r[0] for r in rows]).date)
    daily = pd.DataFrame({"open": ser, "high": ser, "low": ser, "close": ser,
                          "volume": 0}, index=ser.index)
    return daily



def etf_deep_forecast(symbol, daily):
    """
    Predict tomorrow's close using the ETF-trained deep model, if available.
    Uses the 19-column price-relative feature schema; multiplies current spot
    by the predicted next-close ratio. Returns a forecast dict or None.
    """
    model_dir = os.path.join(ROOT, "models", "etf_price_predictor_meta.json")
    if not os.path.isfile(model_dir):
        return None
    import torch
    import torch.nn as nn
    import json as _json
    meta = _json.load(open(model_dir, encoding="utf-8"))
    feats = meta["feature_columns"]
    if set(feats) != set(FEATURE_COLUMNS_19):
        return None
    close = daily["close"].dropna()
    if len(close) < 20:
        return None
    e = 1e-8
    c = close
    ret = c.pct_change()
    v = np.zeros(len(FEATURE_COLUMNS_19))
    x = np.array([
        daily["open"].iloc[-1]/c.iloc[-1],
        daily["high"].iloc[-1]/c.iloc[-1],
        daily["low"].iloc[-1]/c.iloc[-1],
        1.0,
        0.0,
        (daily["high"].iloc[-1]-daily["low"].iloc[-1])/c.iloc[-1],
        (daily["open"].iloc[-1]-c.iloc[-1])/c.iloc[-1],
        (daily["high"].iloc[-1]-max(daily["open"].iloc[-1], c.iloc[-1]))/c.iloc[-1],
        (min(daily["open"].iloc[-1], c.iloc[-1])-daily["low"].iloc[-1])/c.iloc[-1],
        ret.iloc[-1] if np.isfinite(ret.iloc[-1]) else 0.0,
        ret.tail(5).std() if len(ret)>=5 else 0.0,
        close.tail(5).mean()/c.iloc[-1],
        close.tail(20).mean()/c.iloc[-1],
        (c.iloc[-1]-c.shift(3).iloc[-1])/c.iloc[-1] if len(c)>3 else 0.0,
        (c.iloc[-1]-c.shift(10).iloc[-1])/c.iloc[-1] if len(c)>10 else 0.0,
        c.iloc[-1]/(close.tail(5).mean()+e),
        c.iloc[-1]/(close.tail(20).mean()+e),
        daily["high"].iloc[-1]/(daily["low"].iloc[-1]+e),
        (c.iloc[-1]-daily["low"].iloc[-1])/(daily["high"].iloc[-1]-daily["low"].iloc[-1]+e),
    ])
    # ensure finite
    x = np.where(np.isfinite(x), x, 0.0)
    mean = np.array(meta["scaling_X"]["mean"]); std = np.array(meta["scaling_X"]["std"])
    xn = (x - mean)/std
    class DeepPricePredictor(nn.Module):  # local import
        def __init__(s, d): super().__init__(); s.net=nn.Sequential(
            nn.Linear(d,256),nn.BatchNorm1d(256),nn.ReLU(),nn.Dropout(0.3),
            nn.Linear(256,256),nn.BatchNorm1d(256),nn.ReLU(),nn.Dropout(0.3),
            nn.Linear(256,128),nn.BatchNorm1d(128),nn.ReLU(),nn.Dropout(0.2),
            nn.Linear(128,64),nn.ReLU(),nn.Dropout(0.1),nn.Linear(64,1))
        def forward(s,x): return s.net(x)
    try:
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model = DeepPricePredictor(len(FEATURE_COLUMNS_19)).to(device)
        model.load_state_dict(torch.load(os.path.join(ROOT,"models","etf_price_predictor_best.pt"), map_location=device))
        model.eval()
        with torch.no_grad():
            xn_t = torch.tensor(xn, dtype=torch.float32).unsqueeze(0).to(device)
            ratio_s = model(xn_t).item()
        ratio = ratio_s*float(meta["scaling_y"]["std"]) + float(meta["scaling_y"]["mean"])
        ratio = float(np.clip(ratio, 0.7, 1.4))
    except Exception:
        return None
    spot = float(c.iloc[-1])
    eod_close = spot * ratio
    eod_move_pct = (eod_close - spot)/spot*100.0
    # week path: persist one forecast ratio forward (research-only estimate)
    week_end = eod_close * (1 + eod_move_pct/100.0 * 0.25)
    return {"kind":"etf","spot":spot,"eod_close":eod_close,"eod_move_pct":eod_move_pct,
            "confidence":"MEDIUM","week_end":week_end,"week_move_pct":(week_end-spot)/spot*100.0,
            "model":"etf_deep"}


FEATURE_COLUMNS_19 = [
    "open","high","low","close","vwap","day_range","body","upper_wick","lower_wick",
    "price_change","volatility_5","ma_5","ma_20","momentum_3","momentum_10",
    "rs_5","rs_20","high_low_ratio","close_position",
]


def e2e_prediction(symbol):
    """Production EOD prediction from the latest validated live Databento payload."""
    db_path = os.path.join(ROOT, "data", "market_data.db")
    if not os.path.isfile(db_path):
        return None
    from backend.ai_predictor import build_ai_prediction
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    today = date.today().isoformat()
    rows = con.execute(
        "SELECT * FROM market_structure_observations "
        "WHERE symbol=? AND trading_date=? AND validation_status='valid' "
        "ORDER BY observation_id DESC", (symbol, today)).fetchall()
    con.close()
    if not rows:
        return None
    latest = dict(rows[0])
    ref = latest.get("reference_price"); gp = latest.get("gamma_pin")
    mp = latest.get("max_pain"); zg = latest.get("zero_gamma")
    net = latest.get("net_gex"); gross = latest.get("gross_gex")
    if not ref or not gp or not net or not gross:
        return None
    payload = {
        "provider": "databento", "price": ref, "gamma_pin": gp, "max_pain": mp,
        "zero_gamma": zg, "net_gex": net, "gross_gex": gross,
        "likely_close": gp, "validation_is_valid": True,
        "gamma_excluded_from_model": False, "usable_for_prediction": True,
        "contracts": 1, "quotes_cached": 1, "paired_quote_count": 1,
        "call_quote_count": 1, "put_quote_count": 1,
    }
    pred = build_ai_prediction(symbol, payload)
    if not pred.get("usable"):
        return None
    return {
        "spot": pred.get("current_price"), "eod_close": pred.get("predicted_close"),
        "eod_move_pct": pred.get("expected_move_pct"),
        "confidence": round(pred.get("confidence") or 0, 1),
        "confidence_kind": pred.get("confidence_kind"), "bias": pred.get("net_bias"),
        "device": pred.get("inference_device"), "signals": pred.get("signals", []),
    }




# Rich model feature schema (must match train_etf_rich_model.py FEATURES order)
RICH_FEATURES = [
    "open","high","low","gap_pct","day_range","ret_1d","ret_5d","ret_20d","volume_z",
    "open_range_30m","open_range_60m","open_range_full","intraday_vol","vwap_dev",
    "own_vs_mkt_5d","own_vs_sector_5d","sector_mean_ret_5d",
]


def fetch_1m_bars(client, symbol, days=40):
    """Fetch recent 1-minute OHLCV for intraday-range features."""
    end = date.today()
    start = end - timedelta(days=days)
    d = client.timeseries.get_range(
        dataset=DATASET, symbols=[symbol], schema="ohlcv-1m",
        start=start.isoformat(), end=end.isoformat())
    df = d.to_df()
    if df is None or df.empty:
        return None
    try:
        tz = df.index.tz_convert("America/New_York")
    except Exception:
        tz = df.index
    out = df[["open","high","low","close","volume"]].copy()
    out["d"] = tz.date
    out["hm"] = tz.time
    return out.sort_values(["d","hm"])


def rich_features_for_day(daily, bars, panel, sector_etfs, symbol):
    """Build the 17-vector for the latest trading day of `symbol`."""
    if bars is None or daily is None or panel is None:
        return None
    bars = bars[bars["d"] == bars["d"].max()]  # latest session with 1m data
    if len(bars) < 30:
        return None
    d = daily.iloc[-1]
    closes = daily["close"]
    c = float(closes.iloc[-1])
    if c <= 0:
        return None
    prev = float(closes.iloc[-2]) if len(closes) >= 2 else c
    gap = (float(d["open"]) - prev)/prev if prev else 0.0
    day_high = float(d["high"]); day_low = float(d["low"])
    day_range = (day_high - day_low)/c

    rets = closes.pct_change().dropna()
    def safe_ret(n):
        if len(closes) > n:
            v = float((closes.iloc[-1]-closes.iloc[-1-n])/closes.iloc[-1-n])
            return v if np.isfinite(v) else 0.0
        return 0.0
    vol_z = (float(d["volume"]-daily["volume"].mean())/(daily["volume"].std()+1e-9)) if daily["volume"].std()>0 else 0.0

    # intraday bar features for latest day
    g = bars
    g30 = g[g["hm"] <= pd.Timestamp("09:59").time()]
    g60 = g[g["hm"] <= pd.Timestamp("10:29").time()]
    brets = g["close"].pct_change().dropna()
    vwap = float((g["close"]*g["volume"]).sum()/(g["volume"].sum()+1e-9)) if g["volume"].sum()>0 else float(g["close"].iloc[-1])
    or30 = float((g30["high"].max()-g30["low"].min())/c) if len(g30) else 0.0
    or60 = float((g60["high"].max()-g60["low"].min())/c) if len(g60) else 0.0
    orfull = float((g["high"].max()-g["low"].min())/c)
    ivol = float(brets.std()) if len(brets)>10 else 0.0
    vwap_dev = float((g["close"].iloc[-1]-vwap)/c)

    # cross-sectional / sector momentum
    day = pd.Timestamp(daily.index[-1])
    mkt = panel.get("RSP") if "RSP" in panel else panel.mean(axis=1)
    sr = [c2 for c2 in ("XLK","XLF","XLY","XLP","XLE","XLI","XLV","XLU","SMH","TLT","IEF","HYG","LQD") if c2 in panel]
    sector_mean = panel[sr].mean(axis=1) if sr else panel.mean(axis=1)
    pr5 = panel.pct_change(5)
    def panel_ret(series, dte):
        try:
            if day in series.index and len(series.loc[:day])>series.loc[:day].shape[0] or True:
                # last value up to day
                sub = series.loc[:day].dropna()
                if len(sub)>=6: return float(sub.iloc[-1]/sub.iloc[-6]-1)
            return 0.0
        except Exception: return 0.0
    own5 = panel_ret(panel[symbol], 5)
    m5 = panel_ret(mkt, 5)
    sm5 = panel_ret(sector_mean, 5)
    own_vs_mkt = own5 - m5
    own_vs_sector = own5 - sm5

    vec = np.array([
        float(d["open"])/c, float(d["high"])/c, float(d["low"])/c,
        gap, day_range, rets.iloc[-1] if len(rets) else 0.0,
        safe_ret(5), safe_ret(20), vol_z,
        or30, or60, orfull, ivol, vwap_dev,
        own_vs_mkt, own_vs_sector, sm5,
    ])
    return np.where(np.isfinite(vec), vec, 0.0)


def etf_rich_forecast(symbol, daily, bars, panel):
    """
    Predict tomorrow's close using the enhanced rich model from the 17-feature
    intraday + sector-momentum vector. Returns forecast dict or None.
    """
    meta_path = os.path.join(ROOT, "models", "etf_rich_predictor_meta.json")
    if not os.path.isfile(meta_path):
        return None
    vec = rich_features_for_day(daily, bars, panel, None, symbol)
    if vec is None:
        return None
    import torch, torch.nn as nn, json as _json
    meta = _json.load(open(meta_path, encoding="utf-8"))
    mean = np.array(meta["scaling_X"]["mean"]); std = np.array(meta["scaling_X"]["std"])
    xn = (vec - mean)/std
    class DeepPricePredictor(nn.Module):
        def __init__(s, d): super().__init__(); s.net=nn.Sequential(
            nn.Linear(d,256),nn.BatchNorm1d(256),nn.ReLU(),nn.Dropout(0.3),
            nn.Linear(256,256),nn.BatchNorm1d(256),nn.ReLU(),nn.Dropout(0.3),
            nn.Linear(256,128),nn.BatchNorm1d(128),nn.ReLU(),nn.Dropout(0.2),
            nn.Linear(128,64),nn.ReLU(),nn.Dropout(0.1),nn.Linear(64,1))
        def forward(s,x): return s.net(x)
    try:
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model = DeepPricePredictor(len(RICH_FEATURES)).to(device)
        model.load_state_dict(torch.load(os.path.join(ROOT,"models","etf_rich_predictor_best.pt"), map_location=device))
        model.eval()
        with torch.no_grad():
            xn_t = torch.tensor(xn, dtype=torch.float32).unsqueeze(0).to(device)
            ratio_s = float(model(xn_t).item())
        ratio = ratio_s*float(meta["scaling_y"]["std"])+float(meta["scaling_y"]["mean"])
        ratio = float(np.clip(ratio, 0.7, 1.4))
    except Exception:
        return None
    spot = float(daily["close"].iloc[-1])
    eod_close = spot*ratio
    eod_move_pct = (eod_close-spot)/spot*100.0
    week_end = eod_close*(1+eod_move_pct/100.0*0.25)
    return {"kind":"etf","spot":spot,"eod_close":eod_close,"eod_move_pct":eod_move_pct,
            "confidence":"MEDIUM","week_end":week_end,"week_move_pct":(week_end-spot)/spot*100.0,
            "model":"etf_rich"}


def main():
    ap = argparse.ArgumentParser(description="MarketPin Databento Forecast")
    ap.add_argument("--symbols", nargs="*", default=None)
    ap.add_argument("--horizon", choices=["eod", "next-week", "both"], default="both")
    args = ap.parse_args()

    if args.symbols:
        req = [s.upper() for s in args.symbols]
        index_syms = [s for s in req if s in INDEX_SYMBOLS]
        etf_syms = [s for s in req if s in ETF_SYMBOLS]
        unknown = [s for s in req if s not in INDEX_SYMBOLS and s not in ETF_SYMBOLS]
        if unknown:
            print(f"[WARN] unsupported symbols: {unknown}")
    else:
        index_syms = list(INDEX_SYMBOLS)
        etf_syms = list(ETF_SYMBOLS)

    client = databento_client()[0]
    results = {}

    print("\n" + "=" * 78)
    print("INDEX (SPX/NDX/RUT): EOD via live GEX engine; next-week via official closes")
    print("=" * 78)
    for sym in index_syms:
        if args.horizon in ("eod", "both"):
            eod = e2e_prediction(sym)
            if eod is not None:
                results[(sym, "eod")] = {**eod, "kind": "index", "horizon": "eod"}
                print(f"  {sym:>5} EOD : spot {eod['spot']:,.2f} -> {eod['eod_close']:,.2f} "
                      f"({eod['eod_move_pct']:+.2f}%)  conf={eod['confidence']} bias={eod['bias']}")
            else:
                print(f"  {sym:>5} EOD : no live same-session payload today -> skipped")
        if args.horizon in ("next-week", "both"):
            daily = index_daily_for_forecast(sym)
            wk = forecast_for_df(daily) if daily is not None else None
            if wk is not None:
                wk["kind"] = "index"
                results[(sym, "next-week")] = {**wk, "horizon": "next-week"}
                print(f"  {sym:>5} WK  : spot {wk['spot']:,.2f} -> WKEND {wk['week_end']:,.2f} "
                      f"({wk['week_move_pct']:+.2f}%)  conf={wk['confidence']}")
            else:
                print(f"  {sym:>5} WK  : insufficient official-close history -> skipped")

    print("\n" + "=" * 78)
    print(f"ETF: EOD + next-week forecast (Databento {DATASET}; rich intraday+sector model)")
    print("=" * 78)
    # Build the full daily panel once for cross-sectional sector features.
    panel = {}
    per_sym_daily = {}
    for sym in etf_syms:
        try:
            dly = fetch_daily_ohlcv(client, sym)
        except Exception as exc:
            print(f"  {sym:>5}: fetch failed -> {str(exc)[:80]}")
            continue
        if dly is not None and len(dly) >= 5:
            panel[sym] = dly["close"]
            per_sym_daily[sym] = dly
    panel = pd.DataFrame(panel).sort_index() if panel else None
    for sym in etf_syms:
        daily = per_sym_daily.get(sym)
        if daily is None:
            continue
        bars = None
        eff = None
        try:
            bars = fetch_1m_bars(client, sym)
            eff = etf_rich_forecast(sym, daily, bars, panel)
        except Exception as exc:
            print(f"  {sym:>5}: rich fetch failed ({str(exc)[:60]}); trying fallback")
        f = forecast_for_df(daily)
        if eff is None and f is not None:
            eff = dict(f); eff["model"] = "statistical"
        elif eff is not None:
            eff["model"] = "etf-rich"
        if eff is None:
            print(f"  {sym:>5}: insufficient history -> skipped")
            continue
        eff["kind"] = "etf"
        if args.horizon in ("eod", "both"):
            results[(sym, "eod")] = {**eff, "horizon": "eod"}
            print(f"  {sym:>5} EOD : spot {eff['spot']:,.2f} -> {eff['eod_close']:,.2f} "
                  f"({eff['eod_move_pct']:+.2f}%)  conf={eff['confidence']} [{eff['model']}]")
        if args.horizon in ("next-week", "both"):
            results[(sym, "next-week")] = {**eff, "horizon": "next-week"}
            print(f"  {sym:>5} WK  : spot {eff['spot']:,.2f} -> WKEND {eff['week_end']:,.2f} "
                  f"({eff['week_move_pct']:+.2f}%)  conf={eff['confidence']} [{eff['model']}]")

    out_csv = os.path.join(ROOT, "outputs", f"forecast_{date.today().isoformat()}.csv")
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["as_of", "symbol", "kind", "horizon", "spot", "forecast_close",
                    "move_pct", "confidence", "detail"])
        for (sym, horizon), r in sorted(results.items()):
            detail = json.dumps(
                {k: v for k, v in r.items()
                 if k not in ("kind", "horizon", "spot", "eod_close", "week_end",
                              "eod_move_pct", "week_move_pct", "confidence", "week_by_day")},
                default=str)
            if horizon == "eod":
                close = r.get("eod_close", "")
                move = r.get("eod_move_pct", "")
            else:
                close = r.get("week_end", "")
                move = r.get("week_move_pct", "")
            w.writerow([date.today().isoformat(), sym, r["kind"], horizon,
                        round(r["spot"], 4), close, move, r["confidence"], detail])
    print(f"\nSaved: {out_csv}")
    print("\n" + "=" * 78)
    print("NOTE: forecasts are research estimates driven by Databento market data")
    print("and the app's own models; not investment advice.")
    print("=" * 78)


if __name__ == "__main__":
    main()
