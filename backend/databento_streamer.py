"""Databento OPRA live streamer for same-day SPXW/NDXP gamma pins.

This is an alternate market data provider for the existing FastAPI backend. It
preserves the same high-level streamer interface as the Polygon streamer:
start/stop, get_latest_data, get_all_latest, and callback support.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import re
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Optional

import databento as db
import pandas as pd
from scipy.optimize import brentq
from scipy.stats import norm

from app.core.gex import compute_aggregate_gex
from backend.config import DATA_DIR, MAX_BUFFER_SIZE

logger = logging.getLogger(__name__)

DATASET = "OPRA.PILLAR"
RISK_FREE_RATE = 0.0525
CONTRACT_MULTIPLIER = 100.0
OI_STAT_TYPE = 9
MAX_SYMBOLS_PER_REQUEST = 200
RECONNECT_BASE_DELAY_SECONDS = float(os.getenv("DATABENTO_RECONNECT_BASE_SECONDS", "3"))
RECONNECT_MAX_DELAY_SECONDS = float(os.getenv("DATABENTO_RECONNECT_MAX_SECONDS", "30"))
RAW_SYMBOL_RE = re.compile(r"^(?P<root>[A-Z]+)\s+(?P<yymmdd>\d{6})(?P<cp>[CP])(?P<strike>\d{8})$")


@dataclass(frozen=True)
class DatabentoMarketConfig:
    label: str
    daily_root: str
    strike_step: int
    strike_min: int
    strike_max: int
    days_forward: int = 0


MARKETS = {
    "SPX": DatabentoMarketConfig("SPX", "SPXW", 5, 7350, 7550),
    "NDX": DatabentoMarketConfig("NDX", "NDXP", 25, 29400, 30100),
    "XSP": DatabentoMarketConfig("XSP", "XSP", 1, 700, 760),
    "XND": DatabentoMarketConfig("XND", "XND", 1, 280, 310),
    "RUT": DatabentoMarketConfig("RUT", "RUTW", 5, 2100, 2300),
    "MRUT": DatabentoMarketConfig("MRUT", "MRUT", 1, 210, 230),
    "VIX": DatabentoMarketConfig("VIX", "VIXW", 1, 10, 35, days_forward=35),
    "OEX": DatabentoMarketConfig("OEX", "OEX", 5, 3600, 3900),
    "DJX": DatabentoMarketConfig("DJX", "DJX", 1, 420, 460),
    "RUI": DatabentoMarketConfig("RUI", "RUI", 5, 4000, 4600),
    "XAU": DatabentoMarketConfig("XAU", "XAU", 5, 250, 360),
    "HGX": DatabentoMarketConfig("HGX", "HGX", 5, 550, 700),
    "OSX": DatabentoMarketConfig("OSX", "OSX", 5, 75, 115),
    "UTY": DatabentoMarketConfig("UTY", "UTY", 5, 950, 1100),
}


def raw_option_symbol(root: str, expiration: date, option_type: str, strike: float) -> str:
    return f"{root:<6}{expiration.strftime('%y%m%d')}{option_type}{int(round(strike * 1000)):08d}"


def parse_raw_option_symbol(symbol: str) -> dict[str, object] | None:
    match = RAW_SYMBOL_RE.match(str(symbol))
    if not match:
        return None
    yymmdd = match.group("yymmdd")
    expiration = date(2000 + int(yymmdd[:2]), int(yymmdd[2:4]), int(yymmdd[4:6]))
    return {
        "expiration": expiration,
        "option_type": match.group("cp"),
        "strike": int(match.group("strike")) / 1000.0,
    }


def chunks(values: list[str], size: int = MAX_SYMBOLS_PER_REQUEST):
    for start in range(0, len(values), size):
        yield values[start:start + size]


def parse_db_time(value: str) -> datetime:
    cleaned = value.replace("Z", "+00:00")
    if "." in cleaned:
        head, tail = cleaned.split(".", 1)
        frac, zone = tail[:9], tail[9:]
        cleaned = f"{head}.{frac[:6]}{zone}"
    return datetime.fromisoformat(cleaned)


def normalize_price(value) -> float | None:
    if value is None:
        return None
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(price) or price <= 0:
        return None
    if abs(price) > 1_000_000:
        price /= 1_000_000_000.0
    return price


def option_intrinsic(spot: float, strike: float, option_type: str) -> float:
    return max(spot - strike, 0.0) if option_type == "C" else max(strike - spot, 0.0)


def black_scholes_price(spot: float, strike: float, years: float, vol: float, option_type: str) -> float:
    if years <= 0 or vol <= 0:
        return option_intrinsic(spot, strike, option_type)
    d1 = (math.log(spot / strike) + (RISK_FREE_RATE + 0.5 * vol * vol) * years) / (vol * math.sqrt(years))
    d2 = d1 - vol * math.sqrt(years)
    discounted_strike = strike * math.exp(-RISK_FREE_RATE * years)
    if option_type == "C":
        return spot * norm.cdf(d1) - discounted_strike * norm.cdf(d2)
    return discounted_strike * norm.cdf(-d2) - spot * norm.cdf(-d1)


def implied_volatility(spot: float, strike: float, years: float, mid: float, option_type: str) -> float | None:
    if mid <= option_intrinsic(spot, strike, option_type):
        return None

    def error(vol: float) -> float:
        return black_scholes_price(spot, strike, years, vol, option_type) - mid

    try:
        return brentq(error, 0.0001, 5.0, maxiter=60)
    except ValueError:
        return None


def black_scholes_gamma(spot: float, strike: float, years: float, vol: float) -> float:
    if spot <= 0 or strike <= 0 or years <= 0 or vol <= 0:
        return 0.0
    d1 = (math.log(spot / strike) + (RISK_FREE_RATE + 0.5 * vol * vol) * years) / (vol * math.sqrt(years))
    return norm.pdf(d1) / (spot * vol * math.sqrt(years))


def years_to_expiration(expiration: date) -> float:
    days = max((expiration - date.today()).days, 0)
    return max(days / 365.0, 1.0 / 365.0)


def zero_gamma_level(gex_by_strike: dict[float, float]) -> float | None:
    strikes = sorted(gex_by_strike)
    for lower, upper in zip(strikes, strikes[1:]):
        lower_gex = gex_by_strike[lower]
        upper_gex = gex_by_strike[upper]
        if lower_gex * upper_gex < 0:
            return lower + (upper - lower) * abs(lower_gex) / (abs(lower_gex) + abs(upper_gex))
    return None


def max_pain(chain: pd.DataFrame) -> float | None:
    strikes = sorted(chain["strike"].dropna().unique())
    if not strikes:
        return None
    calls = chain[chain["option_type"] == "C"]
    puts = chain[chain["option_type"] == "P"]
    pain_by_strike: dict[float, float] = {}
    for settlement in strikes:
        call_pain = ((settlement - calls["strike"]).clip(lower=0) * calls["open_interest"]).sum()
        put_pain = ((puts["strike"] - settlement).clip(lower=0) * puts["open_interest"]).sum()
        pain_by_strike[float(settlement)] = float(call_pain + put_pain)
    return min(pain_by_strike, key=pain_by_strike.get)


def infer_spot_from_pairs(chain: pd.DataFrame, years: float) -> float | None:
    paired = chain.pivot_table(index="strike", columns="option_type", values="mid", aggfunc="last")
    if "C" not in paired.columns or "P" not in paired.columns:
        return None
    paired = paired.dropna(subset=["C", "P"])
    paired = paired[(paired["C"] > 0) & (paired["P"] > 0)].copy()
    if paired.empty:
        return None
    paired["forward"] = (paired.index.to_series().astype(float) + paired["C"] - paired["P"]) * math.exp(RISK_FREE_RATE * years)
    paired["pair_gap"] = (paired["C"] - paired["P"]).abs()
    return float(paired.sort_values("pair_gap").head(15)["forward"].median())


class DatabentoGammaStreamer:
    """Live Databento OPRA streamer compatible with the old backend streamer API."""

    provider_name = "databento"

    def __init__(self, symbols: list[str] | None = None):
        self.symbols = [symbol for symbol in (symbols or ["SPX", "NDX"]) if symbol in MARKETS]
        self.api_key = os.getenv("DATABENTO_API_KEY")
        self.is_running = False
        self.callbacks: list[Callable] = []
        self.buffers: dict[str, deque] = {symbol: deque(maxlen=MAX_BUFFER_SIZE) for symbol in self.symbols}
        self.latest_pins: dict[str, dict] = {}
        self.id_to_symbol: dict[int, str] = {}
        self.quotes: dict[str, dict[str, float]] = {}
        self.universe = pd.DataFrame()
        self.live_symbols: list[str] = []
        self.latest_invalid: dict[str, dict] = {}
        self.thread: Optional[threading.Thread] = None
        self.client: Optional[db.Live] = None
        self.messages_received = 0
        self.last_message_time: dict[str, datetime] = {}
        self.last_error: str | None = None
        self.schema = os.getenv("DATABENTO_LIVE_SCHEMA", "cbbo-1s")
        self.replay_minutes = int(os.getenv("DATABENTO_REPLAY_MINUTES", "2"))
        self.update_interval = float(os.getenv("DATABENTO_UPDATE_INTERVAL", "5"))
        self.snapshot_interval = float(os.getenv("DATABENTO_SNAPSHOT_INTERVAL_SECONDS", "60"))
        self._last_compute = 0.0
        self._last_snapshot_write: dict[str, float] = {}
        self.cache_dir = DATA_DIR / "databento_cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.exports_dir = DATA_DIR.parent / "exports"
        self.audit_dir = DATA_DIR.parent / "logs" / "audit"

    def _cache_path(self) -> object:
        symbol_key = "-".join(sorted(self.symbols)) or "none"
        symbol_hash = hashlib.sha1(symbol_key.encode("utf-8")).hexdigest()[:10]
        return self.cache_dir / f"opra_universe_{date.today().isoformat()}_{symbol_hash}.csv"

    def _load_cached_universe(self) -> bool:
        cache_path = self._cache_path()
        if not cache_path.exists() or os.getenv("DATABENTO_REFRESH_CACHE", "0") == "1":
            return False
        try:
            self.universe = pd.read_csv(cache_path)
            self.universe["expiration_date"] = pd.to_datetime(self.universe["expiration_date"]).dt.date
            self.live_symbols = sorted(self.universe["symbol"].dropna().unique().tolist())
            logger.info("Loaded Databento universe cache: %s symbols from %s", len(self.live_symbols), cache_path)
            return bool(self.live_symbols)
        except Exception as exc:
            logger.warning("Failed to load Databento universe cache: %s", exc)
            return False

    def _save_cached_universe(self) -> None:
        if self.universe.empty:
            return
        cache_path = self._cache_path()
        try:
            self.universe.to_csv(cache_path, index=False)
            logger.info("Saved Databento universe cache: %s", cache_path)
        except Exception as exc:
            logger.warning("Failed to save Databento universe cache: %s", exc)

    def add_callback(self, callback: Callable):
        self.callbacks.append(callback)

    def _generate_symbols(self, config: DatabentoMarketConfig) -> list[str]:
        today = date.today()
        values: list[str] = []
        roots = [config.daily_root]
        if config.label in {"SPX", "NDX"} and config.label not in roots:
            roots.append(config.label)
        for root in roots:
            for strike in range(config.strike_min, config.strike_max + config.strike_step, config.strike_step):
                values.append(raw_option_symbol(root, today, "C", strike))
                values.append(raw_option_symbol(root, today, "P", strike))
        return values

    def _definition_parents(self, config: DatabentoMarketConfig) -> list[str]:
        roots = [config.daily_root]
        if config.label not in roots:
            roots.append(config.label)
        return [f"{root}.OPT" for root in roots]

    def _fetch_definition_universe(self, historical: db.Historical, config: DatabentoMarketConfig) -> pd.DataFrame:
        today = date.today()
        max_expiration = today + timedelta(days=config.days_forward)
        end = self._available_end(historical, "definition")
        start_dt = datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc)
        if end <= start_dt:
            logger.warning("Databento definition schema has no data available for %s yet (start=%s end=%s)", today, start_dt, end)
            return pd.DataFrame(columns=["market", "symbol", "expiration_date", "option_type", "strike"])
        frames: list[pd.DataFrame] = []

        for parent in self._definition_parents(config):
            try:
                data = historical.timeseries.get_range(
                    dataset=DATASET,
                    schema="definition",
                    symbols=parent,
                    stype_in="parent",
                    start=today.isoformat(),
                    end=end,
                    limit=20000,
                )
            except db.BentoClientError as exc:
                logger.warning("Databento definition discovery failed for %s: %s", parent, exc)
                continue
            frame = data.to_df()
            if not frame.empty:
                frames.append(frame.reset_index(drop=False))

        if not frames:
            return pd.DataFrame(columns=["market", "symbol", "expiration_date", "option_type", "strike"])

        definitions = pd.concat(frames, axis=0).drop_duplicates(subset=["symbol"], keep="last")
        definitions["expiration_date"] = pd.to_datetime(definitions["expiration"], errors="coerce").dt.date
        definitions["option_type"] = definitions["instrument_class"].astype(str)
        definitions["strike"] = pd.to_numeric(definitions["strike_price"], errors="coerce")
        definitions["market"] = config.label
        definitions = definitions[
            definitions["expiration_date"].between(today, max_expiration) &
            definitions["option_type"].isin(["C", "P"]) &
            definitions["strike"].between(config.strike_min, config.strike_max)
        ].copy()
        return definitions[["market", "symbol", "expiration_date", "option_type", "strike"]]

    def _available_end(self, historical: db.Historical, schema: str) -> datetime:
        range_info = historical.metadata.get_dataset_range(dataset=DATASET)
        return parse_db_time(range_info["schema"][schema]["end"])

    def _fetch_open_interest(self, historical: db.Historical, symbols: list[str]) -> pd.DataFrame:
        today = date.today().isoformat()
        end = self._available_end(historical, "statistics")
        start_dt = datetime.combine(date.today(), datetime.min.time(), tzinfo=timezone.utc)
        if end <= start_dt:
            logger.warning("Databento statistics schema has no data available for %s yet (start=%s end=%s)", today, start_dt, end)
            return pd.DataFrame(columns=["symbol", "open_interest"])
        frames: list[pd.DataFrame] = []
        for symbol_chunk in chunks(symbols):
            try:
                data = historical.timeseries.get_range(
                    dataset=DATASET,
                    schema="statistics",
                    symbols=symbol_chunk,
                    stype_in="raw_symbol",
                    start=today,
                    end=end,
                )
            except db.BentoClientError as exc:
                if "None of the symbols could be resolved" in str(exc):
                    continue
                raise
            frame = data.to_df()
            if not frame.empty:
                frames.append(frame)
        if not frames:
            return pd.DataFrame(columns=["symbol", "open_interest"])
        df = pd.concat(frames, axis=0)
        oi = df[df["stat_type"] == OI_STAT_TYPE].copy()
        if oi.empty:
            return pd.DataFrame(columns=["symbol", "open_interest"])
        return oi.groupby("symbol", as_index=False)["quantity"].max().rename(columns={"quantity": "open_interest"})

    def _build_universe(self) -> None:
        if not self.api_key:
            raise RuntimeError("DATABENTO_API_KEY is not set")
        if self._load_cached_universe():
            return
        historical = db.Historical(self.api_key)
        frames: list[pd.DataFrame] = []
        live_symbols: list[str] = []
        for symbol in self.symbols:
            config = MARKETS[symbol]
            if config.days_forward > 0:
                frame = self._fetch_definition_universe(historical, config)
                candidates = sorted(frame["symbol"].dropna().unique().tolist())
            else:
                candidates = self._generate_symbols(config)
                rows = []
                for raw_symbol in candidates:
                    parsed = parse_raw_option_symbol(raw_symbol)
                    if not parsed:
                        continue
                    rows.append({
                        "market": config.label,
                        "symbol": raw_symbol,
                        "expiration_date": parsed["expiration"],
                        "option_type": parsed["option_type"],
                        "strike": parsed["strike"],
                    })
                frame = pd.DataFrame(rows)

            if frame.empty or not candidates:
                logger.warning("No Databento definition rows for %s", symbol)
                continue

            oi = self._fetch_open_interest(historical, candidates)
            frame = frame.merge(oi, on="symbol", how="left")
            frame["open_interest"] = pd.to_numeric(frame["open_interest"], errors="coerce").fillna(0.0)
            active = frame[frame["open_interest"] > 0].copy()
            if active.empty:
                logger.warning("No Databento OI rows for %s", symbol)
                continue
            frames.append(active)
            live_symbols.extend(active["symbol"].dropna().unique().tolist())
            logger.info("Databento %s universe: %s same-day symbols with OI", symbol, len(active))
        if not frames:
            raise RuntimeError(
                f"No Databento OPRA universe could be built for {date.today().isoformat()}. "
                "OPRA may be closed, today may be a market holiday, or Databento has not published today's definitions/statistics yet."
            )
        self.universe = pd.concat(frames, axis=0).drop_duplicates(subset=["symbol"])
        self.live_symbols = sorted(set(live_symbols))
        self._save_cached_universe()

    def _calculate_pin(self, market: str) -> dict | None:
        rows = []
        for _, row in self.universe[self.universe["market"] == market].iterrows():
            quote = self.quotes.get(str(row["symbol"]))
            if not quote:
                continue
            rows.append({
                "symbol": row["symbol"],
                "strike": float(row["strike"]),
                "option_type": str(row["option_type"]),
                "expiration_date": row["expiration_date"],
                "open_interest": float(row["open_interest"]),
                "mid": float(quote["mid"]),
            })
        chain = pd.DataFrame(rows)
        if chain.empty:
            return None
        expiration = min(chain["expiration_date"])
        years = years_to_expiration(expiration)
        spot = infer_spot_from_pairs(chain, years)
        if spot is None:
            return None
        chain = chain[(chain["open_interest"] > 0) & (chain["strike"].between(spot * 0.96, spot * 1.04))].copy()
        calc_rows = []
        for _, row in chain.iterrows():
            iv = implied_volatility(spot, float(row["strike"]), years, float(row["mid"]), str(row["option_type"]))
            if iv is None:
                continue
            gamma = black_scholes_gamma(spot, float(row["strike"]), years, iv)
            exposure = gamma * float(row["open_interest"]) * CONTRACT_MULTIPLIER
            is_call = row["option_type"] == "C"
            expiration_date = row["expiration_date"]
            days_to_expiry = max((expiration_date - date.today()).days, 0) if isinstance(expiration_date, date) else 0
            calc_rows.append({
                "strike": float(row["strike"]),
                "call_gex": exposure if is_call else 0.0,
                "put_gex": exposure if not is_call else 0.0,
                "gex": exposure if is_call else -exposure,
                "option_type": str(row["option_type"]),
                "days_to_expiry": float(days_to_expiry),
                "expiration_date": expiration_date,
            })
        calc = pd.DataFrame(calc_rows)
        if calc.empty:
            return None
        gex_by_strike = calc.groupby("strike")["gex"].sum().to_dict()
        max_gamma_pin = max(gex_by_strike, key=lambda strike: abs(gex_by_strike[strike]))
        zero_gamma = zero_gamma_level(gex_by_strike)
        pain = max_pain(chain)
        anchors = [("Max Gamma Pin", max_gamma_pin)]
        if zero_gamma is not None:
            anchors.append(("Zero-Gamma", zero_gamma))
        if pain is not None:
            anchors.append(("Max Pain", pain))
        likely_name, likely = min(anchors, key=lambda item: abs(item[1] - spot))
        pos_wall = max((strike for strike, value in gex_by_strike.items() if value > 0), key=lambda strike: gex_by_strike[strike], default=None)
        neg_wall = min((strike for strike, value in gex_by_strike.items() if value < 0), key=lambda strike: gex_by_strike[strike], default=None)
        top = sorted(gex_by_strike.items(), key=lambda item: abs(item[1]), reverse=True)[:5]
        aggregate_gex = compute_aggregate_gex(
            calc[["strike", "call_gex", "put_gex"]].to_dict(orient="records")
        )
        call_gex_total = aggregate_gex.call_gex_total
        put_gex_total = aggregate_gex.put_gex_total
        gross_gex = aggregate_gex.gross_gex
        net_gex = aggregate_gex.net_gex
        min_days = float(calc["days_to_expiry"].min())
        max_days = float(calc["days_to_expiry"].max())
        top_strikes = []
        for strike, value in top:
            strike_rows = calc[calc["strike"] == strike].copy()
            weights = strike_rows["gex"].abs()
            if weights.sum() > 0:
                days_to_expiry = float((strike_rows["days_to_expiry"] * weights).sum() / weights.sum())
            else:
                days_to_expiry = float(strike_rows["days_to_expiry"].min())
            expirations = sorted(str(expiration) for expiration in strike_rows["expiration_date"].dropna().unique())
            top_strikes.append({
                "strike": strike,
                "gex": value,
                "net_gex": value,
                "total_gex": abs(value),
                "days_to_expiry": days_to_expiry,
                "expiration_count": len(expirations),
                "expirations": expirations[:4],
            })
        return {
            "symbol": market,
            "timestamp": datetime.utcnow(),
            "price": spot,
            "provider": self.provider_name,
            "predicted_close": likely,
            "likely_close": likely,
            "likely_anchor": likely_name,
            "gamma_pin": max_gamma_pin,
            "zero_gamma": zero_gamma,
            "max_pain": pain,
            "positive_gex_wall": pos_wall,
            "negative_gex_wall": neg_wall,
            "net_gex": net_gex,
            "gross_gex": gross_gex,
            "call_gex_total": call_gex_total,
            "put_gex_total": put_gex_total,
            "expirations_min_days": min_days,
            "expirations_max_days": max_days,
            "contracts": int(len(calc)),
            "quotes_cached": int(len(self.quotes)),
            "top_strikes": top_strikes,
        }

    def _snapshot_payload(self, result: dict) -> dict:
        timestamp = result.get("timestamp")
        if isinstance(timestamp, datetime):
            generated_at_utc = timestamp.replace(tzinfo=timezone.utc).isoformat()
        else:
            generated_at_utc = datetime.now(timezone.utc).isoformat()

        gross_gex = float(result.get("gross_gex") or abs(float(result.get("net_gex") or 0)))
        net_gex = float(result.get("net_gex") or 0)
        gamma_pin = result.get("gamma_pin")
        spot = float(result.get("price") or 0)
        top_strikes = result.get("top_strikes") or []

        return {
            "snapshot_version": "databento-live-1.0",
            "generated_at_utc": generated_at_utc,
            "timestamp_utc": generated_at_utc,
            "symbol": result.get("symbol"),
            "spot_last": spot,
            "spot_source": "databento_opra_put_call_parity",
            "chain_symbol_used": "OPRA",
            "underlying_reported": result.get("symbol"),
            "is_etf_proxy": False,
            "primary_gamma_pin_strike": gamma_pin,
            "gamma_pin_strike": gamma_pin,
            "primary_gamma_pin_abs_gex": abs(net_gex),
            "zero_gamma_level": result.get("zero_gamma"),
            "zero_gamma_method": "linear_interpolation",
            "max_pain_strike": result.get("max_pain"),
            "likely_close": result.get("likely_close"),
            "likely_anchor": result.get("likely_anchor"),
            "call_gex_total": float(result.get("call_gex_total") or 0),
            "put_gex_total": float(result.get("put_gex_total") or 0),
            "gross_gex": gross_gex,
            "net_gex": net_gex,
            "total_gex_abs": gross_gex,
            "total_gex_net": net_gex,
            "contracts_count": int(result.get("contracts") or 0),
            "expirations_min_days": int(float(result.get("expirations_min_days") or 0)),
            "expirations_max_days": int(float(result.get("expirations_max_days") or 0)),
            "validation_is_valid": True,
            "validation_failure_reasons": [],
            "gamma_excluded_from_model": False,
            "confidence": None,
            "top_strikes_by_abs_gex": top_strikes,
            "quotes_cached": int(result.get("quotes_cached") or 0),
            "provider": self.provider_name,
        }

    def _write_snapshot(self, result: dict) -> None:
        symbol = str(result.get("symbol") or "").upper()
        if not symbol:
            return

        now = time.monotonic()
        last_write = self._last_snapshot_write.get(symbol, 0.0)
        if now - last_write < self.snapshot_interval:
            return
        self._last_snapshot_write[symbol] = now

        payload = self._snapshot_payload(result)
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        try:
            export_dir = self.exports_dir / symbol
            export_dir.mkdir(parents=True, exist_ok=True)
            export_file = export_dir / f"{date_str}.ndjson"
            with open(export_file, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, separators=(",", ":"), default=str) + "\n")
        except Exception as exc:
            logger.warning("Failed to write Databento NDJSON snapshot for %s: %s", symbol, exc)

        try:
            audit_dir = self.audit_dir / symbol
            audit_dir.mkdir(parents=True, exist_ok=True)
            audit_file = audit_dir / (datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S") + ".json")
            with open(audit_file, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, default=str)
        except Exception as exc:
            logger.warning("Failed to write Databento audit snapshot for %s: %s", symbol, exc)

    def _write_invalid_snapshot(self, symbol: str, reason: str) -> None:
        symbol = symbol.upper()
        self.latest_pins.pop(symbol, None)
        now = time.monotonic()
        last_write = self._last_snapshot_write.get(symbol, 0.0)
        if now - last_write < self.snapshot_interval:
            return
        self._last_snapshot_write[symbol] = now

        generated_at_utc = datetime.now(timezone.utc).isoformat()
        symbol_universe = self.universe[self.universe["market"] == symbol] if not self.universe.empty else pd.DataFrame()
        payload = {
            "snapshot_version": "databento-live-1.0",
            "generated_at_utc": generated_at_utc,
            "timestamp_utc": generated_at_utc,
            "symbol": symbol,
            "timestamp": datetime.utcnow(),
            "provider": self.provider_name,
            "spot_last": 0.0,
            "price": 0.0,
            "spot_source": "databento_opra_put_call_parity",
            "chain_symbol_used": "OPRA",
            "underlying_reported": symbol,
            "is_etf_proxy": False,
            "primary_gamma_pin_strike": 0.0,
            "gamma_pin_strike": 0.0,
            "gamma_pin": None,
            "primary_gamma_pin_abs_gex": 0.0,
            "zero_gamma_level": None,
            "zero_gamma": None,
            "zero_gamma_method": None,
            "max_pain_strike": None,
            "max_pain": None,
            "likely_close": None,
            "predicted_close": None,
            "likely_anchor": None,
            "call_gex_total": 0.0,
            "put_gex_total": 0.0,
            "gross_gex": 0.0,
            "net_gex": 0.0,
            "total_gex_abs": 0.0,
            "total_gex_net": 0.0,
            "contracts_count": 0,
            "contracts_available": int(len(symbol_universe)),
            "expirations_min_days": 0,
            "expirations_max_days": 0,
            "validation_is_valid": False,
            "validation_failure_reasons": [reason],
            "gamma_excluded_from_model": True,
            "confidence": 0.0,
            "top_strikes_by_abs_gex": [],
            "quotes_cached": int(len(self.quotes)),
            "provider": self.provider_name,
            "pregate_reason": reason,
        }

        self.buffers[symbol].append(payload)
        self.last_message_time[symbol] = datetime.utcnow()
        self.latest_invalid[symbol] = payload

        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        try:
            export_dir = self.exports_dir / symbol
            export_dir.mkdir(parents=True, exist_ok=True)
            export_file = export_dir / f"{date_str}.ndjson"
            with open(export_file, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, separators=(",", ":"), default=str) + "\n")
        except Exception as exc:
            logger.warning("Failed to write invalid Databento NDJSON snapshot for %s: %s", symbol, exc)

        try:
            audit_dir = self.audit_dir / symbol
            audit_dir.mkdir(parents=True, exist_ok=True)
            audit_file = audit_dir / (datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S") + ".json")
            with open(audit_file, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, default=str)
        except Exception as exc:
            logger.warning("Failed to write invalid Databento audit snapshot for %s: %s", symbol, exc)

    def _compute_and_publish(self) -> None:
        now = time.monotonic()
        if now - self._last_compute < self.update_interval:
            return
        self._last_compute = now
        for market in self.symbols:
            result = self._calculate_pin(market)
            if not result:
                self._write_invalid_snapshot(market, "No valid Databento pin: insufficient live quote pairs or IV/gamma rows")
                continue
            self.latest_pins[market] = result
            self.latest_invalid.pop(market, None)
            self.buffers[market].append(result)
            self.last_message_time[market] = datetime.utcnow()
            self._write_snapshot(result)
            for callback in self.callbacks:
                try:
                    output = callback(result)
                    if asyncio.iscoroutine(output):
                        asyncio.run(output)
                except Exception as exc:
                    logger.error("Databento callback error: %s", exc)

    def _run_live(self) -> None:
        assert self.api_key is not None
        reconnect_delay = RECONNECT_BASE_DELAY_SECONDS
        while self.is_running:
            try:
                self._build_universe()
                self.client = db.Live(key=self.api_key, heartbeat_interval_s=5, slow_reader_behavior="skip")
                self.client.subscribe(
                    dataset=DATASET,
                    schema=self.schema,
                    symbols=self.live_symbols,
                    stype_in="raw_symbol",
                    start=(datetime.now(timezone.utc) - timedelta(minutes=self.replay_minutes)).isoformat(),
                )
                self.last_error = None
                reconnect_delay = RECONNECT_BASE_DELAY_SECONDS
                logger.info("Databento Live subscribed to %s OPRA symbols (%s)", len(self.live_symbols), self.schema)

                for record in self.client:
                    if not self.is_running:
                        break
                    class_name = type(record).__name__
                    if class_name == "SymbolMappingMsg":
                        symbol = getattr(record, "stype_out_symbol", None) or getattr(record, "stype_in_symbol", None)
                        if symbol:
                            self.id_to_symbol[int(record.instrument_id)] = str(symbol)
                        continue
                    if hasattr(record, "bid_px_00") and hasattr(record, "ask_px_00"):
                        symbol = self.id_to_symbol.get(int(record.instrument_id))
                        if not symbol:
                            continue
                        bid = normalize_price(getattr(record, "bid_px_00", None))
                        ask = normalize_price(getattr(record, "ask_px_00", None))
                        if bid is None or ask is None or ask < bid:
                            continue
                        self.quotes[symbol] = {"bid": bid, "ask": ask, "mid": (bid + ask) / 2.0}
                        self.messages_received += 1
                        self._compute_and_publish()

            except Exception as exc:
                self.last_error = str(exc)
                logger.error("Databento live stream error: %s", exc)
                if self.is_running:
                    logger.warning("Databento stream reconnecting in %.1fs", reconnect_delay)
                    time.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2.0, RECONNECT_MAX_DELAY_SECONDS)
            finally:
                if self.client:
                    try:
                        self.client.stop()
                    except Exception:
                        pass
                    self.client = None

    async def start(self):
        if self.is_running:
            return
        logger.info("Starting Databento OPRA streamer...")
        self.is_running = True
        self.thread = threading.Thread(target=self._run_live, daemon=True)
        self.thread.start()

    async def stop(self):
        self.is_running = False
        if self.client:
            self.client.stop()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2)

    def get_latest_data(self, symbol: str, n: int = 1) -> list:
        symbol = symbol.upper()
        if symbol not in self.buffers:
            return []
        buffer = self.buffers[symbol]
        return list(buffer)[-n:] if n < len(buffer) else list(buffer)

    def get_all_latest(self) -> dict[str, dict]:
        return {symbol: values[-1] for symbol, values in self.buffers.items() if values}

    def get_latest_pin(self, symbol: str) -> dict | None:
        return self.latest_pins.get(symbol.upper())

    def get_health(self) -> dict:
        latest_update = max(self.last_message_time.values()) if self.last_message_time else None
        data_age_seconds = (datetime.utcnow() - latest_update).total_seconds() if latest_update else None
        valid_symbols = sorted(self.latest_pins.keys())
        invalid_symbols = sorted(symbol for symbol in self.latest_invalid.keys() if symbol not in self.latest_pins)
        invalid_reasons = {
            symbol: (self.latest_invalid.get(symbol, {}).get("validation_failure_reasons") or [self.latest_invalid.get(symbol, {}).get("pregate_reason") or "Unavailable"])[0]
            for symbol in invalid_symbols
        }
        return {
            "provider": self.provider_name,
            "websocket": "active" if self.is_running else "stopped",
            "buffer_health": "healthy" if self.latest_pins else "warming",
            "messages_received": self.messages_received,
            "quotes_cached": len(self.quotes),
            "symbols_subscribed": len(self.live_symbols),
            "symbols_requested": self.symbols,
            "valid_symbols": valid_symbols,
            "invalid_symbols": invalid_symbols,
            "invalid_reasons": invalid_reasons,
            "cache_file": str(self._cache_path()),
            "schema": self.schema,
            "last_update_utc": latest_update.isoformat() if latest_update else None,
            "data_age_seconds": data_age_seconds,
            "last_error": self.last_error,
        }
