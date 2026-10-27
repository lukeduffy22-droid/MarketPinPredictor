"""Compact immutable EOD receipts and prospective fixed-lead selections.

Only the research writer initializes/appends these tables. Readers never migrate
or backfill a journal; legacy rows retain their original diagnostic authority.
"""
import hashlib
import json
from datetime import timedelta

from backend.forecast_horizons import _utc, _forecast_digest

PROTOCOL = "eod-fixed-lead-first-issued.v1"
LEADS_MINUTES = (15, 30, 60, 120, 240, 375)
ISSUANCE_WINDOW_SECONDS = 90

IMMUTABLE_COLUMNS = {
    "research_forecasts": (("forecast_id",), ("forecast_id", "symbol", "as_of_utc", "horizon_sessions", "target_session_date", "payload_json")),
    "research_forecast_receipts": (("forecast_id",), ("forecast_id", "committed_at_utc", "receipt_sha256")),
    "research_eod_index": (("forecast_id",), ("forecast_id", "forecast_sha256", "binding_sha256", "forecast_json")),
    "research_eod_fixed_lead_selections": (("protocol_id", "symbol", "target_session_date", "lead_minutes"),
        ("protocol_id", "symbol", "target_session_date", "lead_minutes", "forecast_id", "selected_at_utc", "receipt_sha256")),
}


def immutable_guards():
    guards = {}
    for table, (keys, columns) in IMMUTABLE_COLUMNS.items():
        for action in ("UPDATE", "DELETE"):
            name = f"{table}_no_{action.lower()}"
            guards[name] = f"CREATE TRIGGER {name} BEFORE {action} ON {table} BEGIN SELECT RAISE(ABORT,'research receipts are append only'); END"
        name = f"{table}_no_replace"
        identity = " AND ".join(f"old.{key} IS NEW.{key}" for key in keys)
        identical = " AND ".join(f"old.{column} IS NEW.{column}" for column in columns)
        guards[name] = (f"CREATE TRIGGER {name} BEFORE INSERT ON {table} WHEN EXISTS(SELECT 1 FROM {table} old "
            f"WHERE {identity} AND NOT ({identical})) BEGIN SELECT RAISE(ABORT,'research receipt replacement denied'); END")
    return guards


def guards_verified(conn):
    actual = dict(conn.execute("SELECT name,sql FROM sqlite_master WHERE type='trigger'"))
    return all(actual.get(name) == sql for name, sql in immutable_guards().items())


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def binding(key, forecast_sha256, committed):
    return hashlib.sha256((key + "\n" + forecast_sha256 + "\n" + committed).encode()).hexdigest()


def initialize_eod_index(conn):
    conn.execute("CREATE INDEX IF NOT EXISTS research_forecasts_eod_lookup "
                 "ON research_forecasts(symbol,horizon_sessions,target_session_date,as_of_utc)")
    conn.execute("CREATE TABLE IF NOT EXISTS research_eod_index ("
        "forecast_id TEXT PRIMARY KEY, forecast_sha256 TEXT NOT NULL, binding_sha256 TEXT NOT NULL, "
        "forecast_json TEXT NOT NULL)")
    conn.execute("CREATE TABLE IF NOT EXISTS research_eod_fixed_lead_selections ("
        "protocol_id TEXT NOT NULL,symbol TEXT NOT NULL,target_session_date TEXT NOT NULL,"
        "lead_minutes INTEGER NOT NULL,forecast_id TEXT NOT NULL,selected_at_utc TEXT NOT NULL,"
        "receipt_sha256 TEXT NOT NULL,PRIMARY KEY(protocol_id,symbol,target_session_date,lead_minutes))")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS research_eod_selection_forecast "
                 "ON research_eod_fixed_lead_selections(forecast_id)")
    # Existing rows are preserved. Enforce the documented append-only contract
    # before permitting a content-addressed reader cache to reuse them.
    existing = dict(conn.execute("SELECT name,sql FROM sqlite_master WHERE type='trigger'"))
    for name, sql in immutable_guards().items():
        if name in existing and existing[name] != sql:
            raise ValueError("RESEARCH_IMMUTABILITY_GUARD_MISMATCH")
        if name not in existing:
            conn.execute(sql)


def append_eod_index(conn, rows, committed):
    """Called after the full forecast transaction commits; no retrospective selection."""
    selected_at = _utc(committed)
    for key, symbol, origin, horizon, day, raw in rows:
        if horizon != 0:
            continue
        forecast = json.loads(raw)["forecast"]
        if forecast.get("forecast_id") != _forecast_digest(forecast):
            raise ValueError("EOD_FORECAST_BINDING_INVALID")
        compact = canonical(forecast)
        sha = hashlib.sha256(compact.encode()).hexdigest()
        conn.execute("INSERT OR IGNORE INTO research_eod_index VALUES (?,?,?,?)",
                     (key, sha, binding(key, sha, committed), compact))
        issued, target = _utc(origin), _utc(forecast.get("target_close_utc"))
        if None in (issued, target, selected_at) or not issued <= selected_at < target:
            continue
        for lead in LEADS_MINUTES:
            scheduled = target - timedelta(minutes=lead)
            # The first persisted forecast in a preregistered 90-second window
            # wins permanently. Missed windows stay missing; never backfilled.
            if not scheduled <= issued <= selected_at <= scheduled + timedelta(seconds=ISSUANCE_WINDOW_SECONDS):
                continue
            receipt = [PROTOCOL, symbol, day, lead, key, committed]
            digest = hashlib.sha256(canonical(receipt).encode()).hexdigest()
            conn.execute("INSERT INTO research_eod_fixed_lead_selections SELECT ?,?,?,?,?,?,? "
                         "WHERE NOT EXISTS (SELECT 1 FROM research_eod_fixed_lead_selections "
                         "WHERE protocol_id=? AND symbol=? AND target_session_date=? AND lead_minutes=?)",
                         (*receipt, digest, PROTOCOL, symbol, day, lead))


def validate_selection(row, *, key, symbol, day, origin, target, committed):
    if row is None:
        return None
    protocol, lead, selected, digest = row
    expected = hashlib.sha256(canonical([protocol, symbol, day, lead, key, selected]).encode()).hexdigest()
    selected_at = _utc(selected)
    if (protocol != PROTOCOL or lead not in LEADS_MINUTES or digest != expected or selected != committed
            or None in (origin, target, selected_at)):
        raise ValueError("FIXED_LEAD_RECEIPT_INVALID")
    scheduled = target - timedelta(minutes=lead)
    if not scheduled <= origin <= selected_at <= scheduled + timedelta(seconds=ISSUANCE_WINDOW_SECONDS) or selected_at >= target:
        raise ValueError("FIXED_LEAD_RECEIPT_NOT_PROSPECTIVE")
    return {"protocol_id": protocol, "lead_minutes": lead, "selected_at_utc": selected,
            "receipt_sha256": digest, "binding_verified": True,
            "issuance_window_seconds": ISSUANCE_WINDOW_SECONDS}
