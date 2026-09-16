# MarketPinPredictor — Development Guidelines

## Code Quality Standards

### Module-Level Documentation
Every module begins with a docstring explaining its purpose, key constraints, and what it intentionally avoids. Example pattern:
```python
"""Explainable Databento EOD prediction layer.

This module intentionally avoids LLM-based price guessing. It creates a
backtest-friendly feature snapshot from live Databento OPRA pin/GEX payloads and
returns a deterministic ensemble target with confidence and explanatory signals.
"""
from __future__ import annotations
```

Always include `from __future__ import annotations` at the top of every module.

### Imports
- Standard library first, then third-party, then local
- Use explicit imports (no wildcard `from x import *`)
- SQLAlchemy imports are grouped in one long import line from `sqlalchemy`
- `zoneinfo` (stdlib) is preferred over `pytz` for new code; `pytz` exists in legacy modules

### Type Annotations
- Use `from __future__ import annotations` to enable PEP 604 union syntax (`X | Y`)
- Return types always annotated on public functions
- Use `Any` from `typing` for untyped external payloads
- `Mapping[str, Any]` for read-only dict parameters; `dict[str, Any]` for mutable
- `Sequence[T]` for read-only list parameters

---

## Naming Conventions

### Variables and Functions
- `snake_case` throughout
- Private helpers prefixed with `_` (e.g., `_coerce_float`, `_finite`, `_num`)
- Boolean flags use `is_` or `has_` prefix (e.g., `is_valid`, `has_fallback_provenance`)
- SHA-256 fields always named `*_sha256` (e.g., `universe_sha256`, `feature_hash`)

### Constants
- `UPPER_SNAKE_CASE` for module-level constants
- Schema/version strings use kebab-case values (e.g., `"databento-close-features-2.0"`)
- Reason codes use `UPPER_SNAKE_CASE` strings (e.g., `"SUBSCRIPTION_EPOCH_INVALID"`)

### Classes
- `PascalCase` for all classes
- SQLAlchemy ORM models named as domain nouns (e.g., `MarketStructureObservation`, `OrbReferenceSample`)
- Dataclasses used for lightweight value objects (e.g., `CloseSignal`, `ClosePrediction`)

---

## Structural Conventions

### Fail-Closed Return Pattern
Functions that persist or validate data return `None` on failure rather than raising exceptions for expected failures. Callers check the return value:
```python
def save_market_structure_observation(observation: Mapping[str, Any]) -> MarketStructureObservation | None:
    if not observation_id or not symbol or ...:
        return None
    # ... persist
    return row
```

### Structured Result Dicts
Functions that perform multi-step operations return structured dicts with `recorded: bool` and `reason: str | None`:
```python
return {"recorded": False, "reason": "SUBSCRIPTION_EPOCH_INVALID"}
return {"recorded": True, "reason": None, "observation_id": observation_id}
```

### Numeric Safety Helpers
Every module that handles financial data defines or imports a `_finite` / `_num` / `_coerce_float` helper that returns `None` for non-finite values:
```python
def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None
```

Never use raw `float(value)` on untrusted input. Always guard with `math.isfinite()`.

### Timestamp Handling
- All timestamps stored as UTC naive datetimes in SQLite (no tzinfo)
- `ZoneInfo("America/New_York")` for ET conversions (never `pytz.timezone("US/Eastern")` in new code)
- ISO 8601 strings use `Z` suffix for UTC: `.replace("+00:00", "Z")`
- Helper pattern for parsing:
```python
def _parse_utc(value: Any) -> datetime | None:
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    ...
    return parsed.astimezone(UTC)
```

---

## Provenance and Identity Patterns

### SHA-256 Identity Keys
All append-only records use a deterministic SHA-256 as their primary key, computed from a canonical JSON payload:
```python
identity = json.dumps(
    {"symbol": symbol, "sample_timestamp_utc": ..., "subscription_epoch_id": ...},
    sort_keys=True,
    separators=(",", ":"),
).encode("utf-8")
sample_id = hashlib.sha256(identity).hexdigest()
```

### Canonical JSON Serialization
For replay-safe hashing, always use:
```python
json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
```
`allow_nan=False` is mandatory — NaN/Infinity in financial data is always a bug.

### Subscription Epoch Validation
Epoch IDs must be exactly 64 lowercase hex characters. Always validate with:
```python
re.fullmatch(r"[0-9a-f]{64}", subscription_epoch_id)
```

### Provenance Chain Fields
Every live data record must carry:
- `provider` — always `"databento"` for live data
- `subscription_epoch_id` — 64-hex process identity
- `subscription_generation` — positive integer, monotonic within epoch
- `universe_sha256` — 64-hex hash of selected contract universe

---

## Database Patterns

### SQLAlchemy ORM
- `SessionLocal()` created per operation, always closed in `finally`
- `engine.connect()` used for read-only queries (avoids session overhead)
- `session.begin()` context manager for atomic multi-table writes
- `IntegrityError` caught separately from `Exception` for idempotent insert handling:
```python
try:
    session.add(row)
    session.commit()
    return row
except IntegrityError:
    session.rollback()
    existing = session.get(Model, primary_key)
    if _row_matches(existing, payload):
        return existing
    return None
except Exception as exc:
    session.rollback()
    logger.warning("Failed to persist ...: %s", exc)
    return None
finally:
    session.close()
```

### SQLite Immutability Triggers
Append-only tables are protected by SQLite triggers installed at `init_db()`:
```python
connection.execute(text("""
    CREATE TRIGGER IF NOT EXISTS table_no_update
    BEFORE UPDATE ON table_name
    BEGIN SELECT RAISE(ABORT, 'rows are immutable'); END
"""))
```
Pattern: `no_update`, `no_delete`, `no_replace`, `insert_guard`, `epoch_guard` triggers per table.

### SQLite Performance Configuration
Applied via `event.listens_for(engine, "connect")`:
```python
cursor.execute("PRAGMA journal_mode=WAL")
cursor.execute("PRAGMA synchronous=NORMAL")
cursor.execute("PRAGMA busy_timeout=10000")
cursor.execute("PRAGMA foreign_keys=ON")
```

### CheckConstraints
Every critical invariant is enforced at the DB level with named `CheckConstraint`:
```python
CheckConstraint("provider = 'databento'", name="ck_market_structure_databento_provider"),
CheckConstraint("subscription_generation > 0", name="ck_market_structure_positive_generation"),
CheckConstraint("length(universe_sha256) = 64", name="ck_market_structure_universe_hash_length"),
```

### Selective Field Loading
For high-frequency read paths, select only needed columns rather than hydrating full ORM objects:
```python
statement = select(
    *(getattr(Model, field).label(field) for field in fields)
).where(...)
```

---

## Async Patterns

### Background Capture Loops
Long-running capture loops use `asyncio.to_thread` for blocking DB writes:
```python
async def run_market_structure_capture_loop(streamer, journal, *, poll_seconds=0.1):
    seen: dict[str, tuple] = {}
    while True:
        for symbol, payload in streamer.get_all_latest().items():
            revision = payload_revision_key(payload)
            if seen.get(symbol) == revision:
                continue
            result = await asyncio.to_thread(journal.record, payload)
            if result.get("reason") != "PERSISTENCE_FAILED":
                seen[symbol] = revision
        await asyncio.sleep(max(0.05, float(poll_seconds)))
```

### CancelledError Propagation
Always re-raise `asyncio.CancelledError` in loops:
```python
except asyncio.CancelledError:
    raise
except Exception:
    logger.exception("...")
```

---

## ML / PyTorch Patterns

### Device Selection
```python
def inference_device() -> torch.device:
    return torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
```
Never hardcode `"cuda"` — always check availability.

### Financial Precision
Use `torch.float64` for all financial calculations:
```python
values = torch.tensor([...], dtype=torch.float64, device=device)
weights = torch.tensor([...], dtype=torch.float64, device=device)
result = float(torch.sum(values * weights).div(torch.sum(weights)).item())
```

### Model Artifact Identity
Code-defined models (no `.pt` file) use file hash as artifact identity:
```python
@lru_cache(maxsize=1)
def model_artifact_sha256() -> str:
    with Path(__file__).open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()
```

---

## Logging Patterns

### Logger Initialization
```python
logger = logging.getLogger(__name__)
```
One logger per module, always at module level.

### Log Levels
- `logger.warning(...)` for rejected/failed persistence (expected failures)
- `logger.exception(...)` for unexpected exceptions in background loops
- `logger.info(...)` for startup/initialization milestones
- `logger.error(...)` for conditions that require operator attention

### Log Message Format
Use `%s` formatting (not f-strings) in logger calls:
```python
logger.warning("Failed to persist market structure observation for %s: %s", symbol, exc)
```

---

## Dataclass Patterns

Lightweight value objects use `@dataclass` with typed fields and `field(default_factory=list)` for mutable defaults:
```python
@dataclass
class ClosePrediction:
    index: str
    pin_strike: float
    spot_price: float
    expected_close: float
    signals: List[CloseSignal] = field(default_factory=list)
    net_bias: str = 'neutral'
    confidence: float = 0.5
```

---

## Dependency Injection for Testability

Classes that interact with the database accept injected callables:
```python
class MarketStructureJournal:
    def __init__(
        self,
        *,
        saver: Callable[[Mapping[str, Any]], Any] = save_market_structure_observation,
        loader: Callable[..., list[dict[str, Any]]] = load_market_structure_observations,
        now_utc: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
```
This enables pure in-memory unit tests without database setup.

---

## Environment Configuration

- All configuration from `.env` via `python-dotenv` or `pydantic-settings`
- Runtime tuning via `os.getenv()` with safe defaults:
```python
ORB_REFERENCE_CADENCE_SECONDS = max(1, int(os.getenv("DATABENTO_ORB_REFERENCE_INTERVAL_SECONDS", "5")))
```
- Never hardcode API keys, credentials, or environment-specific paths
- `DATABASE_URL` resolved to absolute path at startup

---

## Testing Conventions

- Test files in `tests/` named `test_<module>.py`
- PowerShell integration tests named `*.Tests.ps1`
- `PYTHONPATH = ["."]` configured in `pyproject.toml` — no `sys.path` manipulation in tests
- Tests use injected savers/loaders for DB isolation (no real SQLite in unit tests)
- Closing-tape tests use `closing_tape_authority_helpers.py` for shared fixtures

---

## File Naming and Organization

- Backend domain modules: flat in `backend/` (e.g., `ai_predictor.py`, `market_structure.py`)
- Closing-tape sub-pipeline: `backend/closing_tape/` package (40+ modules)
- Monitor modules: `backend/monitor_*.py` naming convention
- Operator CLI tools: `tools/` directory, standalone scripts
- App services: `app/services/` with `*_view.py` suffix for read-only view services
