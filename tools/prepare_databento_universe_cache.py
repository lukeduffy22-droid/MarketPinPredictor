"""Prepare the Databento OPRA universe before a controlled backend start.

This tool uses Databento Historical metadata only when a valid current-day
cache is absent.  It never starts the live client or applies a subscription to
an existing process.  A bounded prior-session cache is accepted only as an
explicitly labeled fallback so launchers cannot mistake it for current-day
discovery.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Mapping
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

_PREPARATION_STARTED = time.monotonic()


def _emit_phase(phase: str, event: str, *, phase_started: float, **fields) -> None:
    """Retain a safe progress boundary even if the supervisor kills this child."""
    now = time.monotonic()
    record = {
        "schema": "marketpin-universe-preparation-phase.v1",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "phase": phase,
        "event": event,
        "elapsed_seconds": round(max(0.0, now - _PREPARATION_STARTED), 6),
        "phase_elapsed_seconds": round(max(0.0, now - phase_started), 6),
        **fields,
    }
    try:
        print(json.dumps(record, sort_keys=True), file=sys.stderr, flush=True)
    except Exception:
        # Diagnostic output must not change discovery results or exceptions.
        pass


@contextmanager
def _preparation_phase(phase: str, **fields):
    started = time.monotonic()
    _emit_phase(phase, "start", phase_started=started, **fields)
    try:
        yield
    except BaseException as exc:
        # Exception messages and arguments can contain credentials or URLs.
        _emit_phase(phase, "error", phase_started=started,
                    error_type=type(exc).__name__, **fields)
        raise
    else:
        _emit_phase(phase, "complete", phase_started=started, **fields)


if __name__ == "__main__":
    with _preparation_phase("dependency_import"):
        from backend.databento_streamer import (
            MIN_PAIRED_QUOTES,
            DatabentoGammaStreamer,
            current_market_date,
        )
else:
    from backend.databento_streamer import (
        MIN_PAIRED_QUOTES,
        DatabentoGammaStreamer,
        current_market_date,
    )


class _DiagnosticPreparationStreamer(DatabentoGammaStreamer):
    """Trace only this maintenance tool; preserve the live streamer unchanged."""

    def __init__(self, *args, **kwargs):
        with _preparation_phase("streamer_construction"):
            super().__init__(*args, **kwargs)

    def _load_cached_universe(self, *args, **kwargs):
        with _preparation_phase("current_cache_validation"):
            return super()._load_cached_universe(*args, **kwargs)

    def _load_prior_cached_universe(self, *args, **kwargs):
        with _preparation_phase("prior_cache_validation"):
            return super()._load_prior_cached_universe(*args, **kwargs)

    def stage_current_day_universe_cache(self, *args, **kwargs):
        with _preparation_phase("current_day_provider_staging"):
            return super().stage_current_day_universe_cache(*args, **kwargs)

    def stage_prior_session_universe_cache(self, *args, **kwargs):
        with _preparation_phase("prior_session_provider_staging"):
            return super().stage_prior_session_universe_cache(*args, **kwargs)

    def _available_end(self, historical, schema):
        with _preparation_phase("provider_metadata", schema_name=schema):
            return super()._available_end(historical, schema)

    def _fetch_definition_universe(self, historical, config, **kwargs):
        with _preparation_phase("market_definitions", market=config.label):
            return super()._fetch_definition_universe(historical, config, **kwargs)

    def _fetch_parent_open_interest(self, historical, config, candidate_symbols, **kwargs):
        with _preparation_phase("market_parent_open_interest", market=config.label):
            return super()._fetch_parent_open_interest(
                historical, config, candidate_symbols, **kwargs
            )

    def _has_required_primary_pair_coverage(self, *args, **kwargs):
        with _preparation_phase("primary_pair_admission"):
            return super()._has_required_primary_pair_coverage(*args, **kwargs)

    def _save_cached_universe(self, *args, **kwargs):
        with _preparation_phase("atomic_cache_save"):
            return super()._save_cached_universe(*args, **kwargs)


CURRENT_DAY_LABEL = "CURRENT_DAY_CACHE"
PRIOR_SESSION_FALLBACK_LABEL = "PRIOR_SESSION_FALLBACK"
PROVIDER_ALLOWED_MODE = "provider_allowed"
CACHE_ONLY_MODE = "cache_only"


def _safe_nonnegative_int(value: object) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError, OverflowError):
        return 0


def _market_primary_readiness(
    streamer: DatabentoGammaStreamer,
    symbols: list[str],
) -> dict[str, dict[str, object]]:
    """Emit the compact evidence needed to prove every requested primary plan.

    Core universe admission intentionally remains SPX/NDX-only.  The pre-open
    four-index launcher has a stronger requirement: every requested family must
    be present and subscription-ready before a current-day pre-stage receipt can
    authorize a later process replacement.  Keep that stronger proof outside
    the streamer's core/optional admission policy.
    """
    metadata = streamer.subscription_metadata
    market_plans = metadata.get("markets") if isinstance(metadata, Mapping) else {}
    if not isinstance(market_plans, Mapping):
        market_plans = {}
    diagnostics = getattr(streamer, "primary_pair_admission_diagnostics", {})
    if not isinstance(diagnostics, Mapping):
        diagnostics = {}

    evidence: dict[str, dict[str, object]] = {}
    for raw_symbol in symbols:
        symbol = str(raw_symbol).strip().upper()
        if not symbol or symbol in evidence:
            continue
        raw_plan = market_plans.get(symbol)
        plan = raw_plan if isinstance(raw_plan, Mapping) else {}
        raw_admission = plan.get("primary_pair_admission")
        if not isinstance(raw_admission, Mapping):
            raw_admission = diagnostics.get(symbol)
        admission = raw_admission if isinstance(raw_admission, Mapping) else {}
        primary_entries = [
            entry
            for entry in (plan.get("selected_expirations") or [])
            if isinstance(entry, Mapping) and entry.get("role") == "primary"
        ]
        primary = primary_entries[0] if len(primary_entries) == 1 else {}
        orb_reference_minimum_pair_count = max(
            5,
            _safe_nonnegative_int(MIN_PAIRED_QUOTES),
        )
        evidence[symbol] = {
            "subscription_available": plan.get("subscription_available") is True,
            "admission_passes": admission.get("passes") is True,
            "primary_plan_count": len(primary_entries),
            "primary_expiration": primary.get("expiration"),
            "primary_contract_count": _safe_nonnegative_int(primary.get("contracts")),
            "selected_strike_pairs": _safe_nonnegative_int(
                primary.get("selected_strike_pairs")
            ),
            "minimum_pair_count": _safe_nonnegative_int(
                admission.get("minimum_pair_count")
            ),
            "complete_pair_count": _safe_nonnegative_int(
                admission.get("complete_pair_count")
            ),
            "orb_reference_minimum_pair_count": (
                orb_reference_minimum_pair_count
            ),
            "primary_expiration_authority": plan.get(
                "primary_expiration_authority"
            ),
            "primary_expiration_context_only": plan.get(
                "primary_expiration_context_only"
            ),
            "primary_expiration_same_day_authority": plan.get(
                "primary_expiration_same_day_authority"
            ),
            "primary_expiration_selection_basis": plan.get(
                "primary_expiration_selection_basis"
            ),
        }
    return evidence


def _market_evidence_fields(
    streamer: DatabentoGammaStreamer,
    symbols: list[str],
) -> dict[str, object]:
    requested_symbols = list(
        dict.fromkeys(
            str(symbol).strip().upper()
            for symbol in symbols
            if str(symbol).strip()
        )
    )
    return {
        "requested_symbols": requested_symbols,
        "market_primary_readiness": _market_primary_readiness(
            streamer,
            requested_symbols,
        ),
    }


def _safe_error_message(error: Exception, api_key: str | None) -> str:
    message = str(error).strip() or error.__class__.__name__
    if api_key:
        message = message.replace(api_key, "<redacted>")
    return f"{error.__class__.__name__}: {message}"


def prepare_current_day_universe_cache(
    *,
    symbols: list[str],
    trading_date: date | None = None,
    cache_only: bool = False,
    streamer_factory: Callable[[list[str]], DatabentoGammaStreamer] = _DiagnosticPreparationStreamer,
) -> dict[str, object]:
    """Return a launch-safe cache decision without opening a live session.

    ``cache_only`` is the post-timeout validation path.  It may read and apply
    already-published cache files, but it cannot enter either provider staging
    method (and therefore cannot construct a Databento Historical client).
    """
    target_date = trading_date or current_market_date()
    preparation_mode = CACHE_ONLY_MODE if cache_only else PROVIDER_ALLOWED_MODE
    streamer = streamer_factory(symbols)

    if streamer._load_cached_universe(
        target_date,
        ignore_refresh_flag=cache_only,
    ):
        provenance = dict(streamer.subscription_metadata.get("universe_provenance") or {})
        return {
            "status": "ready",
            "preparation_mode": preparation_mode,
            "provenance_label": CURRENT_DAY_LABEL,
            "current_day_cache_ready": True,
            "selected_contract_count": len(streamer.live_symbols),
            "selected_universe_sha256": streamer.subscription_metadata.get(
                "selected_universe_sha256"
            ),
            "provenance": provenance,
            **_market_evidence_fields(streamer, symbols),
        }

    if cache_only:
        # Keep this branch above every provider-backed staging call.  The
        # bounded PowerShell supervisor uses it only after the first child has
        # exited or been terminated and the cache directory is quiescent.
        failure_reason = (
            "cache-only validation found no valid current-day cache; "
            "provider discovery is disabled"
        )
        if streamer._load_prior_cached_universe(
            trading_date=target_date,
            reason=failure_reason,
        ):
            provenance = dict(
                streamer.subscription_metadata.get("universe_provenance") or {}
            )
            return {
                "status": "fallback",
                "preparation_mode": CACHE_ONLY_MODE,
                "provenance_label": PRIOR_SESSION_FALLBACK_LABEL,
                "current_day_cache_ready": False,
                "selected_contract_count": len(streamer.live_symbols),
                "selected_universe_sha256": streamer.subscription_metadata.get(
                    "selected_universe_sha256"
                ),
                "provenance": provenance,
                "warning": failure_reason,
                **_market_evidence_fields(streamer, symbols),
            }
        raise RuntimeError(
            f"No launch-safe cached Databento universe is available for "
            f"{target_date}; provider discovery is disabled in cache-only mode"
        )

    stage_error: Exception | None = None
    staged_provenance: dict[str, object] | None = None
    current_day_failure_diagnostics: dict[str, object] = {}
    try:
        staged_provenance = streamer.stage_current_day_universe_cache(
            trading_date=target_date
        )
        if not streamer._load_cached_universe(target_date, ignore_refresh_flag=True):
            raise RuntimeError(
                f"staged cache for {target_date} did not pass current-day load validation"
            )
        provenance = dict(streamer.subscription_metadata.get("universe_provenance") or {})
        return {
            "status": "staged",
            "preparation_mode": PROVIDER_ALLOWED_MODE,
            "provenance_label": CURRENT_DAY_LABEL,
            "current_day_cache_ready": True,
            "selected_contract_count": len(streamer.live_symbols),
            "selected_universe_sha256": streamer.subscription_metadata.get(
                "selected_universe_sha256"
            ),
            "provenance": provenance,
            "staged_provenance": staged_provenance,
            **_market_evidence_fields(streamer, symbols),
        }
    except Exception as exc:  # provider/cache failures are handled by the bounded fallback gate
        stage_error = exc
        current_day_failure_diagnostics = {
            market: dict(values)
            for market, values in getattr(
                streamer, "definition_discovery_metadata", {}
            ).items()
        }

    failure_reason = (
        "controlled pre-start current-day cache preparation failed: "
        + _safe_error_message(stage_error, streamer.api_key)
    )
    if streamer._load_prior_cached_universe(
        trading_date=target_date,
        reason=failure_reason,
    ):
        provenance = dict(streamer.subscription_metadata.get("universe_provenance") or {})
        return {
            "status": "fallback",
            "preparation_mode": PROVIDER_ALLOWED_MODE,
            "provenance_label": PRIOR_SESSION_FALLBACK_LABEL,
            "current_day_cache_ready": False,
            "selected_contract_count": len(streamer.live_symbols),
            "selected_universe_sha256": streamer.subscription_metadata.get(
                "selected_universe_sha256"
            ),
            "provenance": provenance,
            "current_day_failure_diagnostics": current_day_failure_diagnostics,
            "warning": failure_reason,
            **_market_evidence_fields(streamer, symbols),
        }

    prior_stage_error: Exception | None = None
    try:
        staged_fallback_provenance = streamer.stage_prior_session_universe_cache(
            trading_date=target_date
        )
        prior_reason = (
            f"{failure_reason}; staged provider-complete prior session "
            f"{staged_fallback_provenance.get('source_date')} as an explicit fallback"
        )
        if not streamer._load_prior_cached_universe(
            trading_date=target_date,
            reason=prior_reason,
        ):
            raise RuntimeError(
                "staged prior-session cache did not pass target-date fallback validation"
            )
        provenance = dict(streamer.subscription_metadata.get("universe_provenance") or {})
        return {
            "status": "fallback",
            "preparation_mode": PROVIDER_ALLOWED_MODE,
            "provenance_label": PRIOR_SESSION_FALLBACK_LABEL,
            "current_day_cache_ready": False,
            "selected_contract_count": len(streamer.live_symbols),
            "selected_universe_sha256": streamer.subscription_metadata.get(
                "selected_universe_sha256"
            ),
            "provenance": provenance,
            "current_day_failure_diagnostics": current_day_failure_diagnostics,
            "staged_fallback_provenance": staged_fallback_provenance,
            "warning": prior_reason,
            **_market_evidence_fields(streamer, symbols),
        }
    except Exception as exc:
        prior_stage_error = exc

    raise RuntimeError(
        f"No launch-safe Databento universe is available for {target_date}; "
        f"{failure_reason}; provider prior-session staging also failed: "
        f"{_safe_error_message(prior_stage_error, streamer.api_key)}"
    ) from stage_error


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", default="SPX,NDX,VIX")
    parser.add_argument("--trading-date", type=date.fromisoformat)
    parser.add_argument(
        "--cache-only",
        action="store_true",
        help="validate existing current/prior cache files without provider discovery",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    symbols = [value.strip().upper() for value in args.symbols.split(",") if value.strip()]
    try:
        with _preparation_phase("preparation", preparation_mode=(
            CACHE_ONLY_MODE if args.cache_only else PROVIDER_ALLOWED_MODE
        )):
            result = prepare_current_day_universe_cache(
                symbols=symbols,
                trading_date=args.trading_date,
                cache_only=args.cache_only,
            )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "preparation_mode": (
                        CACHE_ONLY_MODE if args.cache_only else PROVIDER_ALLOWED_MODE
                    ),
                    "provenance_label": "NO_LAUNCH_SAFE_UNIVERSE",
                    "current_day_cache_ready": False,
                    "error": _safe_error_message(exc, None),
                },
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
