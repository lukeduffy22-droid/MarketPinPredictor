"""Bounded evidence for cycles that cannot produce a gamma calculation.

These receipts contain no price and never count as valid research snapshots.
"""
import json
from datetime import datetime, timezone
from pathlib import Path


def record_blocked_capture(streamer, reason, monotonic_now):
    previous = getattr(streamer, '_last_blocked_capture_receipt', None)
    if previous is not None and monotonic_now - previous < streamer.snapshot_interval:
        return
    now = datetime.now(timezone.utc)
    directory = Path(streamer.audit_dir).parent / 'capture_attempts'
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / f'{now.date()}.ndjson').open('a', encoding='utf-8') as handle:
        for symbol in streamer.symbols:
            handle.write(json.dumps({
                'schema_version': 'blocked-capture-v1', 'symbol': symbol,
                'generated_at_utc': now.isoformat(), 'validation_is_valid': False,
                'gamma_excluded_from_model': True, 'usable_for_prediction': False,
                'validation_failure_reasons': [reason],
                'subscription_generation': streamer.active_generation,
                'subscription_epoch_id': streamer.subscription_epoch_id,
                'handoff_status': streamer.handoff_status,
            }) + '\n')
    streamer._last_blocked_capture_receipt = monotonic_now
