"""Safe, append-only AI request receipts without credentials or provider error bodies."""
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import uuid


class DiagnosticError(ValueError):
    def __init__(self, category, receipt, status_code=None):
        super().__init__(category)
        self.category = category
        self.receipt = receipt
        self.status_code = status_code


def new_receipt(packet, model):
    return {'schema_version': 'diagnostic-receipt-v1', 'attempt_id': uuid.uuid4().hex,
            'started_at_utc': datetime.now(timezone.utc).isoformat(),
            'session_date_ct': date.fromisoformat(packet['session_date_ct']).isoformat(),
            'requested_model': model, 'actual_model': None, 'outcome': 'STARTED',
            'evidence_created_at_utc': packet.get('created_at_utc'),
            'runtime_observed_at_utc': packet.get('evidence', {}).get('runtime', {}).get('observed_at_utc'),
            'packet_sha256': hashlib.sha256(json.dumps(packet, sort_keys=True, allow_nan=False).encode()).hexdigest(),
            'review_path': None}


def write_receipt(receipt, root):
    raw = (json.dumps(receipt, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()
    directory = Path(root) / 'data/diagnostic_receipts' / receipt['session_date_ct']
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / (hashlib.sha256(raw).hexdigest() + '.json')
    try:
        with path.open('xb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        if path.read_bytes() != raw:
            raise ValueError('Receipt identity conflict')
    return path


def failure_category(exc):
    code = getattr(exc, 'status_code', None)
    if code in (401, 403, 429):
        return {401: 'AUTHENTICATION_FAILED', 403: 'PERMISSION_DENIED', 429: 'RATE_LIMITED'}[code]
    if isinstance(code, int):
        return 'PROVIDER_UNAVAILABLE' if code >= 500 else 'REQUEST_REJECTED'
    name = type(exc).__name__
    if 'Timeout' in name:
        return 'TIMEOUT_OUTCOME_UNCERTAIN'
    if 'Connection' in name:
        return 'CONNECTION_FAILED'
    return 'CLIENT_ERROR'


def verified_saved_receipt(review, root):
    """Resolve the final receipt without modifying a content-addressed review."""
    embedded = review.get('request_receipt') or {}
    day = date.fromisoformat(review['packet']['session_date_ct']).isoformat()
    raw = (json.dumps(review, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()
    expected = Path(root) / 'data/diagnostic_reviews' / day / (hashlib.sha256(raw).hexdigest() + '.json')
    if not expected.is_file() or expected.read_bytes() != raw:
        return None
    for path in (Path(root) / 'data/diagnostic_receipts' / day).glob('*.json'):
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != path.stem:
            continue
        try:
            receipt = json.loads(data)
        except ValueError:
            continue
        if (receipt.get('outcome') == 'SAVED' and receipt.get('attempt_id') == embedded.get('attempt_id')
                and receipt.get('packet_sha256') == embedded.get('packet_sha256')
                and receipt.get('review_path') and Path(receipt['review_path']).resolve() == expected.resolve()):
            return receipt
    return None
