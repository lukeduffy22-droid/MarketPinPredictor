"""Shared local governance credential configuration; never return secrets in reports."""
import os
from pathlib import Path
import secrets

from dotenv import dotenv_values, set_key

ROOT = Path(__file__).resolve().parents[1]
KEY = 'MARKETPIN_GOVERNANCE_ACCESS_TOKEN'


def governance_token(root=None):
    root = ROOT if root is None else root
    environment = os.getenv(KEY, '').strip()
    configured = str(dotenv_values(Path(root) / '.env').get(KEY) or '').strip()
    if environment and configured and environment != configured:
        raise ValueError('GOVERNANCE_CREDENTIAL_CONFLICT')
    token = environment or configured
    if token and len(token) < 16:
        raise ValueError('GOVERNANCE_CREDENTIAL_TOO_SHORT')
    return token


def ensure_governance_token(root=None):
    """Explicit launcher/setup operation. Reuse existing value; never rotate silently."""
    root = Path(ROOT if root is None else root)
    lock = root / '.governance-setup.lock'
    guard = lock.open('x')
    try:
        token = governance_token(root)
        created = not bool(token)
        token = token or secrets.token_urlsafe(32)
        path = root / '.env'
        if not str(dotenv_values(path).get(KEY) or '').strip():
            set_key(str(path), KEY, token)
        return {'status': 'CONFIGURED', 'created': created, 'runtime_loading': 'NOT_VERIFIED'}
    finally:
        guard.close()
        lock.unlink(missing_ok=True)
