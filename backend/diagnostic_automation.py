"""Post-close operational reviews. No prediction, trading or repair authority."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import sqlite3

from app.services.diagnostic_agent import build_packet, diagnose
from app.services.diagnostic_history import CT, save_review
from app.services.advisor_workflow import inspect_capabilities
from backend.forecast_calendar import session_bounds

ROOT = Path(__file__).resolve().parents[1]
LOGGER = logging.getLogger(__name__)
QUESTION = ('Review this completed capture session against the failure register. '
            'Distinguish whole-session and late-session results, historical incidents, '
            'missing evidence, and proposed corrections. Compare previous_session if present; '
            'do not claim deployment, incident closure, continuous capture or improved prediction accuracy '
            'without the specific evidence. Give bounded next-session acceptance checks.')
MAX_ATTEMPTS = 2
MAX_PACKET_BYTES = 64_000


def due_session(now):
    """Latest completed reviewed-calendar session; never work during cash hours."""
    if now.tzinfo is None:
        raise ValueError('Scheduler time must include a timezone')
    now = now.astimezone(timezone.utc)
    today = now.astimezone(CT).date()
    try:
        opening, closing = session_bounds(today)
        if opening <= now < closing + timedelta(minutes=15):
            return None
    except ValueError as exc:
        if str(exc) != 'not_a_trading_session':
            return None
    for offset in range(10):
        day = today - timedelta(days=offset)
        try:
            _, closing = session_bounds(day)
        except ValueError:
            continue
        if now >= closing + timedelta(minutes=15):
            return day.isoformat()
    return None


def _connect(root):
    directory = Path(root) / 'data/diagnostic_automation'
    directory.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(directory / 'jobs.sqlite', timeout=2)
    connection.row_factory = sqlite3.Row
    connection.execute('''CREATE TABLE IF NOT EXISTS jobs (
        session_date TEXT PRIMARY KEY, packet_json TEXT NOT NULL,
        packet_sha256 TEXT NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_utc TEXT, review_path TEXT, error_kind TEXT, updated_at_utc TEXT NOT NULL)''')
    return connection


def _write_once(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open('xb') as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        if path.read_bytes() != content:
            raise ValueError('Existing diagnostic evidence differs from its identity')


def collect_packet(day, root):
    """Bounded runtime poll plus retained files; failures stay explicit."""
    import requests
    context = {'timestamp_utc': datetime.now(timezone.utc).isoformat(),
               'requested_symbols': ['SPX', 'NDX', 'RUT', 'VIX'], 'pipeline': {}}
    try:
        response = requests.get('http://127.0.0.1:8000/health/live', timeout=3)
        response.raise_for_status()
        context['pipeline'] = response.json()
    except (requests.RequestException, ValueError):
        context['timestamp_utc'] = None
    capabilities = inspect_capabilities(root)
    packet = build_packet(context, capabilities, root, day)
    packet['evidence']['automation'] = {
        'policy': 'post-close-v1', 'pid': os.getpid(), 'research_only': True,
        'meaning': 'Backend evidence capture only; no code changes or incident closure.',
    }
    with _connect(root) as connection:
        previous = connection.execute(
            'SELECT session_date, packet_json FROM jobs WHERE session_date < ? '
            'ORDER BY session_date DESC LIMIT 1', (day,)).fetchone()
    if previous:
        old = json.loads(previous['packet_json'])
        packet['evidence']['previous_session'] = {
            'session_date_ct': previous['session_date'],
            'captures': {k: v for k, v in old['evidence'].items() if k.startswith('capture:')},
            'meaning': 'Previous archived session evidence, not current runtime or proof of a fix.',
        }
    return packet


def run_once(root=ROOT, *, now=None, packet_factory=collect_packet,
             reviewer=diagnose, api_available=None, evidence_only=False):
    """Persist evidence first; serialize paid attempts across processes/restarts.

    An interrupted IN_FLIGHT job is deliberately not retried automatically: the
    provider may already have charged/completed it. Only explicit 429/5xx responses
    are retried, once, after 15 minutes. Network timeouts remain ambiguous.
    """
    now = now or datetime.now(timezone.utc)
    day = due_session(now)
    if day is None:
        return {'status': 'NOT_DUE'}
    now = now.astimezone(timezone.utc)
    stamp = now.astimezone(timezone.utc).isoformat()
    connection = _connect(root)
    try:
        job = connection.execute('SELECT * FROM jobs WHERE session_date=?', (day,)).fetchone()
        if job is None:
            packet = packet_factory(day, root)
            if packet.get('session_date_ct') != day or packet.get('research_only') is not True:
                raise ValueError('Evidence session or authority mismatch')
            raw = json.dumps(packet, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
            digest = hashlib.sha256(raw).hexdigest()
            _write_once(Path(root) / 'data/diagnostic_automation' / day / f'{digest}.evidence.json', raw)
            with connection:
                connection.execute('''INSERT OR IGNORE INTO jobs
                    (session_date,packet_json,packet_sha256,status,updated_at_utc)
                    VALUES (?,?,?,'EVIDENCE_SAVED',?)''', (day, raw.decode(), digest, stamp))
        connection.execute('BEGIN IMMEDIATE')
        job = dict(connection.execute('SELECT * FROM jobs WHERE session_date=?', (day,)).fetchone())
        if job['status'] in ('COMPLETE', 'IN_FLIGHT', 'FAILED', 'INPUT_LIMIT'):
            connection.rollback()
            return {'session_date': day, 'status': job['status']}
        raw = job['packet_json'].encode()
        if hashlib.sha256(raw).hexdigest() != job['packet_sha256']:
            raise ValueError('Stored evidence hash mismatch')
        available = bool(os.getenv('OPENAI_API_KEY')) if api_available is None else api_available
        if len(raw) > MAX_PACKET_BYTES:
            connection.execute("UPDATE jobs SET status='INPUT_LIMIT', updated_at_utc=? WHERE session_date=?", (stamp, day))
            connection.commit()
            return {'session_date': day, 'status': 'INPUT_LIMIT'}
        if evidence_only or not available:
            connection.rollback()
            return {'session_date': day, 'status': 'EVIDENCE_SAVED' if evidence_only else 'AWAITING_API_KEY'}
        if job['next_attempt_utc'] and stamp < job['next_attempt_utc']:
            connection.rollback()
            return {'session_date': day, 'status': 'RETRY_WAIT'}
        if job['attempts'] >= MAX_ATTEMPTS:
            connection.rollback()
            return {'session_date': day, 'status': 'FAILED'}
        attempt = job['attempts'] + 1
        connection.execute("UPDATE jobs SET status='IN_FLIGHT', attempts=?, updated_at_utc=? WHERE session_date=?",
                           (attempt, stamp, day))
        connection.commit()
        try:
            result = reviewer(QUESTION, json.loads(raw))
            if result.get('packet') != json.loads(raw) or result.get('execution_status') != 'PROPOSED_ONLY':
                raise ValueError('Review does not bind the retained evidence')
            path = save_review(result, root)
        except Exception as exc:
            code = getattr(exc, 'status_code', None)
            retry = (code == 429 or isinstance(code, int) and 500 <= code < 600) and attempt < MAX_ATTEMPTS
            status = 'RETRY_WAIT' if retry else 'FAILED'
            # Never persist provider exception strings: they can contain credentials/body data.
            with connection:
                connection.execute('UPDATE jobs SET status=?, next_attempt_utc=?, error_kind=?, updated_at_utc=? WHERE session_date=?',
                    (status, (now + timedelta(minutes=15)).isoformat() if retry else None,
                     type(exc).__name__, stamp, day))
            return {'session_date': day, 'status': status, 'error_kind': type(exc).__name__}
        with connection:
            connection.execute("UPDATE jobs SET status='COMPLETE',review_path=?,error_kind=NULL,updated_at_utc=? WHERE session_date=?",
                               (str(path), stamp, day))
        return {'session_date': day, 'status': 'COMPLETE', 'review_path': str(path)}
    finally:
        connection.close()


def read_job_status(root, day):
    path = Path(root) / 'data/diagnostic_automation/jobs.sqlite'
    if not path.is_file():
        return None
    connection = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=2)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute('SELECT session_date,status,attempts,error_kind,updated_at_utc,review_path '
                                 'FROM jobs WHERE session_date=?', (day,)).fetchone()
        return dict(row) if row else None
    finally:
        connection.close()


async def run_diagnostic_review_loop(root=ROOT):
    if os.getenv('MARKETPIN_POSTCLOSE_REVIEW_ENABLED', '1').lower() in ('0', 'false', 'no'):
        return
    last_result = None
    while True:
        try:
            result = await asyncio.to_thread(run_once, root)
            if result != last_result:
                LOGGER.info('Post-close review: %s', result)
                last_result = result
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.warning('Post-close review unavailable (%s); capture remains independent', type(exc).__name__)
        await asyncio.sleep(60)
