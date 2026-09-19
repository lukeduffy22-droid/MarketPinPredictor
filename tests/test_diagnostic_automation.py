from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
import sqlite3
import threading

import pytest

from backend.diagnostic_automation import due_session, run_once, read_job_status
from app.services.diagnostic_history import load_reviews

NOW = datetime(2026, 9, 17, 20, 20, tzinfo=timezone.utc)


def packet(day, root):
    return {'schema_version': 'diagnostic-evidence-v1', 'session_date_ct': day,
            'research_only': True, 'evidence': {'runtime': {}}}


def review(question, evidence):
    return {'schema_version': 'diagnostic-review-v1', 'research_only': True,
            'generated_at_utc': NOW.isoformat(), 'execution_status': 'PROPOSED_ONLY',
            'packet': evidence, 'review': {'summary': 'Review only', 'uncertainties': [], 'change_requests': []}}


def test_calendar_regular_close_early_close_weekend_and_catchup():
    assert due_session(NOW) == '2026-09-17'
    assert due_session(NOW.replace(hour=19)) is None
    assert due_session(NOW.replace(minute=14)) is None
    assert due_session(datetime(2026, 9, 19, 15, tzinfo=timezone.utc)) == '2026-09-18'
    assert due_session(datetime(2026, 9, 21, 12, tzinfo=timezone.utc)) == '2026-09-18'
    assert due_session(datetime(2026, 11, 27, 18, 15, tzinfo=timezone.utc)) == '2026-11-27'
    assert due_session(datetime(2040, 9, 17, 22, tzinfo=timezone.utc)) is None
    with pytest.raises(ValueError):
        due_session(datetime(2026, 9, 17))


def test_missing_key_retains_evidence_then_restart_completes_once(tmp_path):
    result = run_once(tmp_path, now=NOW, packet_factory=packet, api_available=False)
    assert result['status'] == 'AWAITING_API_KEY'
    assert read_job_status(tmp_path, '2026-09-17')['attempts'] == 0
    calls = []
    def reviewer(q, p):
        calls.append(p)
        return review(q, p)
    assert run_once(tmp_path, now=NOW, reviewer=reviewer, api_available=True)['status'] == 'COMPLETE'
    assert run_once(tmp_path, now=NOW, reviewer=reviewer, api_available=True)['status'] == 'COMPLETE'
    assert len(calls) == 1
    assert len(load_reviews(tmp_path, '2026-09-17')) == 1
    assert len(list((tmp_path / 'data/diagnostic_automation/2026-09-17').glob('*.evidence.json'))) == 1


def test_concurrent_workers_cannot_duplicate_paid_call(tmp_path):
    run_once(tmp_path, now=NOW, packet_factory=packet, evidence_only=True)
    started, release = threading.Event(), threading.Event()
    def reviewer(q, p):
        started.set()
        assert release.wait(5)
        return review(q, p)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(run_once, tmp_path, now=NOW, reviewer=reviewer, api_available=True)
        assert started.wait(5)
        second = run_once(tmp_path, now=NOW, reviewer=reviewer, api_available=True)
        assert second['status'] == 'IN_FLIGHT'
        release.set()
        assert first.result()['status'] == 'COMPLETE'


def test_bounded_retry_and_no_provider_error_text_saved(tmp_path):
    class Busy(Exception):
        status_code = 429
    def failed(q, p):
        raise Busy('secret-provider-text')
    assert run_once(tmp_path, now=NOW, packet_factory=packet, reviewer=failed, api_available=True)['status'] == 'RETRY_WAIT'
    assert run_once(tmp_path, now=NOW + timedelta(minutes=1), reviewer=failed, api_available=True)['status'] == 'RETRY_WAIT'
    assert read_job_status(tmp_path, '2026-09-17')['attempts'] == 1
    assert run_once(tmp_path, now=NOW + timedelta(minutes=16), reviewer=failed, api_available=True)['status'] == 'FAILED'
    assert run_once(tmp_path, now=NOW + timedelta(minutes=32), reviewer=failed, api_available=True)['status'] == 'FAILED'
    assert read_job_status(tmp_path, '2026-09-17')['attempts'] == 2
    assert 'secret-provider-text' not in (tmp_path / 'data/diagnostic_automation/jobs.sqlite').read_bytes().decode(errors='ignore')


def test_ambiguous_timeout_and_interrupted_job_are_not_retried(tmp_path):
    def timeout(q, p):
        raise TimeoutError('uncertain billing')
    assert run_once(tmp_path, now=NOW, packet_factory=packet, reviewer=timeout, api_available=True)['status'] == 'FAILED'
    with sqlite3.connect(tmp_path / 'data/diagnostic_automation/jobs.sqlite') as c:
        c.execute("UPDATE jobs SET status='IN_FLIGHT'")
    assert run_once(tmp_path, now=NOW, reviewer=review, api_available=True)['status'] == 'IN_FLIGHT'
    assert read_job_status(tmp_path, '2026-09-17')['attempts'] == 1


def test_oversized_packet_and_not_due_never_call_ai(tmp_path):
    def oversized(day, root):
        return {**packet(day, root), 'padding': 'x' * 64000}
    def unexpected(*args):
        pytest.fail('AI must not be called')
    assert run_once(tmp_path, now=NOW.replace(hour=19), reviewer=unexpected)['status'] == 'NOT_DUE'
    assert not (tmp_path / 'data').exists()
    assert run_once(tmp_path, now=NOW, packet_factory=oversized, reviewer=unexpected, api_available=True)['status'] == 'INPUT_LIMIT'


def test_loop_disable_and_cancellation(monkeypatch, tmp_path):
    import asyncio
    import backend.diagnostic_automation as automation
    calls = []
    monkeypatch.setattr(automation, 'run_once', lambda *args: calls.append(1) or {'status': 'NOT_DUE'})
    monkeypatch.setenv('MARKETPIN_POSTCLOSE_REVIEW_ENABLED', '0')
    asyncio.run(automation.run_diagnostic_review_loop(tmp_path))
    assert calls == []
    monkeypatch.setenv('MARKETPIN_POSTCLOSE_REVIEW_ENABLED', '1')
    async def check():
        task = asyncio.create_task(automation.run_diagnostic_review_loop(tmp_path))
        while not calls:
            await asyncio.sleep(.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    asyncio.run(check())
