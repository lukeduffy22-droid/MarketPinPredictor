from dotenv import dotenv_values
import pytest
from backend.governance_credentials import KEY, governance_token, ensure_governance_token


def test_setup_reuses_secret_and_preserves_other_configuration(tmp_path, monkeypatch):
    monkeypatch.delenv(KEY, raising=False)
    (tmp_path/'.env').write_text('UNRELATED=retained\n')
    assert ensure_governance_token(tmp_path)['created'] is True
    token=governance_token(tmp_path)
    assert len(token)>=32
    assert ensure_governance_token(tmp_path)['created'] is False
    assert governance_token(tmp_path)==token
    assert dotenv_values(tmp_path/'.env')['UNRELATED']=='retained'
    assert not (tmp_path/'.governance-setup.lock').exists()


def test_conflict_fails_without_replacing_configuration(tmp_path,monkeypatch):
    (tmp_path/'.env').write_text(KEY+'=file-secret-123456789\n')
    original=(tmp_path/'.env').read_bytes()
    monkeypatch.setenv(KEY,'environment-secret-123456789')
    with pytest.raises(ValueError,match='CONFLICT'):
        ensure_governance_token(tmp_path)
    assert (tmp_path/'.env').read_bytes()==original


def test_existing_environment_is_persisted_without_rotation(tmp_path,monkeypatch):
    monkeypatch.setenv(KEY,'existing-secret-123456789')
    assert ensure_governance_token(tmp_path)['created'] is False
    assert dotenv_values(tmp_path/'.env')[KEY]=='existing-secret-123456789'
