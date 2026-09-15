import asyncio

import pytest
from fastapi import HTTPException

from app.services import live_panel_view
from backend.api.routers import admin as admin_router
from backend.api.schemas import DatabentoCacheRefreshRequest


class _DummyDatabentoStreamer:
    def __init__(self, cache_path, *, is_running):
        self._test_cache_path = cache_path
        self.is_running = is_running

    def _cache_path(self):
        return self._test_cache_path


def _call_refresh(*, confirm=False, force_maintenance=False):
    return asyncio.run(
        admin_router.refresh_databento_cache(
            DatabentoCacheRefreshRequest(
                confirm=confirm,
                force_maintenance=force_maintenance,
            )
        )
    )


def test_refresh_cache_does_not_delete_without_confirmation(tmp_path, monkeypatch):
    cache_path = tmp_path / "universe.csv"
    cache_path.write_text("symbol\nSPXW\n", encoding="utf-8")
    monkeypatch.setattr(
        admin_router,
        "get_streamer",
        lambda: _DummyDatabentoStreamer(cache_path, is_running=False),
    )

    with pytest.raises(HTTPException) as exc:
        _call_refresh(confirm=False, force_maintenance=True)

    assert exc.value.status_code == 400
    assert "confirmation" in str(exc.value.detail).lower()
    assert cache_path.exists()


def test_refresh_cache_does_not_delete_during_active_stream(tmp_path, monkeypatch):
    cache_path = tmp_path / "universe.csv"
    cache_path.write_text("symbol\nSPXW\n", encoding="utf-8")
    monkeypatch.setattr(
        admin_router,
        "get_streamer",
        lambda: _DummyDatabentoStreamer(cache_path, is_running=True),
    )

    with pytest.raises(HTTPException) as exc:
        _call_refresh(confirm=True)

    assert exc.value.status_code == 409
    assert "streamer is running" in str(exc.value.detail).lower()
    assert cache_path.exists()


def test_refresh_cache_fails_closed_when_stream_state_is_unknown(tmp_path, monkeypatch):
    cache_path = tmp_path / "universe.csv"
    cache_path.write_text("symbol\nSPXW\n", encoding="utf-8")
    monkeypatch.setattr(
        admin_router,
        "get_streamer",
        lambda: _DummyDatabentoStreamer(cache_path, is_running=None),
    )

    with pytest.raises(HTTPException) as exc:
        _call_refresh(confirm=True)

    assert exc.value.status_code == 409
    assert "state is unknown" in str(exc.value.detail).lower()
    assert cache_path.exists()


def test_refresh_cache_deletes_only_after_confirmed_inactive_approval(tmp_path, monkeypatch):
    cache_path = tmp_path / "universe.csv"
    cache_path.write_text("symbol\nSPXW\n", encoding="utf-8")
    monkeypatch.setattr(
        admin_router,
        "get_streamer",
        lambda: _DummyDatabentoStreamer(cache_path, is_running=False),
    )

    result = _call_refresh(confirm=True)

    assert result["deleted"] is True
    assert result["forced_while_running"] is False
    assert result["restart_required"] is True
    assert not cache_path.exists()


def test_active_stream_override_requires_explicit_force_maintenance(tmp_path, monkeypatch):
    cache_path = tmp_path / "universe.csv"
    cache_path.write_text("symbol\nSPXW\n", encoding="utf-8")
    monkeypatch.setattr(
        admin_router,
        "get_streamer",
        lambda: _DummyDatabentoStreamer(cache_path, is_running=True),
    )

    result = _call_refresh(confirm=True, force_maintenance=True)

    assert result["deleted"] is True
    assert result["forced_while_running"] is True
    assert not cache_path.exists()


def test_dashboard_refresh_request_never_sends_force(monkeypatch):
    captured = {}

    def _post(url, **kwargs):
        captured["url"] = url
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(live_panel_view.requests, "post", _post)

    live_panel_view._request_databento_cache_refresh()

    assert captured["url"].endswith("/databento/refresh-cache")
    assert captured["json"] == {"confirm": True}
    assert "force_maintenance" not in captured["json"]


class _FakeStreamlit:
    def __init__(self, button_results):
        self.session_state = {}
        self._button_results = iter(button_results)
        self.messages = []

    def button(self, *args, **kwargs):
        return next(self._button_results)

    def error(self, message):
        self.messages.append(("error", message))

    def warning(self, message):
        self.messages.append(("warning", message))

    def info(self, message):
        self.messages.append(("info", message))

    def success(self, message):
        self.messages.append(("success", message))


def test_dashboard_cache_maintenance_requires_two_distinct_clicks(monkeypatch):
    fake_st = _FakeStreamlit([True, False, False, True, False])
    requests_sent = []

    class _Response:
        status_code = 200

        @staticmethod
        def json():
            return {"message": "Cache cleared in test."}

    monkeypatch.setattr(live_panel_view, "st", fake_st)
    monkeypatch.setattr(
        live_panel_view,
        "_request_databento_cache_refresh",
        lambda: requests_sent.append(True) or _Response(),
    )

    live_panel_view._render_databento_cache_maintenance()

    assert fake_st.session_state[live_panel_view._CACHE_REFRESH_CONFIRMATION_KEY] is True
    assert requests_sent == []

    live_panel_view._render_databento_cache_maintenance()

    assert fake_st.session_state[live_panel_view._CACHE_REFRESH_CONFIRMATION_KEY] is False
    assert requests_sent == [True]
