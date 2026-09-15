import asyncio

import databento as db
import pytest

import backend.databento_streamer as streamer_module
from backend.databento_streamer import (
    DatabentoGammaStreamer,
    _abort_pre_auth_transport_once,
    _install_pre_auth_transport_guard,
)


class _FakeTransport:
    def __init__(self):
        self.abort_calls = 0

    def abort(self):
        self.abort_calls += 1

    def write(self, _data):
        return None


class _FakeProtocol:
    def __init__(self, transport):
        self.transport = transport


def test_rejected_authentication_aborts_only_own_transport_and_consumes_error():
    async def exercise():
        transport = _FakeTransport()
        protocol = _FakeProtocol(transport)
        authenticated = asyncio.get_running_loop().create_future()
        events = []
        error = RuntimeError("quota rejected")
        authenticated.set_exception(error)

        _abort_pre_auth_transport_once(
            protocol,
            authenticated,
            on_event=lambda outcome, reason: events.append((outcome, reason)),
        )

        assert authenticated.exception() is error
        assert transport.abort_calls == 1
        assert events == [
            ("transport_aborted", "authentication_failed:RuntimeError")
        ]

    asyncio.run(exercise())


def test_authentication_timeout_cancellation_aborts_transport():
    async def exercise():
        transport = _FakeTransport()
        protocol = _FakeProtocol(transport)
        authenticated = asyncio.get_running_loop().create_future()
        events = []
        authenticated.cancel()

        _abort_pre_auth_transport_once(
            protocol,
            authenticated,
            on_event=lambda outcome, reason: events.append((outcome, reason)),
        )

        assert transport.abort_calls == 1
        assert events == [("transport_aborted", "authentication_cancelled")]

    asyncio.run(exercise())


def test_wait_for_timeout_triggers_registered_abort_callback():
    async def exercise():
        transport = _FakeTransport()
        protocol = _FakeProtocol(transport)
        authenticated = asyncio.get_running_loop().create_future()
        events = []
        authenticated.add_done_callback(
            lambda future: _abort_pre_auth_transport_once(
                protocol,
                future,
                on_event=lambda outcome, reason: events.append((outcome, reason)),
            )
        )

        with pytest.raises(TimeoutError):
            await asyncio.wait_for(authenticated, timeout=0.001)
        await asyncio.sleep(0)

        assert authenticated.cancelled()
        assert transport.abort_calls == 1
        assert events == [("transport_aborted", "authentication_cancelled")]

    asyncio.run(exercise())


def test_successful_authentication_never_aborts_transport():
    async def exercise():
        transport = _FakeTransport()
        protocol = _FakeProtocol(transport)
        authenticated = asyncio.get_running_loop().create_future()
        events = []
        authenticated.set_result("session-id")

        _abort_pre_auth_transport_once(
            protocol,
            authenticated,
            on_event=lambda outcome, reason: events.append((outcome, reason)),
        )

        assert transport.abort_calls == 0
        assert events == []

    asyncio.run(exercise())


def test_duplicate_failed_auth_callback_is_idempotent():
    async def exercise():
        transport = _FakeTransport()
        protocol = _FakeProtocol(transport)
        authenticated = asyncio.get_running_loop().create_future()
        events = []
        authenticated.set_exception(RuntimeError("rejected"))

        for _ in range(2):
            _abort_pre_auth_transport_once(
                protocol,
                authenticated,
                on_event=lambda outcome, reason: events.append((outcome, reason)),
            )

        assert transport.abort_calls == 1
        assert events == [
            ("transport_aborted", "authentication_failed:RuntimeError")
        ]

    asyncio.run(exercise())


def test_telemetry_callback_failure_cannot_poison_sdk_callback():
    async def exercise():
        transport = _FakeTransport()
        protocol = _FakeProtocol(transport)
        authenticated = asyncio.get_running_loop().create_future()
        authenticated.set_exception(RuntimeError("rejected"))

        def broken_telemetry(_outcome, _reason):
            raise RuntimeError("telemetry unavailable")

        _abort_pre_auth_transport_once(
            protocol,
            authenticated,
            on_event=broken_telemetry,
        )

        assert transport.abort_calls == 1

    asyncio.run(exercise())


def test_installed_sdk_guard_wraps_one_client_and_is_idempotent():
    async def exercise():
        client = db.Live(key="offline-test-key", reconnect_policy="none")
        events = []

        assert _install_pre_auth_transport_guard(
            client,
            on_event=lambda outcome, reason: events.append((outcome, reason)),
        )
        guarded_factory = client._session._create_protocol
        assert _install_pre_auth_transport_guard(client)
        assert client._session._create_protocol is guarded_factory

        protocol = guarded_factory(dataset="OPRA.PILLAR")
        transport = _FakeTransport()
        protocol.connection_made(transport)
        protocol.authenticated.set_exception(RuntimeError("rejected"))
        await asyncio.sleep(0)

        assert transport.abort_calls == 1
        assert events == [
            ("transport_aborted", "authentication_failed:RuntimeError")
        ]

    asyncio.run(exercise())


def test_client_without_sdk_internals_is_not_guarded():
    class LocalTestDouble:
        pass

    client = LocalTestDouble()

    assert not _install_pre_auth_transport_guard(client)
    assert not isinstance(client, streamer_module._DATABENTO_SDK_LIVE_CLASS)


def test_real_sdk_guard_contract_drift_fails_before_subscribe(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.api_key = "offline-test-key"
    streamer.replay_minutes = 0
    streamer.live_symbols = ["SPXW  260910C06500000"]
    streamer.is_running = True
    subscribe_calls = []

    monkeypatch.setattr(
        streamer,
        "_subscription_window",
        lambda: {"state": "regular_session", "subscription_allowed": True},
    )
    monkeypatch.setattr(streamer, "_build_universe", lambda **_kwargs: None)
    monkeypatch.setattr(
        streamer_module,
        "_install_pre_auth_transport_guard",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        streamer_module._DATABENTO_SDK_LIVE_CLASS,
        "subscribe",
        lambda *_args, **_kwargs: subscribe_calls.append(True),
    )

    def stop_after_guard_failure(_seconds):
        streamer.is_running = False

    monkeypatch.setattr(streamer_module.time, "sleep", stop_after_guard_failure)

    streamer._run_live()

    assert subscribe_calls == []
    assert streamer.pre_auth_transport_guard_status == "unavailable"
    assert streamer.last_error == "DATABENTO_PRE_AUTH_TRANSPORT_GUARD_UNAVAILABLE"


def test_pre_auth_guard_events_are_visible_in_health():
    streamer = DatabentoGammaStreamer(["SPX"])

    streamer._record_pre_auth_transport_event(
        "transport_aborted", "authentication_cancelled"
    )
    streamer._record_pre_auth_transport_event(
        "transport_abort_error", "authentication_failed:BentoError:OSError"
    )
    health = streamer._connection_lifecycle_health()

    assert health["pre_auth_transport_aborts_total"] == 1
    assert health["pre_auth_transport_abort_failures_total"] == 1
    assert health["last_pre_auth_transport_event"] == "transport_abort_error"
    assert health["last_pre_auth_transport_reason"] == (
        "authentication_failed:BentoError:OSError"
    )
    assert health["last_pre_auth_transport_event_utc"] is not None
