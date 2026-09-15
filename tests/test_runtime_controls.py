from backend.runtime_controls import (
    ES_CONTINUOUS,
    ES_SYSTEM_REQUIRED,
    SystemSleepGuard,
)


def test_windows_sleep_guard_holds_and_releases_process_request():
    flags = []
    guard = SystemSleepGuard(
        platform_name="nt",
        setter=lambda value: flags.append(value) or 1,
    )

    assert guard.activate()
    assert guard.to_dict()["active"] is True
    guard.release()

    assert flags == [ES_CONTINUOUS | ES_SYSTEM_REQUIRED, ES_CONTINUOUS]
    assert guard.to_dict()["active"] is False


def test_sleep_guard_failure_is_visible_and_non_windows_is_not_claimed():
    failed = SystemSleepGuard(platform_name="nt", setter=lambda _value: 0)
    assert not failed.activate()
    assert failed.to_dict()["error"] == "SetThreadExecutionState returned 0"

    skipped = SystemSleepGuard(platform_name="posix", setter=lambda _value: 1)
    assert not skipped.activate()
    assert skipped.to_dict()["mode"] == "not_applicable"
