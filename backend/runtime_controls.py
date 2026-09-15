"""Small process-lifetime operating-system controls with explicit evidence."""

from __future__ import annotations

import os
from typing import Callable


ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001


class SystemSleepGuard:
    """Request process-lifetime system availability without changing power policy."""

    limitation = "does not override explicit lid-close, sleep, hibernate, or shutdown actions"

    def __init__(
        self,
        *,
        platform_name: str | None = None,
        setter: Callable[[int], object] | None = None,
    ) -> None:
        self.platform_name = platform_name or os.name
        self.requested = self.platform_name == "nt"
        self.active = False
        self.error: str | None = None
        self._setter = setter

    def _resolve_setter(self) -> Callable[[int], object]:
        if self._setter is None:
            import ctypes

            self._setter = ctypes.windll.kernel32.SetThreadExecutionState
        return self._setter

    def activate(self) -> bool:
        if not self.requested:
            return False
        try:
            result = int(self._resolve_setter()(ES_CONTINUOUS | ES_SYSTEM_REQUIRED) or 0)
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {str(exc)[:300]}"
            return False
        if result == 0:
            self.error = "SetThreadExecutionState returned 0"
            return False
        self.active = True
        self.error = None
        return True

    def release(self) -> None:
        if not self.active:
            return
        try:
            result = int(self._resolve_setter()(ES_CONTINUOUS) or 0)
            if result == 0:
                self.error = "SetThreadExecutionState release returned 0"
        except Exception as exc:
            self.error = f"release {type(exc).__name__}: {str(exc)[:300]}"
        finally:
            self.active = False

    def to_dict(self) -> dict[str, object]:
        return {
            "platform": self.platform_name,
            "requested": self.requested,
            "active": self.active,
            "mode": "system_required_process_lifetime" if self.requested else "not_applicable",
            "limitation": self.limitation,
            "error": self.error,
        }

    def __enter__(self):
        self.activate()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()
