"""Deprecated Streamlit entrypoint kept only as a guard.

Use app.py from the repository root instead.
"""

from __future__ import annotations


def _main() -> None:
    raise SystemExit(
        "app_new.py is deprecated and intentionally disabled. "
        "Run `streamlit run app.py` from the repository root."
    )


if __name__ == "__main__":
    _main()
