"""Retired compatibility entrypoint for the legacy CSV gamma trainer."""

from __future__ import annotations

import sys


RETIREMENT_MESSAGE = (
    "The legacy CSV gamma trainer is retired. Use "
    "tools/evaluate_closing_tape_models.py with verified official-close labels "
    "and a replay-verified immutable surface manifest. Candidate packaging "
    "does not promote production state automatically."
)


def train_model(*_args: object, **_kwargs: object) -> None:
    """Reject legacy training calls before reading data or importing ML runtimes."""

    raise RuntimeError(RETIREMENT_MESSAGE)


def main() -> int:
    print(RETIREMENT_MESSAGE, file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
