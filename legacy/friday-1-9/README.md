# Archived `friday-1/9` compatibility code

This directory preserves superseded files from the GitHub `friday-1/9` branch
that are not part of the current MarketPinPredictor architecture. The files were
moved here unchanged during the source-only integration; they were not deleted.

The archive is retained for behavior comparison and recovery only. It is not an
active import root, it is excluded from normal pytest discovery, and Copilot
changes should not target it unless a task explicitly concerns legacy behavior.
The malformed Conda workflow is also retained here as inert reference material;
placing it outside `.github/workflows/` prevents GitHub from executing it.

Current equivalents live under the root Streamlit `app.py`, `backend/`, the
active `app/` services, `tests/`, and `tools/` trees. New production work must
preserve the observed-versus-inferred evidence boundary and fail-closed authority
contracts documented in `.github/copilot-instructions.md`.
