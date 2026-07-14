# Project Audit: Repository Drift and Databento Configuration

## What is making the project messy

1. **There are multiple runnable app entry points.**
   The repository currently contains the FastAPI backend under `app/api/main.py`, the Streamlit dashboard at `app.py`, historical variants such as `app_backup.py` and `app_new.py`, and a separate `clean_app/` tree. This makes it easy to edit or launch the wrong application path.

2. **Editor/local-history snapshots are committed.**
   The `.history/` directory contains many timestamped copies of files, including Databento and settings modules. Those files are not runtime code, but they make search results noisy and can be mistaken for the active implementation.

3. **Generated data and large local artifacts are mixed with source code.**
   CSV exports, screenshots, pasted prompt artifacts, and model outputs are present beside production modules. This increases merge noise and makes it harder to identify the canonical source files.

4. **Recent merge history imported broad, unrelated changes at once.**
   The merge commit `2d34d8d` brought in runtime files, generated snapshots, models, data, tests, and `.history/` copies together. That kind of merge makes later Databento changes look like they were overwritten because the same setup appears in several places.

## Why the Databento setup appears to be overwritten

1. **The active Databento path is backend-only.**
   Databento fallback starts from the FastAPI startup path, not from the Streamlit UI. The active chain is:
   `app/api/main.py` -> `app/ingest/rest_fallback.py` -> `app/ingest/databento_fallback.py`.

2. **The Streamlit entry point intentionally disables direct streaming.**
   `app.py` has a disabled websocket setup and expects streaming data to come from the backend API. If only the Streamlit app is launched, it will not initialize Databento polling by itself.

3. **Replit runs two processes, and the visible web app is Streamlit.**
   `.replit` starts both `streamlit run app.py --server.port 5000` and `python server.py`. The public web port points at Streamlit, while Databento belongs to the backend process on port 8000. If the backend process is not healthy, the UI can make it look like the Databento setup disappeared.

4. **Configuration is environment-driven.**
   Databento is selected only when `DATABENTO_API_KEY` is present in auto mode, or when `MARKET_DATA_PROVIDER=databento` is set. If those environment variables are missing in a new Replit/session/deployment, the fallback provider changes behavior without any source file changing.

## Immediate guardrail added

`.history/` is now ignored so new local-history snapshots do not keep accumulating in future commits. Existing tracked `.history/` files remain in Git until deliberately removed in a cleanup commit.

## Recommended next cleanup

1. Pick one canonical backend entry point and one canonical UI entry point.
2. Move old app variants (`app_backup.py`, `app_new.py`, and `clean_app/`) into an archive folder or remove them after confirming they are not used.
3. Remove tracked `.history/` snapshots with `git rm -r --cached .history` in a dedicated cleanup PR.
4. Move generated CSVs, attached screenshots, exported prompts, and model artifacts out of the application source tree or document which ones are intentional fixtures.
5. Add a startup health check that clearly reports the active market data provider and whether `DATABENTO_API_KEY` is loaded.
