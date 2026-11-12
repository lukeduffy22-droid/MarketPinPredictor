# Fresh diagnostic AI review verification — September 21, 2026

Implemented in source; existing service activation remains unverified. Research only.

## Behavior

- **Refresh live evidence and ask AI** collects two independent bounded observations,
  five seconds apart, before requesting an OpenAI review. It never substitutes the
  Advisor's retained runtime when a new endpoint read fails.
- The allowlisted packet includes timestamped health/dashboard endpoint outcomes,
  per-symbol validation, two capture summaries and count deltas, local read-only
  persistence row timestamps, and bounded error categories. Epoch/generation changes
  suppress runtime message deltas. The local database's identity is not represented
  as verified backend database identity; row times are not commit times.
- Opening proof is explicitly unverified without an exact opening-bucket receipt.
  The closing window is not-started, in-progress, or elapsed-but-unverified based on
  the selected session calendar. Historical excerpts follow current evidence.
- Responses must be completed, non-refused, nonempty, valid JSON and schema-valid.
  Citation choices are restricted to actual packet keys in the request schema and
  checked again locally. These checks do not prove semantic accuracy of AI prose.
- Reviews lead with working, failing, changed, and unverified findings. Each finding
  cites evidence. Suggestions remain proposed-only; no repair or model authority is added.
- Successful reviews automatically retain the exact input packet, question, response
  text, parsed review, actual model, response/request IDs and usage. Reviews are
  content-addressed under `data/diagnostic_reviews/<date>/`.
- Separate append-only receipts under `data/diagnostic_receipts/<date>/` record STARTED
  and terminal outcomes. A SAVED receipt identifies the review path. The embedded
  review receipt is VALIDATED, before persistence, to avoid a self-referential hash.
  Interrupted STARTED attempts are not automatically retried. Provider exception
  bodies and credentials are not written to receipts.
- Post-close automation uses the same collector and validated review path after its
  next activation. Existing failed jobs are preserved; this change does not reset them.

## Observed real request

The first verification request completed but failed local citation validation.
The request schema was tightened to enumerate permitted evidence IDs. A second
real request completed successfully using the existing configured key/model:

- Actual model: `gpt-5.6-luna`.
- Runtime observation: `2026-09-21T20:45:15.143693+00:00`.
- Packet creation: `2026-09-21T20:45:15.313167+00:00`.
- Review completion: `2026-09-21T20:45:32.895664+00:00` (15:45:32 Chicago).
- Ten endpoint reads succeeded across two observations. This establishes read
  availability, not market-hours feed health. The observations were post-close.
- Response ID: `resp_007ddb1de67493b3016ab1975e28a887d18be649bb43ea62ef`.
- Packet SHA-256: `63d2b408e43bfcc6c39c3c87ca01b2c62f0e110884c0353bba90a82945b347c9`.
- Review: `data/diagnostic_reviews/2026-09-21/a30b1efa0f1c268a3d03b79c83ec86f074e34e5537c3a58551e529ee7cd6f312.json`.
- SAVED receipt: `data/diagnostic_receipts/2026-09-21/58ea9d66b9b658ec00a886e43ccef4afb1fdd7a7c9c95c06d9c9f67b95a2c634.json`.
- Immutable review was loaded back and matched the returned result.

The successful response is retained verbatim, including its limitations: its
"changed" section included standalone session totals. Subsequent prompt clarification
restricts that section to explicit comparisons and strengthens post-close wording.
That final wording adjustment is covered by source review, not another paid request.

## Checks and activation boundary

52 focused tests passed across diagnostic live-review, capture-review, history,
automation, Advisor workflow, and live Advisor suites. Tests cover incomplete output,
refusals, empty/malformed output, schema and citation failure, safe failure receipts,
automatic persistence, save failure, generation changes, missing live collection,
read-only database evidence, closing-window phases, and Streamlit review retention.

Tests used an absolute canonical DATABASE_URL in their own process because a separate
concurrent database-configuration change rejects the local env file's relative URL.
No environment file was modified by this work.

The browser at port 8501 still displayed the older Advisor controls. The repository
disables Streamlit hot reload. Windows exposed listener IDs (8501: 60352; 8000: 42724)
but not executable paths or command lines to this session, so process ownership could
not be verified. No service was stopped or restarted. Do not reuse these recorded PIDs
without a fresh ownership check.

Activation requires a verified dashboard restart by a session able to inspect its
owner, followed by visual confirmation of the new button and saved history. The
backend automation changes load on a separately verified backend restart; do not
restart the backend merely to display the dashboard change. Thin-chain and missing-pair
causes remain separate investigations. No retained market artifacts were rewritten.

## Explicit manual verification command

`tools/verify_diagnostic_ai.py --ask-ai --session-date YYYY-MM-DD` makes one paid
request using the configured credentials, collects fresh evidence, saves and
reads back a successful review, and prints safe metadata. It neither restarts a
service nor resets an automatic job. Run using the repository virtual environment
and the same absolute DATABASE_URL convention as the guarded launcher.

Response handling follows the official
[Structured Outputs guide](https://developers.openai.com/api/docs/guides/structured-outputs).
