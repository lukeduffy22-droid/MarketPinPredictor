# Backend post-close operational reviews

The FastAPI lifespan starts `run_diagnostic_review_loop`. It checks once a minute,
outside cash-session hours, and selects the latest completed session using the
reviewed exchange calendar. Normal eligibility is 15 minutes after cash close
(15:15 Chicago); early-close days use their actual close. A later startup can
catch up the latest completed session. It does not bulk-submit historical logs.
The backend must remain running; a sleeping/offline computer cannot execute it.

Set `MARKETPIN_POSTCLOSE_REVIEW_ENABLED=0` to disable the automatic worker at its
next startup. It defaults to enabled. Backend environment configuration supplies
`OPENAI_API_KEY` and the existing `OPENAI_DIAGNOSTIC_MODEL` setting. API costs are
separate from Codex. No prediction, code, threshold, incident status or order can
be changed by the review.

## Evidence, concurrency and cost bounds

- Immutable evidence JSON: `data/diagnostic_automation/<session>/<sha256>.evidence.json`.
- Persistent job state: `data/diagnostic_automation/jobs.sqlite` (separate from market databases).
- Successful immutable AI reviews: `data/diagnostic_reviews/<session>/<sha256>.json`.
- One frozen evidence packet and at most one successful automatic review per session.
  Newer evidence or code changes do not automatically incur another paid review.
- At most two API attempts per session. Only explicit rate-limit/server responses
  receive one retry, after 15 minutes. Authentication errors, invalid responses,
  and uncertain network outcomes stop automatic retries for that session.
- An interrupted `IN_FLIGHT` attempt remains held for inspection across restarts;
  the provider may have completed it. Do not delete its job to force a duplicate.
- Input is capped at 64,000 UTF-8 bytes; oversized evidence is retained but not sent.
  Existing AI output cap is 2,000 tokens; timeout is 30 seconds with SDK retries off.
- Transactional claims prevent concurrent backend workers from making duplicate
  paid requests. API calls run outside the event loop and outside the SQLite lock.

History supplies the previous archived session's capture summaries for comparison,
when available. Missing evidence stays explicit. The runtime poll timestamp can be
later than the selected capture day; it cannot establish the earlier day's health.
Source hashes represent disk files, not proof of deployed model or streamer code.

## Dashboard and manual operation

Open Advisor → AI operational diagnosis → **Available retained sessions**. Select
a date, then open **Saved reviews for selected session** for its job status and
saved AI review. This is available without first requesting a fresh runtime check.
Use **Analyze Live State** when a new runtime observation is needed.

To run the same guarded due-job once:

```powershell
.\.venv\Scripts\python.exe tools/run_postclose_review.py
```

Append `--evidence-only` to retain evidence without an AI call. The CLI loads the
project `.env` without overriding existing environment variables. Authentication
failures require correction of the credential in the process environment; never
paste credentials into reviews or logs. A failed day's evidence remains available
for a manual dashboard review after correcting credentials. Future sessions get
their own independent job. Disabling the background worker does not disable an
explicit CLI or dashboard request.

## September 17 implementation verification

Source integration and offline tests passed. The actual one-shot job saved a
26,012-byte September 17 evidence packet, then received `AuthenticationError` from
OpenAI using the CLI environment. The job is FAILED with one attempted API call;
no successful AI review or automatic correction is claimed. The running backend
has not been restarted; lifespan activation requires the next backend launch.

The retained-file inventory found 34 dates from January 5 through September 17,
with gaps; this does not establish continuous historical retention. The review
archive had zero saved AI reviews before this job. Older manually downloaded
bundles or browser session-state results may exist elsewhere and were not searched.
