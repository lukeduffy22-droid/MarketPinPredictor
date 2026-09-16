# Data validity and extended-hours action plan

Prepared 2026-09-16. This is an implementation plan, not evidence of deployment.
Preserve raw records, research-only authority, exact opening buckets and existing
transport safety limits. No session extension or subscription change is made here.

## Confirmed findings

- `backend/databento_streamer.py` builds strike call/put GEX by summing valid
  calculated rows. An absent option side produces an empty sum of zero. The
  numeric field alone does not establish complete side coverage.
- `app.py` defaults missing gamma-wall days-to-expiry to zero and expiry count
  to one. Its legacy total-GEX fallback uses absolute net GEX, which is not
  generally gross call-plus-put exposure. Missing metadata should remain unknown.
- The user's iCloud CSV `2026-09-16T20-02_export.csv` has one valid primary
  expiration and three missing later-expiration profiles. At 20:10 UTC the
  backend reported primary-only staging, 1,072 deferred contracts and frozen
  promotion, with queue-full, coverage and incomplete exact-ORB reasons.
  Its diagnostics are not proof that the deferred subscriptions were activated.
- `live_subscription_window` closes OPRA connection admission at the cash close.
  The process remains available for audit/post-close work. This is application
  policy, not proof that all exchange trading or all Databento APIs stop then.

## Ordered implementation work

### P0: Make zero, missing and partial evidence distinguishable

Files: `backend/databento_streamer.py`, `app/services/live_data_client.py`,
the gamma-wall renderer in `app.py`, and export adapters.

Add per-strike/per-expiration/per-side expected, active, fresh, IV-eligible and
calculated contract counts, plus exclusion reasons and OI provenance. Distinguish
observed zero from no calculated rows; partial sums remain explicitly partial.
Do not equate raw quote arrival with valid quote or complete chain coverage.
Keep existing raw records immutable; add availability fields and render missing
values as N/A. Do not alter global arithmetic or thresholds without a separately
reviewed formula change. Derive gross exposure only from evidenced call/put sums;
do not substitute absolute net. Missing DTE and expiry count stay unknown.

Acceptance: fixtures for absent put, real zero OI, invalid IV, stale quote,
partial side, missing expiry, and opposing nonzero call/put exposure. UI and CSV
must agree. Reproduce the screenshot's three zero-put rows from retained inputs
before claiming which specific contracts were missing versus genuinely zero.

### P1: Explain the actual expiration subscription state

Files: subscription staging diagnostics in `backend/databento_streamer.py`,
`build_expiration_profile_view`, expiration tables and exports.

Show planned, deferred, requested, receiving, calculated and invalid separately.
Display stage reason, timestamp, epoch and generation. Replace 'subscribed'
denominators when only planned counts are known. Shared payload freshness must
not make an absent expiration appear freshly observed.

Acceptance: frozen-stage fixtures show the 100 planned future contracts as
deferred, no profile and a specific reason; requested-but-no-quotes differs from
invalid calculation. Active subscription and profile timestamps must agree.

### P1: Restore health responsiveness and verify deployment

The health worker isolation change has offline tests, but its loaded runtime
fingerprint and behavior under simultaneous ORB load must be checked after guarded
activation. Measure health/ORB latency separately, advancing persistence, process
identity and overload deltas. Do not call a healthy response proof of data quality.

### P2: Design independent future-expiration research admission

Review whether an incomplete morning ORB should permanently block later-expiry
analytical context. Define a separate research admission policy with bounded
contract count, measured feed capacity, fresh quotes, OI identity, expiration
semantics and overload rollback. Preserve the regular-session ORB failure and
do not let research profiles authorize same-day predictions. Keep the existing
queue-full freeze until a tested replacement policy is approved and measured.

Acceptance: a late-start session may produce labelled future-expiration research
only when its own evidence gates pass; the original ORB remains unavailable.
No extra Databento client, unrestricted family expansion or production blending.

### P3: Add session-aware after-hours collection

First verify dataset/schema coverage and account entitlements without opening
another live client. Establish a product/expiry/session matrix, including early
closes and daylight-saving transitions. The existing cash-close policy is not
an adequate universal options calendar. Expiring contracts need their own cutoff.

Start with the smallest supported post-cash-close window in a separate labelled
research recorder. Freeze regular-session predictions and official-close scoring
at their declared cutoff; append extended-session observations with session ID,
trade date, UTC timestamps, expiry and provenance. Never reuse an expired 0DTE
contract or present an ETF/futures observation as the official cash-index value.

Acceptance: provider timestamps actually advance within the claimed session,
correct universe rolls over, no increase in overload, immutable regular-session
results and no duplicate feed. Test cash close, options close, expiry cutoff,
holidays, overnight trade date and unsupported-session abstention before enabling.

## External coverage evidence checked September 16

- [Cboe options hours](https://www.cboe.com/about/hours/us-options): SPX/VIX and
  other eligible index options have product-specific regular, curb and GTH sessions.
- [Nasdaq options calendar](https://nasdaqtrader.com/Trader.aspx?id=optionshours):
  NDX/NDXP and listed ETF option classes generally trade through 16:15 ET;
  contract-specific expiration rules still apply.
- [Nasdaq equities sessions](https://listingcenter.nasdaq.com/assets/RuleBook/Nasdaq/rules/Nasdaq%20Equity%201.html):
  post-market equities session 16:00–20:00 ET.
- [Databento roadmap](https://roadmap.databento.com/b/n0o5prm6/feature-ideas/api-and-data):
  OPRA GTH support remains listed as Accepted, with regular-hours-only wording.
  Confirm current provider support before assuming overnight coverage.
- [Databento datasets](https://databento.com/docs/knowledge-base/datasets):
  OCEA.MEMOIR covers Blue Ocean overnight equities, a different dataset from OPRA;
  listing availability does not prove this account's entitlement.

Exchange trading hours do not by themselves establish provider coverage or a
licensed, observed feed for this application.
