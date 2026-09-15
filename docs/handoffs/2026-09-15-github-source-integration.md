# GitHub source integration handoff

## Purpose

This branch gives GitHub and Copilot a reviewable source line based on the protected `friday-1/9` branch. It does not merge or publish the unrelated local history.

## Provenance

- GitHub base: `c558a5caaaf85687567e276e73116772c6de53a2`
- Local committed source reference: `fd0b73f99f38a0db49e559cadda5b862f6307ec1`
- Final readiness-test delta reference: `d2ab90585` (content copied, not cherry-picked)

The local and GitHub histories have no merge base. The local tree also contains large databases, captures, bundles, generated outputs, and nested repositories. Those objects are intentionally absent from this branch.

## Included

- current Streamlit, FastAPI, Databento, closing-tape, forecast-authority, and GEX source;
- offline tests and operational tools;
- reviewed configuration templates and documentation;
- the temporary-database isolation fixture for closing-tape readiness tests;
- current Copilot guidance and a publication-boundary CI check.

## Excluded

- credentials and `.env` files;
- databases, OPRA/DBN captures, provider caches, exports, logs, and generated reports;
- model weights, pickles, compiled extensions, virtual environments, and caches;
- Git bundles, ZIP backups, embedded `pytorch`/`vision` repositories, and other third-party source trees;
- legacy scripts containing literal provider credentials;
- campaign and research-output artifacts, which require separate review if they are ever published.

## Preserved legacy code

Forty-two superseded `friday-1/9` app, test, and tool files are retained
byte-for-byte under `legacy/friday-1-9/`. They are outside active import and
pytest discovery so the current architecture has one clear implementation for
GitHub and Copilot, while the old code remains available for comparison or
recovery.

## Review contract

Keep observed OPRA evidence separate from inferred side/positioning estimates. Preserve fail-closed abstentions and the existing maturity, evaluation, calibration, and promotion gates. CUDA or high record volume is not evidence that a forecast is accurate or production-authorized.

Before merge, require the GitHub checks, inspect the source-only manifest, and review the branch as a draft PR into `friday-1/9`. Copilot tasks should be bounded to named files and acceptance tests on this branch.
