# ADR-001: Offline Incident Replay Package

## Status

Accepted

## Decision Drivers

- Reproduce rejection reasons without Databento or mutable runtime state.
- Preserve supplied evidence bytes and validation methods.
- Keep credentials and runtime evidence outside Git.
- Fail closed when evidence is missing, modified, or ambiguous.

## Options Considered

1. Package raw databases and DBN captures. Rejected because packages would be large, expose unrelated runtime evidence, and couple replay to provider libraries.
2. Export derived summaries only. Rejected because summaries do not preserve supplied source evidence.
3. Package strict sanitized JSON records plus byte hashes. Selected because originals remain unchanged, replay is deterministic and provider-independent, and every omission is explicit.

## Decision

Each selected incident is supplied as a strict `marketpin-incident-evidence.v1` JSON record. The exporter rejects credential-like fields, copies the original bytes unchanged, and writes a canonical SHA-256 manifest outside the repository. Replay verifies each source hash and independently derives rejection reasons from failed checks and explicit missing-evidence reasons.

The package is diagnostic and research-only. It cannot grant production authority, alter historical records, or claim that omitted evidence was observed.

## Consequences

- Source systems must first produce a sanitized incident record conforming to the contract.
- Raw DBN and database files remain in their existing runtime locations.
- Identical source records produce identical manifest bytes and rejection reasons.