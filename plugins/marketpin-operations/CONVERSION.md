# MarketPin Operations conversion brief

## Product boundary

This is a skills-only plugin for MarketPinPredictor operators and reviewers. It turns repository-specific operational logic into three public jobs: live-session diagnosis, evidence auditing, and product/market review.

## Candidate dispositions

| Candidate | Disposition | Reason |
| --- | --- | --- |
| Live Databento/FastAPI/Streamlit/SQLite runbook | compile_skill | Repeatable, evidence-led operational job |
| Gamma, prediction, and closing-tape audit logic | compile_skill | Distinct provenance and replayability job |
| Product/market review workflow | compile_skill | Distinct decision-quality and communication job |
| Backend quality-gate workflow | reference_only | Useful repository evidence, but too narrow for a public skill |
| Launchers and recovery scripts | runtime_dependency | Remain in the user's repository; plugin only governs safe use |
| Trading agents, model weights, databases, raw tape, logs | internal_only | Proprietary/runtime data and unsafe or misleading public surface |
| API keys and `.env` material | discard | Secrets must never enter the package |
| Vendored PyTorch, torchvision, Git, caches, exports | discard | Unrelated implementation and generated content |

## Experience and capability profile

The plugin promise is to make MarketPin evidence auditable without disguising stale or invalid data. No MCP server or external app is required.

| Host capability | Disposition |
| --- | --- |
| read, list, search, grep | preferred read-only discovery |
| write, patch | mutation; only for an explicit build/fix request |
| shell | optional for tests and guarded repository commands; mutation depends on command |
| python | preferred for deterministic parsing, SQLite reads, and verification |

The canonical host-workspace operator is installed. The Python execution policy is packaged. Service restarts require explicit recovery authorization and verified process ownership. Trade execution is excluded.

## Brand rationale

The mark combines a market trace with a pinned observation point. The same geometry is used on light and dark surfaces; the compact icon removes no essential geometry and remains legible at small sizes.

## Submission boundary

Local validation and deterministic packaging do not imply submission or publication. Verified publisher identity and accurate support/privacy/terms URLs are intentionally left unresolved.
