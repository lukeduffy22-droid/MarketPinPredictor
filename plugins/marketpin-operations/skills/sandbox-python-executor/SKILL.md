---
name: sandbox-python-executor
description: Use when a MarketPin plugin workflow needs deterministic local parsing, SQLite inspection, hashing, archive checks, or repository verification and the host provides Python.
---

# Sandbox Python Executor

Use host-native Python only when execution materially improves correctness. Execute the check before claiming a result; do not substitute a code block for evidence.

Prefer reviewed repository or plugin scripts after inspecting their inputs and side effects. Keep repository access read-only unless mutation is explicitly authorized. Do not expose secrets, credentials, tokens, or unrelated files. Do not assume sandbox internet access.

Report the runtime used, operation, pass/fail status, important findings, and generated artifact path or hash when relevant. If Python is unavailable, state that and keep execution-dependent conclusions unverified. This Skill does not grant Python access or create an MCP dependency.
