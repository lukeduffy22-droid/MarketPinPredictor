"""Presentation-only evidence stages with explicit scope and unknown states."""


def capture_evidence_stages(payload, readiness):
    payload = payload if isinstance(payload, dict) else {}
    readiness = readiness if isinstance(readiness, dict) else {}
    observed = payload.get("observed") or {}
    tape = observed.get("tcbbo_evidence") or {}
    available = payload.get("transport_ok") is not False and payload.get("catalog_available") is True
    readiness_available = readiness.get("transport_ok") is not False and readiness.get("available") is not False

    def state(value):
        return "Verified" if value is True else "Not established" if value is False else "Unknown"

    return [
        {"Stage": "Captured", "Scope": "Requested date", "Status": state(observed.get("available") if available else None),
         "Meaning": "Observed provider records are present; this does not establish completeness."},
        {"Stage": "Finalized", "Scope": "Requested date", "Status": state(tape.get("finalized") if available else None),
         "Meaning": "The recording is closed; validation and outcome labels are separate."},
        {"Stage": "Validated", "Scope": "Requested date", "Status": state(payload.get("usable_for_research") if available else None),
         "Meaning": "The capture integrity contract passes for research."},
        {"Stage": "Labeled", "Scope": "Across independent sessions", "Status":
            f"{readiness['model_evidence_sessions']} sessions" if readiness_available and isinstance(readiness.get("model_evidence_sessions"), int) else "Unknown",
         "Meaning": "Sessions with model evidence and required labels; this is not today's close count."},
        {"Stage": "Model eligible", "Scope": "Across independent sessions", "Status": state(readiness.get("model_training_ready") if readiness_available else None),
         "Meaning": "Training evidence requirements pass. Promotion and calibrated accuracy require their own evidence."},
    ]


def evidence_cache_context(runtime_context, trading_date):
    """Bind retained summaries to date and runtime identity, never live authority."""
    context = runtime_context if isinstance(runtime_context, dict) else {}
    statuses = context.get("symbol_status") or {}
    generations = tuple(sorted((str(symbol), str(detail.get("active_generation")))
                               for symbol, detail in statuses.items() if isinstance(detail, dict)))
    return (str(trading_date), str(context.get("subscription_epoch_id")),
            str(context.get("subscription_generation")), generations)
