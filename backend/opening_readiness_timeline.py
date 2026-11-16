"""Bounded opening diagnostics. These observations never grant market authority."""
from __future__ import annotations

from collections import Counter, deque
from datetime import date, datetime, timezone
import base64
import json
import math
from pathlib import Path
import re
import threading
import time

SCHEMA = "marketpin-opening-readiness-timeline.v1"
UNKNOWN_UPSTREAM = "UNKNOWN: socket arrival, decode start and earlier SDK handling are not observed"
TIMING_ADMISSION_REASONS = frozenset({
    "ADMITTED", "MISSING_TS_RECV", "CBBO_RTYPE_MISMATCH", "CBBO_INTERVAL_UNALIGNED",
    "STALE_RECEIVE_LAG", "MISSING_INSTRUMENT_ID", "GENERATION_MISMATCH",
    "REPLAY_PENDING", "DUPLICATE_RECORD",
})


def _integer(value):
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if 0 <= result < 10**19 else None


def _utc_ns(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("A timezone-aware cash open is required")
        return int(value.timestamp() * 1_000_000_000)
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        raise ValueError("A timezone-aware cash open is required")
    return int(parsed.timestamp() * 1_000_000_000)


def _safe(value, depth=0):
    if isinstance(value, float) and not math.isfinite(value):
        return "UNKNOWN_NONFINITE_VALUE"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value[:400]
    if depth > 2:
        return "not retained: detail nesting limit"
    if isinstance(value, dict):
        return {
            str(k)[:80]: _safe(v, depth + 1)
            for k, v in list(value.items())[:24]
            if not any(s in str(k).lower() for s in ("api_key", "password", "secret", "token", "authorization"))
        }
    if isinstance(value, (list, tuple)):
        return [_safe(v, depth + 1) for v in value[:16]]
    return str(value)[:400]


class BoundedOpeningTimelineWriter:
    """One off-reader append worker, bounded queues and explicit unknown outcomes."""

    def __init__(self, directory, *, max_pending=1024, critical_capacity=128,
                 batch_size=64, write_batch=None):
        self.directory = Path(directory)
        self.max_pending = max(1, int(max_pending))
        self.critical_capacity = max(1, int(critical_capacity))
        self.batch_size = max(1, int(batch_size))
        self._normal = deque()
        self._critical = deque()
        self._condition = threading.Condition()
        self._thread = None
        self._closing = False
        self._offered = self._completed = self._written = 0
        self._last_successful_batch_order = 0
        self._dropped = self._critical_dropped = 0
        self._overflow_pending = 0
        self._write_failures = self._unknown_events = 0
        self._last_error = None
        self._last_date = None
        self._write_batch = write_batch or self._append_batch

    def start(self):
        with self._condition:
            if self._thread is None and not self._closing:
                self._thread = threading.Thread(
                    target=self._run, name="marketpin-opening-timeline", daemon=True,
                )
                self._thread.start()
        return self

    def offer(self, event, *, critical=False):
        with self._condition:
            self._last_date = event.get("trading_date", self._last_date)
            target = self._critical if critical else self._normal
            limit = self.critical_capacity if critical else self.max_pending
            if self._closing or len(target) >= limit:
                self._dropped += 1
                self._critical_dropped += int(critical)
                self._overflow_pending += 1
                self._condition.notify()
                return False
            self._offered += 1
            event["writer_enqueue_order"] = self._offered
            target.append((self._offered, dict(event)))
            self._condition.notify()
            return True

    def _append_batch(self, rows):
        # Only this worker calls filesystem APIs. Filenames cannot escape root.
        grouped = {}
        for row in rows:
            day = str(row.get("trading_date", ""))
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
                raise ValueError("TIMELINE_TRADING_DATE_INVALID")
            date.fromisoformat(day)
            grouped.setdefault(day, []).append(row)
        self.directory.mkdir(parents=True, exist_ok=True)
        for day, events in grouped.items():
            with (self.directory / (day + ".ndjson")).open("a", encoding="utf-8", newline="\n") as stream:
                for event in events:
                    stream.write(json.dumps(event, separators=(",", ":"), allow_nan=False) + "\n")
                stream.flush()
                # fsync acknowledges this append, not unknown prior upstream data.
                import os
                os.fsync(stream.fileno())

    def _run(self):
        while True:
            with self._condition:
                while not self._normal and not self._critical and not self._overflow_pending:
                    if self._closing:
                        return
                    self._condition.wait()
                pending = []
                for _ in range(self.batch_size):
                    if not self._normal and not self._critical:
                        break
                    if not self._normal:
                        pending.append(self._critical.popleft())
                    elif not self._critical:
                        pending.append(self._normal.popleft())
                    elif self._normal[0][0] < self._critical[0][0]:
                        pending.append(self._normal.popleft())
                    else:
                        pending.append(self._critical.popleft())
                overflow, self._overflow_pending = self._overflow_pending, 0
                day = self._last_date
            rows = [event for _, event in pending]
            if overflow and day:
                rows.append({
                    "schema_version": SCHEMA, "authority": "DIAGNOSTIC_ONLY",
                    "trading_date": day, "kind": "writer_overflow",
                    "dropped_since_previous_notice": overflow,
                    "outcome": "MISSING_EVENTS_UNKNOWN", "prediction_eligible": False,
                })
            try:
                self._write_batch(rows)
            except Exception as exc:
                # A partially appended batch is unknown, never retried blindly.
                with self._condition:
                    self._write_failures += 1
                    self._unknown_events += len(rows)
                    self._last_error = type(exc).__name__
                    if pending:
                        self._completed = pending[-1][0]
                    self._condition.notify_all()
            else:
                with self._condition:
                    if pending:
                        self._completed = self._last_successful_batch_order = pending[-1][0]
                        if self._written == pending[0][0] - 1:
                            self._written = pending[-1][0]
                    self._condition.notify_all()

    def status(self):
        with self._condition:
            return {
                "scope": "shared_writer_lifetime_all_retained_generations",
                "offered_events": self._offered,
                "completed_write_attempt_order": self._completed,
                "acknowledged_write_order": self._written,
                "last_successful_batch_order": self._last_successful_batch_order,
                "acknowledgment_order_semantics": "CONTIGUOUS_FSYNC_ACKNOWLEDGMENTS_WITH_NO_UNKNOWN_GAP",
                "pending_events": len(self._normal) + len(self._critical),
                "dropped_events": self._dropped,
                "dropped_critical_events": self._critical_dropped,
                "write_failures": self._write_failures,
                "write_outcome_unknown_events": self._unknown_events,
                "last_write_error_type": self._last_error,
                "writer_started": self._thread is not None,
                "writer_alive": self._thread is not None and self._thread.is_alive(),
                "durability_contract": "OFF_READER_BATCH_APPEND_FLUSH_FSYNC",
                "fsync_acknowledged_order": self._written,
                "all_offered_events_acknowledged": (
                    self._written == self._offered and not self._dropped and not self._write_failures
                ),
            }

    def flush(self, timeout=2.0):
        # Call only from off-reader checks/shutdown/tests, never SDK callback.
        deadline = time.monotonic() + max(0.0, float(timeout))
        with self._condition:
            target = self._offered
            self._condition.notify()
            while self._completed < target and time.monotonic() < deadline:
                self._condition.wait(max(0.0, deadline - time.monotonic()))
        result = self.status()
        result["flush_target_order"] = target
        result["flush_complete"] = result["completed_write_attempt_order"] >= target
        return result

    def close(self, timeout=2.0):
        with self._condition:
            self._closing = True
            self._condition.notify_all()
            worker = self._thread
        if worker is not None:
            worker.join(max(0.0, float(timeout)))
        return self.status()


class OpeningReadinessTimeline:
    """Generation-owned, first-stage observations; never an admission shortcut."""

    def __init__(self, writer, *, epoch_id, generation, cash_open_utc,
                 quote_schema="cbbo-1s", replay_pending=False,
                 clock_ns=time.time_ns, monotonic_ns=time.monotonic_ns,
                 max_instruments=4096, max_events=2048,
                 preopen_seconds=2700, postopen_seconds=120):
        self.writer = writer
        self.epoch_id = str(epoch_id)
        self.generation = int(generation)
        self.open_ns = _utc_ns(cash_open_utc)
        self.trading_date = datetime.fromtimestamp(self.open_ns / 1e9, timezone.utc).date().isoformat()
        self.quote_schema = str(quote_schema)
        self._clock_ns, self._monotonic_ns = clock_ns, monotonic_ns
        self._replay_pending = bool(replay_pending)
        self.max_instruments, self.max_events = max(1, int(max_instruments)), max(1, int(max_events))
        self.preopen_ns = int(preopen_seconds * 1e9)
        self.postopen_ns = max(5, int(postopen_seconds)) * 10**9
        self._lock = threading.RLock()
        self._mappings = {}
        self._receipts = {}
        self._receipt_counts = Counter()
        self._seen = set()
        self._critical_seen = set()
        self._mapping_links = {}
        self._sequence = 0
        self._counts = Counter()
        self._retired = False
        self._observer_overflow = 0
        self._callback_errors = 0
        self._critical_latest = {}
        # Fixed phase/family/reason dimensions; these count observations rather
        # than coalesced event emissions or distinct provider records.
        self._timing_admission_counts = Counter()

    def _phase(self, stamp):
        if stamp < self.open_ns - self.preopen_ns or stamp >= self.open_ns + self.postopen_ns:
            return None
        if stamp < self.open_ns:
            return "preopen"
        return "opening_bucket" if stamp < self.open_ns + 5 * 10**9 else "after_opening_bucket"

    @staticmethod
    def _family(symbol):
        root = str(symbol or "").strip().split(" ", 1)[0]
        if root in {"SPX", "SPXW"}:
            return "SPX"
        if root in {"NDX", "NDXP"}:
            return "NDX"
        if root in {"VIX", "VIXW", "RUT", "RUTW"}:
            return "VIX" if root.startswith("VIX") else "RUT"
        return "OTHER" if root else "UNKNOWN"

    @staticmethod
    def _record(record):
        instrument = _integer(getattr(record, "instrument_id", None))
        recv = _integer(getattr(record, "ts_recv", None))
        index = _integer(getattr(record, "ts_index", None))
        sequence = _integer(getattr(record, "sequence", None))
        return {
            "instrument_id": instrument,
            "provider_interval_end_ns": recv,
            "provider_last_sale_ns": _integer(getattr(record, "ts_event", None)),
            "provider_index_ns": index,
            "provider_sequence": sequence,
            "record_key": [instrument, recv, sequence],
        }

    def _stamp(self, wall=None, mono=None):
        return int(self._clock_ns() if wall is None else wall), int(self._monotonic_ns() if mono is None else mono)

    def _emit(self, kind, family, wall, mono, *, record=None, details=None, critical=True, force=False):
        phase = self._phase(wall)
        if phase is None and not force:
            return None
        phase = phase or "outside_capture_window"
        family = family or "UNKNOWN"
        reason = str((details or {}).get("reason", ""))[:160]
        stage = str((details or {}).get("stage", ""))[:80]
        state = tuple(str((details or {}).get(name, ""))[:80] for name in ("status", "state", "handoff_status", "recorded", "progress_eligible", "write_outcome"))
        key = (kind, family, phase, reason, stage, state)
        if key in self._counts or len(self._counts) < self.max_events + 256:
            self._counts[key] += 1
        seen = self._critical_seen if critical else self._seen
        limit = 256 if critical else self.max_events
        if key in seen:
            return None
        if len(seen) >= limit and kind != "generation_retired":
            self._observer_overflow += 1
            return None
        seen.add(key)
        self._sequence += 1
        event = {
            "schema_version": SCHEMA, "authority": "DIAGNOSTIC_ONLY",
            "trading_date": self.trading_date,
            "subscription_epoch_id": self.epoch_id,
            "subscription_generation": self.generation,
            "event_id": f"{self.epoch_id}:{self.generation}:{self._sequence}",
            "sequence": self._sequence, "kind": kind, "phase": phase,
            "family_at_observation": family,
            "local_observed_at_ns": wall, "local_observed_monotonic_ns": mono,
            "cash_open_ns": self.open_ns,
            "first_bucket_end_ns": self.open_ns + 5 * 10**9,
            "quote_schema": self.quote_schema,
            "replay_pending": self._replay_pending,
            "prediction_eligible": False, "training_eligible": False,
            "upstream_timing": UNKNOWN_UPSTREAM,
        }
        if record is not None:
            event.update(record() if callable(record) else record)
            provider = event.get("provider_interval_end_ns")
            event["provider_interval_phase"] = self._phase(provider) if provider is not None else "UNKNOWN"
        if details:
            event["details"] = _safe(details)
        event["writer_enqueue_accepted"] = True
        event["persistence_state_at_emit"] = "ENQUEUED_NOT_YET_WRITTEN_OR_FSYNCED"
        accepted = self.writer.offer(event, critical=critical)
        if not accepted:
            event["writer_enqueue_accepted"] = False
        if critical:
            # Fixed family/phase/kind keys retain recent control evidence even
            # if the writer's reserved critical capacity is exhausted.
            latest_key = (family, phase, kind)
            if len(self._critical_latest) < 128 or latest_key in self._critical_latest or kind == "generation_retired":
                self._critical_latest[latest_key] = event
        return event

    def sdk_callback(self, record):
        """Public SDK decoded-record callback; before iteration, not socket I/O."""
        wall, mono = self._stamp()
        try:
            with self._lock:
                if self._retired or self._phase(wall) is None:
                    return
                if hasattr(record, "stype_out_symbol"):
                    self.mapped(
                        getattr(record, "instrument_id", None),
                        getattr(record, "stype_out_symbol", None),
                        observed_at_ns=wall, observed_monotonic_ns=mono,
                        source="sdk_callback",
                        mapping_start_ns=_integer(getattr(record, "start_ts", None)),
                        mapping_end_ns=_integer(getattr(record, "end_ts", None)),
                    )
                    return
                if type(record).__name__ == "SystemMsg" and int(getattr(record, "code", -1)) == 3:
                    self._replay_pending = False
                    self._emit("sdk_replay_completed", "SHARED", wall, mono)
                    return
                if not (hasattr(record, "bid_px_00") and hasattr(record, "ask_px_00")):
                    return
                info = self._record(record)
                iid = info["instrument_id"]
                if iid is None:
                    self._observer_overflow += 1
                    return
                phase = self._phase(wall)
                receipt_key = (phase, iid, self._replay_pending)
                if receipt_key in self._receipts:
                    return
                receipt_budget_full = self._receipt_counts[phase] >= self.max_instruments
                family = self._mappings.get(iid, ("UNKNOWN", None))[0]
                kind = "first_live_sdk_quote" if not self._replay_pending else "first_replay_sdk_quote"
                if receipt_budget_full:
                    self._observer_overflow += 1
                    if family not in {"SPX", "NDX"} or (family, phase, kind) in self._critical_latest:
                        return
                info.update({
                    "sdk_callback_received_at_ns": wall,
                    "sdk_callback_received_monotonic_ns": mono,
                    "callback_semantics": "application SDK callback entry after decoding and earlier SDK handlers",
                    "bid_px_raw": _integer(getattr(record, "bid_px_00", None)),
                    "ask_px_raw": _integer(getattr(record, "ask_px_00", None)),
                    "quote_price_units": "provider fixed-point 1e-9",
                })
                try:
                    info["raw_record_base64"] = base64.b64encode(bytes(record)).decode("ascii")
                except Exception:
                    info["raw_record_base64"] = None
                family = self._mappings.get(iid, ("UNKNOWN", None))[0]
                info["family_at_sdk_receipt"] = family
                known_mapping = self._mappings.get(iid)
                if known_mapping is not None:
                    start, end = known_mapping[2], known_mapping[3]
                    provider = info["provider_interval_end_ns"]
                    info.update(mapping_start_ns=start, mapping_end_ns=end,
                                mapping_version=known_mapping[4])
                    info["mapping_quote_validity_status"] = (
                        "UNKNOWN" if start is None or end is None or provider is None else
                        "VALID" if int(start) <= provider < int(end) else "INVALID"
                    )
                kind = "first_live_sdk_quote" if not self._replay_pending else "first_replay_sdk_quote"
                # Unknown IDs must remain linkable individually; their observed
                # attribution is immutable even after a later mapping.
                if family == "UNKNOWN":
                    info["unmapped_instrument_id_at_receipt"] = iid
                    event = self._emit(
                        kind, family, wall, mono, record=info,
                        details={"stage": f"unmapped instrument {iid}"}, critical=False,
                    )
                elif family in {"SPX", "NDX"}:
                    event = self._emit(kind, family, wall, mono, record=info)
                else:
                    event = None
                info["receipt_event_id"] = event["event_id"] if event else None
                if not receipt_budget_full:
                    self._receipts[receipt_key] = info
                    self._receipt_counts[phase] += 1
        except Exception:
            self._callback_errors += 1

    def mapped(self, instrument_id, symbol, *, family=None, observed_at_ns=None,
               observed_monotonic_ns=None, source="reader", mapping_start_ns=None,
               mapping_end_ns=None, mapping_version=None):
        wall, mono = self._stamp(observed_at_ns, observed_monotonic_ns)
        iid = _integer(instrument_id)
        if iid is None:
            return
        family = family or self._family(symbol)
        with self._lock:
            if self._retired or self._phase(wall) is None:
                return
            if iid not in self._mappings and len(self._mappings) >= self.max_instruments:
                self._observer_overflow += 1
                return
            mapping = (family, str(symbol)[:40], mapping_start_ns, mapping_end_ns, mapping_version)
            self._mappings[iid] = mapping
            # Later mappings may be observed, but cannot rewrite the validity
            # evidence used to link an original receipt.
            if family in {"SPX", "NDX"}:
                linked = []
                for phase in ("preopen", "opening_bucket", "after_opening_bucket"):
                    for replay in (False, True):
                        receipt = self._receipts.get((phase, iid, replay))
                        if not receipt or receipt.get("family_at_sdk_receipt") != "UNKNOWN":
                            continue
                        link_key = (phase, iid, replay)
                        provider = receipt.get("provider_interval_end_ns")
                        start, end = mapping_start_ns, mapping_end_ns
                        if provider is None or start is None or end is None:
                            validity = "CANDIDATE_MAPPING_VALIDITY_UNKNOWN"
                        elif int(start) <= provider < int(end):
                            validity = "PROVIDER_INTERVAL_WITHIN_MAPPING_VALIDITY"
                        else:
                            validity = "REJECTED_PROVIDER_INTERVAL_OUTSIDE_MAPPING_VALIDITY"
                        previous_link = self._mapping_links.get(link_key)
                        if previous_link is not None and (
                            previous_link == "PROVIDER_INTERVAL_WITHIN_MAPPING_VALIDITY"
                            or validity != "PROVIDER_INTERVAL_WITHIN_MAPPING_VALIDITY"
                        ):
                            continue
                        linked.append({
                            "original_receipt_key": list(link_key),
                            "receipt_event_id": receipt.get("receipt_event_id"),
                            "receipt_status": "OBSERVED_NO_SEPARATE_EVENT" if not receipt.get("receipt_event_id") else "OBSERVED_EVENT_RETAINED",
                            "original_family_at_receipt": "UNKNOWN",
                            "sdk_callback_received_at_ns": receipt["sdk_callback_received_at_ns"],
                            "provider_interval_end_ns": provider,
                            "mapping_start_ns": start, "mapping_end_ns": end,
                            "mapping_version": mapping_version, "link_status": validity,
                        })
                kind = "sdk_mapping_received" if source == "sdk_callback" else "reader_mapping_processed"
                emitted = self._emit(kind, family, wall, mono, record={"instrument_id": iid},
                           details={"source": source, "symbol": str(symbol)[:40],
                                    "mapping_start_ns": mapping_start_ns, "mapping_end_ns": mapping_end_ns,
                                    "mapping_version": mapping_version, "linked_receipts": linked})
                for link in linked:
                    evidence = emitted
                    if evidence is None:
                        # Family summaries coalesce; later unknown IDs still
                        # need their own bounded, noncritical linkage evidence.
                        evidence = self._emit(
                            "mapping_receipt_link", family, wall, mono,
                            record={"instrument_id": iid},
                            details={"source": source,
                                     "stage": f"{link['original_receipt_key']}:{link['link_status']}",
                                     "linked_receipts": [link]}, critical=False,
                        )
                    if evidence is not None:
                        self._mapping_links[tuple(link["original_receipt_key"])] = link["link_status"]

    def _reader_stage(self, kind, record, family, wall=None, mono=None, details=None):
        wall, mono = self._stamp(wall, mono)
        info = self._record(record)
        iid = info["instrument_id"]
        with self._lock:
            if self._retired:
                return
            family = family or self._mappings.get(iid, ("UNKNOWN", None))[0]
            # A processed quote may arrive across a phase boundary. Search a
            # fixed six keys; never assume an earlier same-ID quote is this one.
            matching = None
            for phase in ("preopen", "opening_bucket", "after_opening_bucket"):
                for replay in (False, True):
                    receipt = self._receipts.get((phase, iid, replay))
                    if receipt and receipt["record_key"] == info["record_key"]:
                        matching = receipt
                        break
                if matching:
                    break
            if matching is None and family in {"SPX", "NDX"}:
                for phase in ("preopen", "opening_bucket", "after_opening_bucket"):
                    for receipt_kind in ("first_live_sdk_quote", "first_replay_sdk_quote"):
                        reserved = self._critical_latest.get((family, phase, receipt_kind))
                        if reserved and reserved.get("record_key") == info["record_key"]:
                            matching = dict(reserved)
                            matching["receipt_event_id"] = reserved["event_id"]
                            break
                    if matching:
                        break
            info["sdk_callback_receipt_for_record"] = "NOT_RETAINED"
            if matching:
                info.update({
                    "sdk_callback_receipt_status": "OBSERVED_EVENT_RETAINED" if matching["receipt_event_id"] else "OBSERVED_NO_SEPARATE_EVENT",
                    "sdk_callback_receipt_for_record": matching["receipt_event_id"],
                    "sdk_callback_received_at_ns": matching["sdk_callback_received_at_ns"],
                    "sdk_callback_received_monotonic_ns": matching["sdk_callback_received_monotonic_ns"],
                })
                delay = mono - matching["sdk_callback_received_monotonic_ns"]
                elapsed_field = "callback_to_reader_elapsed_ns" if kind == "first_reader_processed_quote" else "callback_to_stage_elapsed_ns"
                info[elapsed_field] = delay if delay >= 0 else None
                info["elapsed_time_status"] = "MONOTONIC" if delay >= 0 else "UNKNOWN_CLOCK_DISCONTINUITY"
            timestamp_fields = {
                "first_reader_processed_quote": "reader_processed_at_ns",
                "first_cache_admitted_quote": "cache_admitted_at_ns",
                "first_quote_rejection": "quote_rejected_at_ns",
            }
            info[timestamp_fields[kind]] = wall

            def first_record_evidence():
                values = dict(info)
                # Invalid/missing prices are precisely the records for which a
                # rejection observation is needed. Never lose it to int(None).
                values["bid_px_raw"] = _integer(getattr(record, "bid_px_00", None))
                values["ask_px_raw"] = _integer(getattr(record, "ask_px_00", None))
                values["quote_price_units"] = "provider fixed-point 1e-9"
                try:
                    values["raw_record_base64"] = base64.b64encode(bytes(record)).decode("ascii")
                except Exception:
                    values["raw_record_base64"] = None
                return values
            self._emit(kind, family, wall, mono, record=first_record_evidence, details=details)

    def processed(self, record, family=None, *, observed_at_ns=None, observed_monotonic_ns=None):
        self._reader_stage("first_reader_processed_quote", record, family, observed_at_ns, observed_monotonic_ns)

    def cache_admitted(self, record, family, *, observed_at_ns=None, observed_monotonic_ns=None):
        self._reader_stage(
            "first_cache_admitted_quote", record, family, observed_at_ns, observed_monotonic_ns,
            {"semantics": "cache admission only; not pair, clock, handoff or ORB acceptance"},
        )

    def rejected(self, record, family, reason, *, observed_at_ns=None, observed_monotonic_ns=None):
        self._reader_stage(
            "first_quote_rejection", record, family, observed_at_ns, observed_monotonic_ns,
            {"reason": str(reason)[:160]},
        )

    def qualified(self, family, *, stage="primary_pair_set", observed_at_ns=None,
                  observed_monotonic_ns=None, details=None):
        fields = dict(details or {})
        fields["stage"] = stage
        return self.milestone(
            "qualified_quote_inputs", family=family, observed_at_ns=observed_at_ns,
            observed_monotonic_ns=observed_monotonic_ns, details=fields,
        )

    def timing_admission(self, record, *, reason, record_generation, sample_count,
                         minimum_samples, reader_processed_at_ns=None,
                         observed_at_ns=None, observed_monotonic_ns=None):
        """Observe timing-window inclusion independently of price/cache admission.

        An admitted negative-lag sample can still make the clock unsafe. These
        counters never establish synchronization or opening capture eligibility.
        """
        if reason not in TIMING_ADMISSION_REASONS:
            raise ValueError("UNKNOWN_TIMING_ADMISSION_REASON")
        wall, mono = self._stamp(observed_at_ns, observed_monotonic_ns)
        with self._lock:
            phase = self._phase(wall)
            if self._retired or phase is None:
                return
            iid = _integer(getattr(record, "instrument_id", None))
            family = self._mappings.get(iid, ("UNKNOWN",))[0]
            # Do not rewrite UNKNOWN observations when a later mapping arrives.
            if family not in {"SPX", "NDX", "VIX", "RUT", "OTHER", "UNKNOWN"}:
                family = "OTHER"
            self._timing_admission_counts[(phase, family, reason)] += 1
            details = {
                "reason": reason, "admitted": reason == "ADMITTED",
                "record_generation": int(record_generation),
                "sample_count": sample_count, "minimum_samples": minimum_samples,
                "scope": "shared_timing_window_inclusion_only",
                "semantics": "first outcome per family/reason/phase; not a complete record trace",
            }
            # Event deduplication bounds writes independently from the exact
            # observation counters. Normal capacity preserves reserved capture
            # control events; saturation remains explicit in writer/observer status.
            return self._emit("provider_timing_admission", family, wall, mono,
                              record=lambda: {**self._record(record),
                                              "reader_processed_at_ns": reader_processed_at_ns},
                              details=details, critical=False)

    def milestone(self, kind, *, family=None, observed_at_ns=None,
                  observed_monotonic_ns=None, details=None, allow_after_retirement=False):
        wall, mono = self._stamp(observed_at_ns, observed_monotonic_ns)
        with self._lock:
            late_kinds = {"sampler_dispatch", "sampler_submission_requested",
                          "orb_attempt_outcome", "retired_worker_outcome",
                          "qualified_quote_inputs", "capture_gates_observed", "first_bucket_dispatch_audit"}
            if self._retired and not (allow_after_retirement and kind in late_kinds):
                return None
            fields = dict(details or {})
            if self._retired:
                fields["observed_after_generation_retirement"] = True
            return self._emit(str(kind)[:80], family or "SHARED", wall, mono,
                              details=fields, force=bool(self._retired))

    def retire(self, reason, *, observed_at_ns=None, observed_monotonic_ns=None):
        wall, mono = self._stamp(observed_at_ns, observed_monotonic_ns)
        with self._lock:
            if self._retired:
                return
            self._emit("generation_retired", "SHARED", wall, mono,
                       details={"reason": reason, "observer_overflow": self._observer_overflow,
                                "callback_errors": self._callback_errors,
                                "writer_status_before_retirement": self.writer.status()}, force=True)
            self._retired = True

    def snapshot(self):
        with self._lock:
            result = {
                "schema_version": SCHEMA, "authority": "DIAGNOSTIC_ONLY",
                "subscription_epoch_id": self.epoch_id,
                "subscription_generation": self.generation,
                "retired": self._retired, "retained_instrument_phase_receipts": len(self._receipts),
                "retained_mappings": len(self._mappings), "emitted_first_event_keys": len(self._seen) + len(self._critical_seen),
                "observer_overflow": self._observer_overflow, "callback_errors": self._callback_errors,
                "critical_latest": list(self._critical_latest.values()),
                "upstream_timing": UNKNOWN_UPSTREAM,
                "timing_admission_counts": [
                    {"phase": phase, "family_at_observation": family,
                     "reason": reason, "observation_count": count}
                    for (phase, family, reason), count in sorted(self._timing_admission_counts.items())
                ],
                "timing_admission_count_semantics": (
                    "DIAGNOSTIC_OBSERVATIONS_NOT_DISTINCT_RECORDS_OR_SYNCHRONIZATION; "
                    "generation-owned phase/family/reason counts; UNKNOWN attribution is immutable; "
                    "snapshot counts are not independently fsync acknowledged"
                ),
            }
            critical_rows = list(self._critical_latest.values())
        writer_status = self.writer.status()
        important = {
            "first_live_sdk_quote", "first_reader_processed_quote",
            "first_cache_admitted_quote", "first_quote_rejection",
            "qualified_quote_inputs", "orb_attempt_outcome", "orb_outcome",
            "sampler_dispatch", "first_bucket_dispatch_audit",
            "provider_timing_count", "handoff_state", "shared_handoff_changed", "shared_handoff",
            "capture_gates_observed", "retired_worker_outcome", "connection_failed",
            "generation_retired",
        }
        compact = []
        for event in critical_rows:
            if event["kind"] not in important:
                continue
            row = {key: event.get(key) for key in (
                "event_id", "sequence", "kind", "family_at_observation", "phase",
                "provider_interval_end_ns", "local_observed_at_ns",
                "local_observed_monotonic_ns", "sdk_callback_received_at_ns",
                "sdk_callback_received_monotonic_ns", "reader_processed_at_ns",
                "cache_admitted_at_ns", "quote_rejected_at_ns",
                "writer_enqueue_accepted", "writer_enqueue_order",
            )}
            fields = event.get("details", {})
            row["details"] = {key: fields[key] for key in (
                "status", "state", "reason", "stage", "recorded",
                "progress_eligible", "write_outcome", "missing_required_families",
                "handoff_status", "processing_clock_sample_count", "sample_count",
                "minimum_samples", "scope", "previous_status", "required_symbols",
                "sampler_acceptance", "accepted_final_decision", "worker_final_decision_acknowledged", "raw_recorded",
                "progress_decision_recorded", "progress_decision_pending",
                "observed_after_generation_retirement",
            ) if key in fields}
            order = event.get("writer_enqueue_order")
            row["persistence_status"] = (
                "NOT_ENQUEUED_MISSING" if not event["writer_enqueue_accepted"] else
                "FSYNC_ACKNOWLEDGED" if order is not None and order <= writer_status["fsync_acknowledged_order"] else
                "ENQUEUED_PENDING_OR_UNKNOWN_NOT_FSYNC_ACKNOWLEDGED"
            )
            compact.append(row)
        # Retain shared terminal/critical states under the fixed cap as well as
        # instrument first stages. Event IDs always link to detailed NDJSON.
        compact.sort(key=lambda row: (
            row["kind"] != "generation_retired",
            row["family_at_observation"] not in {"NDX", "SPX"},
            row["sequence"],
        ))
        result["compact_critical_summary"] = compact[:64]
        result["compact_critical_summary_truncated"] = len(compact) > 64
        result["writer"] = writer_status
        return result
