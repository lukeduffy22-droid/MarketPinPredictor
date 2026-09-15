"""Build discontinuous research history from retained OPRA gamma snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import math
from typing import Any, Iterable

from app.utils.display_time import parse_utc_timestamp
from app.utils.snapshot_history import (
    DIAGNOSTIC_INVALID_SNAPSHOT,
    canonical_subscription_epoch_id,
    gamma_snapshot_provenance_status,
    positive_subscription_generation,
)


OPRA_PARITY_SPOT_SOURCE = "databento_opra_put_call_parity"
OPRA_PARITY_HISTORY_NOTICE = (
    "Research only: the spot line is inferred from stored OPRA option put-call "
    "parity snapshots, and the pin line is a stored gamma calculation. This is "
    "not official exchange OHLCV and is not a validated current forecast. Lines "
    "are split at every subscription epoch or generation change; no interpolation "
    "crosses those gaps."
)


@dataclass(frozen=True)
class OpraParityHistoryPoint:
    timestamp_utc: datetime
    parity_spot: float
    gamma_pin: float
    subscription_epoch_id: str
    subscription_generation: int
    calculation_id: str | None
    provenance_status: str


@dataclass(frozen=True)
class OpraParityHistorySegment:
    subscription_epoch_id: str
    subscription_generation: int
    points: tuple[OpraParityHistoryPoint, ...]


@dataclass(frozen=True)
class OpraParityGammaHistory:
    segments: tuple[OpraParityHistorySegment, ...]
    included_records: int
    excluded_records: int


@dataclass(frozen=True)
class OpraParityObservationReference:
    """Non-predictive spot reference from the latest contiguous identity segment."""

    subscription_epoch_id: str
    subscription_generation: int
    first_timestamp_utc: datetime
    last_timestamp_utc: datetime
    sample_count: int
    reference_value: float | None
    range_low: float | None
    range_high: float | None


def _positive_finite(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and number > 0.0 else None


def _snapshot_timestamp(record: dict[str, Any]) -> datetime | None:
    timestamp = parse_utc_timestamp(record.get("generated_at_utc"))
    if timestamp is not None:
        return timestamp
    return parse_utc_timestamp(record.get("timestamp_utc"))


def build_opra_parity_gamma_history(
    records: Iterable[dict[str, Any]],
) -> OpraParityGammaHistory:
    """Select valid retained parity/pin points and split identity boundaries.

    Canonical epoch and generation identifiers are mandatory because a record
    without them cannot prove where a discontinuity belongs. The returned
    segments are contiguous in timestamp order; callers must render each as a
    separate trace rather than joining or interpolating between segments.
    """

    candidates: list[tuple[datetime, int, OpraParityHistoryPoint]] = []
    total_records = 0
    for sequence, record in enumerate(records):
        total_records += 1
        provenance_status = gamma_snapshot_provenance_status(record)
        if provenance_status == DIAGNOSTIC_INVALID_SNAPSHOT:
            continue
        if record.get("spot_source") != OPRA_PARITY_SPOT_SOURCE:
            continue

        epoch_id = canonical_subscription_epoch_id(
            record.get("subscription_epoch_id")
        )
        generation = positive_subscription_generation(
            record.get("subscription_generation")
        )
        timestamp_utc = _snapshot_timestamp(record)
        parity_spot = _positive_finite(record.get("spot_last"))
        gamma_pin = _positive_finite(record.get("primary_gamma_pin_strike"))
        if gamma_pin is None:
            gamma_pin = _positive_finite(record.get("gamma_pin_strike"))
        if (
            epoch_id is None
            or generation is None
            or timestamp_utc is None
            or parity_spot is None
            or gamma_pin is None
        ):
            continue

        calculation_id = record.get("calculation_id")
        candidates.append(
            (
                timestamp_utc,
                sequence,
                OpraParityHistoryPoint(
                    timestamp_utc=timestamp_utc,
                    parity_spot=parity_spot,
                    gamma_pin=gamma_pin,
                    subscription_epoch_id=epoch_id,
                    subscription_generation=generation,
                    calculation_id=(
                        str(calculation_id) if calculation_id is not None else None
                    ),
                    provenance_status=provenance_status,
                ),
            )
        )

    candidates.sort(key=lambda item: (item[0], item[1]))
    segment_builders: list[list[OpraParityHistoryPoint]] = []
    segment_keys: list[tuple[str, int]] = []
    for _timestamp, _sequence, point in candidates:
        key = (point.subscription_epoch_id, point.subscription_generation)
        if not segment_keys or segment_keys[-1] != key:
            segment_keys.append(key)
            segment_builders.append([])
        segment_builders[-1].append(point)

    segments = tuple(
        OpraParityHistorySegment(
            subscription_epoch_id=key[0],
            subscription_generation=key[1],
            points=tuple(points),
        )
        for key, points in zip(segment_keys, segment_builders)
    )
    included_records = len(candidates)
    return OpraParityGammaHistory(
        segments=segments,
        included_records=included_records,
        excluded_records=total_records - included_records,
    )


def latest_contiguous_parity_reference(
    history: OpraParityGammaHistory,
) -> OpraParityObservationReference | None:
    """Describe only the final contiguous segment; never merge matching identities.

    Two or more retained points support an observed min/max envelope. A single
    point is exposed only as a reference value because it cannot define a
    range. The history builder has already enforced finite positive values and
    canonical epoch/generation identity.
    """

    if not history.segments:
        return None
    segment = history.segments[-1]
    if not segment.points:
        return None

    values = tuple(point.parity_spot for point in segment.points)
    sample_count = len(values)
    return OpraParityObservationReference(
        subscription_epoch_id=segment.subscription_epoch_id,
        subscription_generation=segment.subscription_generation,
        first_timestamp_utc=segment.points[0].timestamp_utc,
        last_timestamp_utc=segment.points[-1].timestamp_utc,
        sample_count=sample_count,
        reference_value=values[0] if sample_count == 1 else None,
        range_low=min(values) if sample_count >= 2 else None,
        range_high=max(values) if sample_count >= 2 else None,
    )
