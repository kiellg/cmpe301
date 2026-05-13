from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


# Ideal cycle time used for the performance factor.
IDEAL_CYCLE_TIME_SECONDS = 20.0


@dataclass(frozen=True)
class OeeMetrics:
    availability: float
    performance: float
    quality: float
    planned_production_time_s: float
    operating_time_s: float
    completed_cycles: int
    failed_cycles: int
    interval_start: str
    interval_end: str
    availability_mode: str
    quality_mode: str

    @property
    def oee(self) -> float:
        return self.availability * self.performance * self.quality


def calculate_oee(
    records: list[dict],
    interval_start: str,
    interval_end: str,
    *,
    ideal_cycle_time_seconds: float = IDEAL_CYCLE_TIME_SECONDS,
) -> OeeMetrics:
    start_dt = _parse_iso(interval_start)
    end_dt = _parse_iso(interval_end)
    planned_production_time_s = max((end_dt - start_dt).total_seconds(), 0.0)

    operating_time_s = 0.0
    completed_cycles = 0
    failed_cycles = 0
    defect_units = 0

    for record in records:
        record_start, record_end = _record_window(record)
        if record_start is not None and record_end is not None:
            operating_time_s += _overlap_seconds(record_start, record_end, start_dt, end_dt)

        if bool(record.get("cycle_complete")):
            completed_cycles += 1

        if _is_failed_record(record):
            failed_cycles += 1

        defect_units += max(int(record.get("defect_count") or 0), 0)

    availability = _safe_ratio(operating_time_s, planned_production_time_s)
    performance = _safe_ratio(completed_cycles * ideal_cycle_time_seconds, operating_time_s)

    # Quality is simplified for this lab MES: use real defect counts when
    # present, otherwise fall back to a cycle-level failed/complete ratio if
    # failures are explicitly logged. If neither exists, default to 100%.
    if defect_units > 0:
        good_units = sum(max(int(record.get("good_units") or 0), 0) for record in records)
        quality = _safe_ratio(good_units, good_units + defect_units)
        quality_mode = "unit_counts"
    elif failed_cycles > 0 and (completed_cycles + failed_cycles) > 0:
        quality = _safe_ratio(completed_cycles, completed_cycles + failed_cycles)
        quality_mode = "cycle_failure_approx"
    else:
        quality = 1.0
        quality_mode = "default_100"

    return OeeMetrics(
        availability=availability,
        performance=performance,
        quality=quality,
        planned_production_time_s=planned_production_time_s,
        operating_time_s=operating_time_s,
        completed_cycles=completed_cycles,
        failed_cycles=failed_cycles,
        interval_start=interval_start,
        interval_end=interval_end,
        availability_mode="summed_logged_durations",
        quality_mode=quality_mode,
    )


def _is_failed_record(record: dict) -> bool:
    final_status = str(record.get("final_status") or "").strip().lower()
    return final_status == "failed" or bool(record.get("fault_code"))


def _record_window(record: dict) -> tuple[datetime | None, datetime | None]:
    start_raw = record.get("actual_start") or record.get("logged_at") or record.get("actual_end")
    end_raw = record.get("actual_end") or record.get("logged_at") or record.get("actual_start")
    if not start_raw or not end_raw:
        return None, None

    start_dt = _parse_iso(str(start_raw))
    end_dt = _parse_iso(str(end_raw))
    if end_dt < start_dt:
        return start_dt, start_dt
    return start_dt, end_dt


def _overlap_seconds(
    record_start: datetime,
    record_end: datetime,
    window_start: datetime,
    window_end: datetime,
) -> float:
    overlap_start = max(record_start, window_start)
    overlap_end = min(record_end, window_end)
    if overlap_end <= overlap_start:
        return 0.0
    return (overlap_end - overlap_start).total_seconds()


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return max(float(numerator), 0.0) / float(denominator)
