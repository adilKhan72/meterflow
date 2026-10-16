"""Data-quality gate (ADR-0003): a run that loads bad or incomplete data must fail, not quietly succeed."""

import os
from dataclasses import dataclass, field

from meterflow.generator import READING_MINUTES

SLOTS_PER_HOUR = 60 // READING_MINUTES


class DataQualityError(RuntimeError):
    """Raised to fail the Airflow task; the message lists every breached threshold."""


@dataclass(frozen=True)
class Thresholds:
    max_reject_rate: float = 0.05
    min_coverage: float = 0.95
    # The last reading of an hour is stamped :45, so up to 15 minutes of lag is normal for complete data.
    max_freshness_lag_s: float = 30 * 60

    @classmethod
    def from_env(cls) -> "Thresholds":
        return cls(
            max_reject_rate=float(os.environ.get("METERFLOW_MAX_REJECT_RATE", cls.max_reject_rate)),
            min_coverage=float(os.environ.get("METERFLOW_MIN_COVERAGE", cls.min_coverage)),
            max_freshness_lag_s=float(os.environ.get("METERFLOW_MAX_FRESHNESS_LAG_S", cls.max_freshness_lag_s)),
        )


@dataclass
class QualityResult:
    reject_rate: float
    coverage: float
    freshness_lag_s: float | None
    expected_rows: int
    stored_rows: int
    failures: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.failures


def evaluate(raw_rows: int, rejected_rows: int, active_meters: int, stored_rows: int, freshness_lag_s: float | None, t: Thresholds) -> QualityResult:
    expected = active_meters * SLOTS_PER_HOUR
    result = QualityResult(
        reject_rate=round(rejected_rows / raw_rows, 4) if raw_rows else 1.0,
        coverage=round(stored_rows / expected, 4) if expected else 0.0,
        freshness_lag_s=freshness_lag_s,
        expected_rows=expected,
        stored_rows=stored_rows,
    )
    if raw_rows == 0:
        result.failures.append("empty batch: no readings received")
    if result.reject_rate > t.max_reject_rate:
        result.failures.append(f"reject rate {result.reject_rate:.2%} > {t.max_reject_rate:.2%}")
    if result.coverage < t.min_coverage:
        result.failures.append(f"coverage {result.coverage:.2%} < {t.min_coverage:.2%} ({stored_rows}/{expected} readings)")
    if freshness_lag_s is None:
        result.failures.append("no readings stored for this interval")
    elif freshness_lag_s > t.max_freshness_lag_s:
        result.failures.append(f"newest reading is {freshness_lag_s / 60:.0f} min before interval end (> {t.max_freshness_lag_s / 60:.0f} min)")
    return result
