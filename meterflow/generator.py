"""Synthetic smart-meter batches with the faults real feeds have, so validation and transforms have something to do.

Deterministic for a given (interval, seed): tests and backfills reproduce exactly.
"""

from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from meterflow.timeutil import utc

READING_MINUTES = 15
FAULTS = ("redelivery", "negative", "bad_meter_id", "future_ts", "missing", "spike", "outage")


def meter_ids(n: int) -> list[str]:
    return [f"MTR-{i:06d}" for i in range(1, n + 1)]


def _load_profile(hour: int) -> float:
    """Residential-ish daily curve: low at night, morning and evening peaks."""
    return 0.35 + 0.45 * np.exp(-((hour - 8) ** 2) / 6) + 0.8 * np.exp(-((hour - 19) ** 2) / 5)


def generate_batch(interval_start: datetime, meters: int = 50, seed: int = 7, faults: bool = True) -> pd.DataFrame:
    """One hour of 15-minute readings for `meters` meters, delivered just after the hour."""
    rng = np.random.default_rng([seed, int(interval_start.timestamp())])
    stamps = pd.date_range(utc(interval_start), periods=60 // READING_MINUTES, freq=f"{READING_MINUTES}min")
    ids = meter_ids(meters)
    base = rng.uniform(0.6, 1.6, size=meters)  # per-meter household size

    rows = []
    for meter, scale in zip(ids, base):
        for ts in stamps:
            kwh = scale * _load_profile(ts.hour) / 4 * rng.normal(1.0, 0.08)
            rows.append((meter, ts, round(max(kwh, 0.0), 4), round(rng.normal(230, 2), 1)))
    df = pd.DataFrame(rows, columns=["meter_id", "ts", "kwh", "voltage"])
    df["delivered_at"] = utc(interval_start) + pd.Timedelta(hours=1, minutes=2)

    if faults:
        df = _inject_faults(df, rng, interval_start)
    return df.sample(frac=1.0, random_state=int(rng.integers(1_000_000))).reset_index(drop=True)  # arrival order is not time order


def _inject_faults(df: pd.DataFrame, rng: np.random.Generator, interval_start: datetime) -> pd.DataFrame:
    pick = lambda: int(rng.integers(len(df)))  # noqa: E731
    df = df.copy()

    # A corrected re-delivery of an existing reading (later delivered_at wins).
    redelivered = df.iloc[[pick()]].copy()
    redelivered["kwh"] = round(float(redelivered["kwh"].iloc[0]) * 1.05, 4)
    redelivered["delivered_at"] = redelivered["delivered_at"] + pd.Timedelta(minutes=20)
    df = pd.concat([df, redelivered], ignore_index=True)

    df.loc[pick(), "kwh"] = -0.5  # impossible negative reading
    df.loc[pick(), "meter_id"] = "MTR-12AB"  # malformed id
    df.loc[pick(), "ts"] = utc(interval_start) + pd.Timedelta(days=3)  # clock drift into the future
    df.loc[pick(), "kwh"] = round(float(df["kwh"].median()) * 25, 4)  # spike
    df.loc[pick(), "voltage"] = 0.0  # supply outage: valid reading, flagged downstream
    return df.drop(index=[pick() for _ in range(3)]).reset_index(drop=True)  # missing readings


def write_batch(df: pd.DataFrame, landing: Path, interval_start: datetime) -> Path:
    path = landing / interval_start.strftime("%Y/%m/%d/%H") / "readings.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return path


def read_batch(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype={"meter_id": str}, parse_dates=["ts", "delivered_at"])
