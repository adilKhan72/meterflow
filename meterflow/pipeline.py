"""One data interval, end to end, as plain functions. The Airflow DAG calls these; tests call them directly.

Intermediate results go to files beside the batch (small metadata only through XCom, never data frames).
"""

import os
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg

from meterflow.generator import generate_batch, read_batch, write_batch
from meterflow.load import apply_schema, load_readings, refresh_aggregates, save_rejects
from meterflow.timeutil import utc
from meterflow.transform import transform
from meterflow.validate import validate


@dataclass(frozen=True)
class Settings:
    database_url: str
    landing_dir: Path
    simulated_meters: int = 50

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_url=os.environ["METERFLOW_DATABASE_URL"],
            landing_dir=Path(os.environ.get("METERFLOW_LANDING_DIR", "/tmp/meterflow/landing")),
            simulated_meters=int(os.environ.get("METERFLOW_SIMULATED_METERS", "50")),
        )


def hourly_interval(data_interval_start: datetime) -> tuple[datetime, datetime]:
    """The UTC hour this run processes.

    Scheduled and backfill runs of an @hourly DAG already get exactly this. Manual runs (and `dag.test()`) in Airflow 3
    get a zero-width interval (start == end), which would process nothing, so the batch is always the hour containing
    the interval start.
    """
    start = utc(data_interval_start).floor("h")
    return start.to_pydatetime(), (start + pd.Timedelta(hours=1)).to_pydatetime()


def batch_id(interval_start: datetime) -> str:
    return utc(interval_start).strftime("%Y%m%dT%H")


def batch_dir(settings: Settings, interval_start: datetime) -> Path:
    return settings.landing_dir / utc(interval_start).strftime("%Y/%m/%d/%H")


def land(settings: Settings, interval_start: datetime) -> str:
    """Stand-in for the upstream feed: writes this interval's batch if it isn't there yet (never overwrites a real
    delivery). In production this task is replaced by a sensor on the landing bucket."""
    path = batch_dir(settings, interval_start) / "readings.csv"
    if not path.exists():
        write_batch(generate_batch(interval_start, meters=settings.simulated_meters), settings.landing_dir, interval_start)
    return str(path)


def ensure_schema(settings: Settings) -> None:
    """Once per run, before anything touches the database (see schema.sql for why it's cheap and lock-safe)."""
    with psycopg.connect(settings.database_url) as conn:
        apply_schema(conn)


def typical_kwh(settings: Settings, before: datetime) -> dict[str, float]:
    """Each meter's median 15-minute reading over the previous 7 days: the spike baseline."""
    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(
            "SELECT meter_id, percentile_cont(0.5) WITHIN GROUP (ORDER BY kwh) FROM readings "
            "WHERE ts >= %s - INTERVAL '7 days' AND ts < %s AND NOT spike GROUP BY meter_id",
            (before, before),
        ).fetchall()
    return {meter: float(median) for meter, median in rows}


def validate_and_transform(settings: Settings, raw_path: str, interval_start: datetime, interval_end: datetime) -> dict:
    raw = read_batch(Path(raw_path))
    valid, rejects = validate(raw, interval_start, interval_end)
    clean = transform(valid, utc(interval_start), typical_kwh(settings, interval_start))
    out = batch_dir(settings, interval_start)
    clean.to_csv(out / "clean.csv", index=False)
    rejects.to_csv(out / "rejects.csv", index=False)
    return {
        "clean_path": str(out / "clean.csv"),
        "rejects_path": str(out / "rejects.csv"),
        "raw_rows": len(raw),
        "clean_rows": len(clean),
        "rejected_rows": len(rejects),
        "estimated_rows": int(clean["estimated"].sum()),
        "spikes": int(clean["spike"].sum()),
    }


def load(settings: Settings, staged: dict, interval_start: datetime) -> dict:
    clean = pd.read_csv(staged["clean_path"], dtype={"meter_id": str}, parse_dates=["ts", "delivered_at"])
    rejects = pd.read_csv(staged["rejects_path"], dtype=str).fillna("")
    bid = batch_id(interval_start)
    with psycopg.connect(settings.database_url) as conn:
        written = load_readings(conn, clean, bid)
        rejected = save_rejects(conn, rejects, bid)
    return {**staged, "batch_id": bid, "rows_written": written, "rejects_saved": rejected}


def refresh(settings: Settings, interval_start: datetime, interval_end: datetime) -> None:
    refresh_aggregates(settings.database_url, utc(interval_start).to_pydatetime(), utc(interval_end).to_pydatetime())


def run_interval(settings: Settings, interval_start: datetime, interval_end: datetime) -> dict:
    """The whole interval without Airflow (tests, local runs, one-off backfills)."""
    ensure_schema(settings)
    staged = validate_and_transform(settings, land(settings, interval_start), interval_start, interval_end)
    loaded = load(settings, staged, interval_start)
    refresh(settings, interval_start, interval_end)
    return loaded


__all__ = ["Settings", "asdict", "run_interval"]
