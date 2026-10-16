"""Idempotent loading into TimescaleDB (ADR-0002): COPY → temp staging → upsert keyed by (meter_id, ts)."""

import json
import time
from datetime import datetime, timedelta
from importlib import resources

import pandas as pd
import psycopg

COLUMNS = ["meter_id", "ts", "kwh", "voltage", "delivered_at", "estimated", "spike", "outage", "batch_id"]


def apply_schema(conn: psycopg.Connection) -> None:
    conn.execute(resources.files("meterflow").joinpath("schema.sql").read_text())
    conn.commit()


def load_readings(conn: psycopg.Connection, df: pd.DataFrame, batch_id: str) -> int:
    """Returns rows inserted or updated. A row only replaces an existing one if it was delivered later, so
    re-running an old batch after a newer correction can't roll the correction back."""
    if df.empty:
        return 0
    rows = df.assign(batch_id=batch_id)[COLUMNS]
    with conn.transaction():
        conn.execute("CREATE TEMP TABLE staging (LIKE readings INCLUDING DEFAULTS) ON COMMIT DROP")
        with conn.cursor().copy(f"COPY staging ({', '.join(COLUMNS)}) FROM STDIN") as copy:
            for row in rows.itertuples(index=False, name=None):
                copy.write_row(row)
        cur = conn.execute(
            f"""
            INSERT INTO readings ({', '.join(COLUMNS)})
            SELECT {', '.join(COLUMNS)} FROM staging
            ON CONFLICT (meter_id, ts) DO UPDATE SET
                kwh = excluded.kwh, voltage = excluded.voltage, delivered_at = excluded.delivered_at,
                estimated = excluded.estimated, spike = excluded.spike, outage = excluded.outage,
                batch_id = excluded.batch_id
            WHERE excluded.delivered_at >= readings.delivered_at
            """
        )
        return cur.rowcount


def save_rejects(conn: psycopg.Connection, rejects: pd.DataFrame, batch_id: str) -> int:
    """Re-running a batch replaces its rejects instead of duplicating them."""
    with conn.transaction():
        conn.execute("DELETE FROM rejects WHERE batch_id = %s", (batch_id,))
        if rejects.empty:
            return 0
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO rejects (batch_id, meter_id, ts, raw, reason) VALUES (%s, %s, %s, %s, %s)",
                [
                    (batch_id, str(r.get("meter_id")), str(r.get("ts")), json.dumps({k: str(v) for k, v in r.items() if k != "reason"}), r["reason"])
                    for r in rejects.to_dict(orient="records")
                ],
            )
        return len(rejects)


def _refresh(conn: psycopg.Connection, view: str, start: datetime, end: datetime, attempts: int = 6) -> None:
    """TimescaleDB rejects a refresh while its own refresh policy (or another run) is refreshing the same window.
    That other refresh finishing is exactly what we're waiting for, so back off and retry; anything else is raised."""
    for attempt in range(attempts):
        try:
            conn.execute(f"CALL refresh_continuous_aggregate('{view}', %s, %s)", (start, end))
            return
        except psycopg.errors.LockNotAvailable:
            if attempt == attempts - 1:
                raise
            time.sleep(0.5 * 2**attempt)


def refresh_aggregates(url: str, start: datetime, end: datetime) -> None:
    """Continuous-aggregate refreshes can't run inside a transaction: use a dedicated autocommit connection.
    The hourly view is refreshed first because the daily one is built from it."""
    with psycopg.connect(url, autocommit=True) as conn:
        _refresh(conn, "readings_hourly", start, end)
        # Daily buckets must be refreshed whole: cover every UTC day the interval touches.
        day_start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = end.replace(hour=0, minute=0, second=0, microsecond=0)
        if day_end < end:
            day_end += timedelta(days=1)
        _refresh(conn, "readings_daily", day_start, day_end)
