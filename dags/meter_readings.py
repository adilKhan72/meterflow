"""Hourly smart-meter pipeline (Airflow 3, TaskFlow API).

One run per hourly data interval: ensure schema → land → validate & transform → load → refresh aggregates →
quality gate. Each run touches only its
own interval and every write is an upsert (ADR-0002), so retries, catch-up and `airflow backfill create` are safe.
"""

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from meterflow import pipeline


@dag(
    dag_id="meter_readings_hourly",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,  # history is loaded deliberately with `airflow backfill create`, not on deploy
    max_active_runs=4,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1), "retry_exponential_backoff": True},
    tags=["meterflow", "timescaledb"],
    doc_md=__doc__,
)
def meter_readings_hourly():
    @task
    def ensure_schema() -> None:
        pipeline.ensure_schema(pipeline.Settings.from_env())

    # Every task derives the same hour from the run's interval start (see pipeline.hourly_interval).
    @task
    def land(data_interval_start=None) -> str:
        start, _ = pipeline.hourly_interval(data_interval_start)
        return pipeline.land(pipeline.Settings.from_env(), start)

    @task
    def validate_and_transform(raw_path: str, data_interval_start=None) -> dict:
        start, end = pipeline.hourly_interval(data_interval_start)
        return pipeline.validate_and_transform(pipeline.Settings.from_env(), raw_path, start, end)

    @task
    def load(staged: dict, data_interval_start=None) -> dict:
        start, _ = pipeline.hourly_interval(data_interval_start)
        return pipeline.load(pipeline.Settings.from_env(), staged, start)

    @task
    def refresh_aggregates(loaded: dict, data_interval_start=None) -> dict:
        start, end = pipeline.hourly_interval(data_interval_start)
        pipeline.refresh(pipeline.Settings.from_env(), start, end)
        return loaded

    @task
    def quality_gate(loaded: dict, data_interval_start=None) -> dict:
        """Fails the run (→ retries, alerts) if the stored interval breaches a threshold (ADR-0003)."""
        start, end = pipeline.hourly_interval(data_interval_start)
        return pipeline.quality_gate(pipeline.Settings.from_env(), loaded, start, end)

    landed = land()
    ensure_schema() >> landed
    quality_gate(refresh_aggregates(load(validate_and_transform(landed))))


dag_object = meter_readings_hourly()
