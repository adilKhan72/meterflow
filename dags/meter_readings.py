"""Hourly smart-meter pipeline (Airflow 3, TaskFlow API).

One run per hourly data interval: land → validate & transform → load → refresh aggregates. Each run touches only its
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
    def land(data_interval_start=None) -> str:
        return pipeline.land(pipeline.Settings.from_env(), data_interval_start)

    @task
    def validate_and_transform(raw_path: str, data_interval_start=None, data_interval_end=None) -> dict:
        return pipeline.validate_and_transform(pipeline.Settings.from_env(), raw_path, data_interval_start, data_interval_end)

    @task
    def load(staged: dict, data_interval_start=None) -> dict:
        return pipeline.load(pipeline.Settings.from_env(), staged, data_interval_start)

    @task
    def refresh_aggregates(loaded: dict, data_interval_start=None, data_interval_end=None) -> dict:
        pipeline.refresh(pipeline.Settings.from_env(), data_interval_start, data_interval_end)
        return loaded

    refresh_aggregates(load(validate_and_transform(land())))


dag_object = meter_readings_hourly()
