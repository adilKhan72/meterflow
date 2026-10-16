"""DAG integrity and a real end-to-end `dag.test()` run. Needs Airflow (CI's airflow job); skipped otherwise."""

import importlib.util
import os
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")

DAG_FILE = Path(__file__).resolve().parents[2] / "dags" / "meter_readings.py"


def load_dag():
    spec = importlib.util.spec_from_file_location("meter_readings", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dag_object


def test_dag_shape_and_policies():
    dag = load_dag()

    assert dag.dag_id == "meter_readings_hourly"
    assert dag.catchup is False and dag.max_active_runs == 4
    assert set(dag.task_ids) == {"land", "validate_and_transform", "load", "refresh_aggregates"}
    assert dag.get_task("validate_and_transform").upstream_task_ids == {"land"}
    assert dag.get_task("load").upstream_task_ids == {"validate_and_transform"}
    assert dag.get_task("refresh_aggregates").upstream_task_ids == {"load"}
    assert all(t.retries == 2 for t in dag.tasks)


@pytest.mark.skipif(not os.environ.get("METERFLOW_DATABASE_URL"), reason="needs TimescaleDB and an Airflow metadata DB")
def test_dag_runs_end_to_end(tmp_path, monkeypatch):
    import pendulum
    import psycopg

    monkeypatch.setenv("METERFLOW_LANDING_DIR", str(tmp_path))
    monkeypatch.setenv("METERFLOW_SIMULATED_METERS", "10")
    dag = load_dag()

    run = dag.test(logical_date=pendulum.datetime(2026, 10, 15, 6, tz="UTC"))

    assert str(run.state) in ("success", "DagRunState.SUCCESS")
    with psycopg.connect(os.environ["METERFLOW_DATABASE_URL"]) as conn:
        loaded = conn.execute(
            "SELECT count(*), count(DISTINCT meter_id) FROM readings WHERE ts >= %s AND ts < %s",
            (run.data_interval_start, run.data_interval_end),
        ).fetchone()
    assert loaded[0] > 0 and loaded[1] == 10
