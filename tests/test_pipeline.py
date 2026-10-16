"""run_interval end to end against TimescaleDB, with no Airflow involved."""

import os
from datetime import timedelta

import pytest

from meterflow.pipeline import Settings, run_interval
from tests.conftest import INTERVAL_END, INTERVAL_START


@pytest.fixture
def settings(db, tmp_path):
    return Settings(database_url=os.environ["DATABASE_URL"], landing_dir=tmp_path, simulated_meters=20)


def test_an_interval_lands_validates_loads_and_aggregates(settings, db):
    result = run_interval(settings, INTERVAL_START, INTERVAL_END)

    assert result["batch_id"] == "20261015T18"
    assert result["rejected_rows"] > 0 and result["clean_rows"] > 0
    assert db.execute("SELECT count(*) FROM readings").fetchone()[0] == result["clean_rows"]
    assert db.execute("SELECT count(*) FROM rejects WHERE batch_id = '20261015T18'").fetchone()[0] == result["rejected_rows"]
    assert db.execute("SELECT count(DISTINCT meter_id) FROM readings_hourly").fetchone()[0] == 20


def test_rerunning_an_interval_changes_nothing(settings, db):
    run_interval(settings, INTERVAL_START, INTERVAL_END)
    before = db.execute("SELECT count(*), sum(kwh) FROM readings").fetchone()

    run_interval(settings, INTERVAL_START, INTERVAL_END)

    assert db.execute("SELECT count(*), sum(kwh) FROM readings").fetchone() == before


def test_history_feeds_the_spike_baseline(settings, db):
    previous = INTERVAL_START - timedelta(hours=1)
    run_interval(settings, previous, INTERVAL_START)

    result = run_interval(settings, INTERVAL_START, INTERVAL_END)

    # The injected 25x spike is caught against each meter's own history.
    assert result["spikes"] >= 1
    assert db.execute("SELECT count(*) FROM readings WHERE spike").fetchone()[0] >= 1


def test_hourly_interval_is_the_hour_containing_the_run_start():
    from datetime import datetime, timezone

    from meterflow.pipeline import hourly_interval

    # A manual Airflow 3 run has start == end; the batch is still a well-defined hour.
    start, end = hourly_interval(datetime(2026, 10, 15, 6, 0, tzinfo=timezone.utc))
    assert (start, end) == (datetime(2026, 10, 15, 6, tzinfo=timezone.utc), datetime(2026, 10, 15, 7, tzinfo=timezone.utc))
    assert hourly_interval(datetime(2026, 10, 15, 6, 41, tzinfo=timezone.utc)) == (start, end)
