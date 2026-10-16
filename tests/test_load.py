"""Integration tests against TimescaleDB (CI service container; skipped without DATABASE_URL)."""

import os

import pandas as pd

from meterflow.generator import generate_batch
from meterflow.load import load_readings, refresh_aggregates, save_rejects
from meterflow.transform import transform
from meterflow.validate import validate
from tests.conftest import INTERVAL_END, INTERVAL_START


def prepared(meters=10):
    valid, rejects = validate(generate_batch(INTERVAL_START, meters=meters), INTERVAL_START, INTERVAL_END)
    return transform(valid, pd.Timestamp(INTERVAL_START)), rejects


def count(db, sql="SELECT count(*) FROM readings"):
    return db.execute(sql).fetchone()[0]


def test_readings_is_a_hypertable_with_compression_and_retention(db):
    assert count(db, "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'readings'") == 1
    jobs = {r[0] for r in db.execute("SELECT proc_name FROM timescaledb_information.jobs WHERE hypertable_name = 'readings'")}
    assert {"policy_compression", "policy_retention"} <= jobs


def test_loading_the_same_batch_twice_is_idempotent(db):
    clean, rejects = prepared()

    load_readings(db, clean, "b1")
    save_rejects(db, rejects, "b1")
    first = count(db), count(db, "SELECT count(*) FROM rejects")
    load_readings(db, clean, "b1")
    save_rejects(db, rejects, "b1")

    assert (count(db), count(db, "SELECT count(*) FROM rejects")) == first
    assert first[0] == len(clean) and first[1] == len(rejects)


def test_a_later_delivery_wins_but_an_older_rerun_cannot_roll_it_back(db):
    clean, _ = prepared()
    load_readings(db, clean, "b1")
    meter, ts = clean.loc[0, "meter_id"], clean.loc[0, "ts"]

    correction = clean.iloc[[0]].assign(kwh=9.99, delivered_at=clean.loc[0, "delivered_at"] + pd.Timedelta(hours=1))
    load_readings(db, correction, "b1-correction")
    load_readings(db, clean, "b1")  # stale re-run of the original batch

    kwh = db.execute("SELECT kwh FROM readings WHERE meter_id = %s AND ts = %s", (meter, ts)).fetchone()[0]
    assert kwh == 9.99


def test_continuous_aggregates_match_raw_totals(db):
    clean, _ = prepared(meters=5)
    load_readings(db, clean, "b1")

    refresh_aggregates(os.environ["DATABASE_URL"], INTERVAL_START, INTERVAL_END)

    raw = db.execute("SELECT round(sum(kwh)::numeric, 6) FROM readings").fetchone()[0]
    hourly = db.execute("SELECT round(sum(kwh)::numeric, 6) FROM readings_hourly").fetchone()[0]
    daily = db.execute("SELECT round(sum(kwh)::numeric, 6) FROM readings_daily").fetchone()[0]
    assert raw == hourly == daily
    assert count(db, "SELECT count(*) FROM readings_hourly") == 5
