import pandas as pd

from meterflow.generator import generate_batch
from meterflow.validate import validate
from tests.conftest import INTERVAL_END, INTERVAL_START


def test_clean_batches_pass_untouched():
    df = generate_batch(INTERVAL_START, meters=10, faults=False)

    valid, rejects = validate(df, INTERVAL_START, INTERVAL_END)

    assert len(valid) == len(df) and rejects.empty


def test_every_invalid_row_is_rejected_with_a_reason():
    df = generate_batch(INTERVAL_START, meters=20)

    valid, rejects = validate(df, INTERVAL_START, INTERVAL_END)

    assert len(valid) + len(rejects) == len(df)
    reasons = " | ".join(rejects["reason"])
    assert "kwh: greater_than_or_equal_to(0)" in reasons
    assert "meter_id: str_matches" in reasons
    assert "ts: outside batch interval" in reasons
    assert (valid["kwh"] >= 0).all()
    assert valid["meter_id"].str.match(r"^MTR-\d{6}$").all()


def test_outage_readings_are_valid_not_rejected():
    df = generate_batch(INTERVAL_START, meters=20)

    valid, _ = validate(df, INTERVAL_START, INTERVAL_END)

    assert (valid["voltage"] == 0).any()


def test_a_row_with_several_problems_lists_them_all():
    df = generate_batch(INTERVAL_START, meters=2, faults=False)
    df.loc[0, ["meter_id", "kwh"]] = ["bad", -1.0]

    _, rejects = validate(df, INTERVAL_START, INTERVAL_END)

    assert len(rejects) == 1
    assert "meter_id" in rejects.loc[0, "reason"] and "kwh" in rejects.loc[0, "reason"]


def test_unexpected_columns_are_dropped_not_fatal():
    df = generate_batch(INTERVAL_START, meters=2, faults=False).assign(firmware="v2")

    valid, rejects = validate(df, INTERVAL_START, INTERVAL_END)

    assert "firmware" not in valid.columns and rejects.empty


def test_an_all_invalid_batch_returns_no_valid_rows():
    df = generate_batch(INTERVAL_START, meters=2, faults=False).assign(kwh=-1.0)

    valid, rejects = validate(df, INTERVAL_START, INTERVAL_END)

    assert valid.empty and len(rejects) == len(df)
    assert isinstance(valid, pd.DataFrame)
