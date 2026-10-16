from meterflow.generator import FAULTS, generate_batch, meter_ids
from tests.conftest import INTERVAL_START


def test_batches_are_deterministic_for_an_interval_and_seed():
    a = generate_batch(INTERVAL_START, meters=10, seed=1)
    b = generate_batch(INTERVAL_START, meters=10, seed=1)

    assert a.equals(b)


def test_a_clean_batch_has_four_readings_per_meter_in_the_hour():
    df = generate_batch(INTERVAL_START, meters=10, faults=False)

    assert len(df) == 40
    assert set(df["meter_id"]) == set(meter_ids(10))
    assert df.groupby("meter_id").size().eq(4).all()
    assert (df["kwh"] >= 0).all()


def test_faulty_batches_contain_each_fault_kind():
    df = generate_batch(INTERVAL_START, meters=20)

    assert (df["kwh"] < 0).any()
    assert (~df["meter_id"].str.match(r"^MTR-\d{6}$")).any()
    assert (df["ts"] >= INTERVAL_START.replace(day=16)).any()
    assert df.duplicated(subset=["meter_id", "ts"], keep=False).any()
    assert (df["voltage"] == 0).any()
    assert len(FAULTS) == 7
