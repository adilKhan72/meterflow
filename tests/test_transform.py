import pytest
import pandas as pd

from meterflow.transform import dedupe, fill_small_gaps, flag_anomalies, transform
from tests.conftest import INTERVAL_START

T0 = pd.Timestamp(INTERVAL_START)


def frame(rows):
    return pd.DataFrame(rows, columns=["meter_id", "ts", "kwh", "voltage", "delivered_at"])


def ts(minutes):
    return T0 + pd.Timedelta(minutes=minutes)


def test_dedupe_keeps_the_latest_delivery():
    df = frame([
        ("MTR-000001", ts(0), 0.20, 230.0, ts(62)),
        ("MTR-000001", ts(0), 0.21, 230.0, ts(80)),  # correction
    ])

    out = dedupe(df)

    assert len(out) == 1 and out.loc[0, "kwh"] == 0.21


def test_single_inner_gap_is_interpolated_and_marked_estimated():
    df = frame([
        ("MTR-000001", ts(0), 0.2, 230.0, ts(62)),
        ("MTR-000001", ts(30), 0.4, 230.0, ts(62)),
        ("MTR-000001", ts(45), 0.5, 230.0, ts(62)),
    ])

    out = fill_small_gaps(df, T0)

    filled = out[out["ts"] == ts(15)].iloc[0]
    assert filled["kwh"] == pytest.approx(0.3) and bool(filled["estimated"])
    assert out["estimated"].sum() == 1


def test_edge_gaps_are_left_missing_not_extrapolated():
    df = frame([
        ("MTR-000001", ts(15), 0.2, 230.0, ts(62)),
        ("MTR-000001", ts(30), 0.4, 230.0, ts(62)),
    ])

    out = fill_small_gaps(df, T0)

    assert sorted(out["ts"]) == [ts(15), ts(30)]


def test_spikes_use_history_when_available():
    df = frame([("MTR-000001", ts(m), k, 230.0, ts(62)) for m, k in [(0, 0.2), (15, 0.2), (30, 3.0), (45, 0.2)]])

    flagged = flag_anomalies(df, typical_kwh={"MTR-000001": 0.2})

    assert flagged["spike"].tolist() == [False, False, True, False]


def test_tiny_absolute_values_are_not_spikes():
    df = frame([("MTR-000001", ts(m), k, 230.0, ts(62)) for m, k in [(0, 0.01), (15, 0.5)]])

    assert not flag_anomalies(df, {"MTR-000001": 0.01})["spike"].any()


def test_outages_are_flagged_and_values_kept():
    df = frame([("MTR-000001", ts(0), 0.0, 0.0, ts(62))])

    out = flag_anomalies(df)

    assert bool(out.loc[0, "outage"]) and out.loc[0, "kwh"] == 0.0


def test_transform_output_columns_are_stable():
    df = frame([("MTR-000001", ts(m), 0.2, 230.0, ts(62)) for m in (0, 15, 30, 45)])

    assert list(transform(df, T0).columns) == ["meter_id", "ts", "kwh", "voltage", "delivered_at", "estimated", "spike", "outage"]
