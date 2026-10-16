import pytest

from meterflow.quality import Thresholds, evaluate

T = Thresholds(max_reject_rate=0.05, min_coverage=0.95, max_freshness_lag_s=1800)


def test_a_normal_batch_passes():
    r = evaluate(raw_rows=200, rejected_rows=6, active_meters=50, stored_rows=196, freshness_lag_s=900, t=T)

    assert r.passed and r.reject_rate == 0.03 and r.coverage == 0.98


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({"rejected_rows": 30}, "reject rate 15.00% > 5.00%"),
        ({"stored_rows": 150}, "coverage 75.00% < 95.00% (150/200 readings)"),
        ({"freshness_lag_s": 7200}, "newest reading is 120 min before interval end"),
        ({"freshness_lag_s": None, "stored_rows": 0}, "no readings stored for this interval"),
        ({"raw_rows": 0, "rejected_rows": 0}, "empty batch"),
    ],
)
def test_each_breach_is_reported(kwargs, expected):
    args = {"raw_rows": 200, "rejected_rows": 6, "active_meters": 50, "stored_rows": 196, "freshness_lag_s": 900} | kwargs

    r = evaluate(**args, t=T)

    assert not r.passed
    assert any(expected in f for f in r.failures)


def test_thresholds_come_from_the_environment(monkeypatch):
    monkeypatch.setenv("METERFLOW_MAX_REJECT_RATE", "0.2")
    monkeypatch.setenv("METERFLOW_MIN_COVERAGE", "0.5")

    t = Thresholds.from_env()

    assert (t.max_reject_rate, t.min_coverage) == (0.2, 0.5)
