"""Schema + rule validation (ADR-0001). Invalid rows are returned with a reason, never silently dropped."""

from datetime import datetime

import pandas as pd
import pandera.pandas as pa

from meterflow.timeutil import utc

MAX_KWH_PER_READING = 25.0  # a 15-minute reading above this is physically implausible for a household meter

SCHEMA = pa.DataFrameSchema(
    {
        "meter_id": pa.Column(str, pa.Check.str_matches(r"^MTR-\d{6}$"), nullable=False),
        "ts": pa.Column("datetime64[ns, UTC]", nullable=False, coerce=True),
        "kwh": pa.Column(float, [pa.Check.ge(0), pa.Check.le(MAX_KWH_PER_READING)], nullable=False, coerce=True),
        "voltage": pa.Column(float, pa.Check.in_range(0, 300), nullable=False, coerce=True),
        "delivered_at": pa.Column("datetime64[ns, UTC]", nullable=False, coerce=True),
    },
    strict="filter",  # unexpected columns are dropped, not an error: upstream may add fields
)


def validate(df: pd.DataFrame, interval_start: datetime, interval_end: datetime) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (valid, rejects). rejects has the original columns plus `reason` (all failures for that row, joined)."""
    df = df.reset_index(drop=True)
    reasons: dict[int, list[str]] = {}

    try:
        typed = SCHEMA.validate(df, lazy=True)
    except pa.errors.SchemaErrors as e:
        cases = e.failure_cases
        for _, case in cases.iterrows():
            if pd.notna(case["index"]):
                reasons.setdefault(int(case["index"]), []).append(f"{case['column']}: {case['check']}")
        # Re-validate the remaining rows so the interval rule below runs on correctly typed data.
        typed = SCHEMA.validate(df.drop(index=list(reasons)), lazy=False) if len(reasons) < len(df) else df.iloc[0:0]

    # Rule: a reading must belong to this batch's data interval (catches clock drift and misrouted files).
    if len(typed):
        out_of_interval = typed[(typed["ts"] < utc(interval_start)) | (typed["ts"] >= utc(interval_end))]
        for idx in out_of_interval.index:
            reasons.setdefault(int(idx), []).append("ts: outside batch interval")

    bad = sorted(reasons)
    rejects = df.loc[bad].copy()
    rejects["reason"] = ["; ".join(reasons[i]) for i in bad]
    valid = typed.drop(index=[i for i in bad if i in typed.index])
    return valid.reset_index(drop=True), rejects.reset_index(drop=True)
