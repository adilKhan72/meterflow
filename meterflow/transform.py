"""Cleaning that's safe to repeat: dedupe, gap handling and flags. Never invents data without saying so."""

import pandas as pd

from meterflow.generator import READING_MINUTES
from meterflow.timeutil import utc

SPIKE_FACTOR = 8.0  # × the meter's own typical reading
SPIKE_MIN_KWH = 1.0  # ignore "spikes" on tiny absolute values


def dedupe(df: pd.DataFrame) -> pd.DataFrame:
    """Keep the latest delivery per (meter, ts): re-deliveries are corrections."""
    return (
        df.sort_values("delivered_at", kind="stable")
        .drop_duplicates(subset=["meter_id", "ts"], keep="last")
        .sort_values(["meter_id", "ts"], kind="stable")
        .reset_index(drop=True)
    )


def fill_small_gaps(df: pd.DataFrame, interval_start: pd.Timestamp) -> pd.DataFrame:
    """Reindex each meter to the expected 15-minute grid and interpolate gaps of ONE reading between real neighbours.

    Interpolated rows get `estimated=True`. Missing readings at the edges of the interval stay missing:
    there's no neighbour on one side, and extrapolating would be invention.
    """
    grid = pd.date_range(utc(interval_start), periods=60 // READING_MINUTES, freq=f"{READING_MINUTES}min")
    out = []
    for meter, group in df.groupby("meter_id", sort=True):
        g = group.set_index("ts").reindex(grid)
        g["meter_id"] = meter
        missing = g["kwh"].isna()
        g["kwh"] = g["kwh"].interpolate(limit=1, limit_area="inside")
        g["voltage"] = g["voltage"].interpolate(limit=1, limit_area="inside")
        g["estimated"] = missing & g["kwh"].notna()
        g["delivered_at"] = g["delivered_at"].ffill().bfill()
        out.append(g.dropna(subset=["kwh"]).rename_axis("ts").reset_index())
    if not out:
        return df.assign(estimated=pd.Series(dtype=bool))
    return pd.concat(out, ignore_index=True)


def flag_anomalies(df: pd.DataFrame, typical_kwh: dict[str, float] | None = None) -> pd.DataFrame:
    """`spike`: far above the meter's typical reading (from history when available, else this batch's median).
    `outage`: zero voltage. Flags inform consumers; values are never altered."""
    df = df.copy()
    batch_median = df.groupby("meter_id")["kwh"].transform("median")
    typical = df["meter_id"].map(typical_kwh or {}).fillna(batch_median)
    df["spike"] = (df["kwh"] > typical * SPIKE_FACTOR) & (df["kwh"] > SPIKE_MIN_KWH)
    df["outage"] = df["voltage"] == 0
    return df


def transform(valid: pd.DataFrame, interval_start: pd.Timestamp, typical_kwh: dict[str, float] | None = None) -> pd.DataFrame:
    cleaned = flag_anomalies(fill_small_gaps(dedupe(valid), interval_start), typical_kwh)
    return cleaned[["meter_id", "ts", "kwh", "voltage", "delivered_at", "estimated", "spike", "outage"]]
