from datetime import datetime

import pandas as pd


def utc(value: datetime | pd.Timestamp | str) -> pd.Timestamp:
    """A UTC Timestamp from naive (assumed UTC) or aware input. pandas 3 refuses `tz=` on aware values."""
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
