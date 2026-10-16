import os
from datetime import datetime, timezone

import psycopg
import pytest

INTERVAL_START = datetime(2026, 10, 15, 18, 0, tzinfo=timezone.utc)
INTERVAL_END = datetime(2026, 10, 15, 19, 0, tzinfo=timezone.utc)


@pytest.fixture
def db():
    url = os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL not set (integration tests need TimescaleDB)")
    from meterflow.load import apply_schema

    with psycopg.connect(url) as conn:
        apply_schema(conn)
        conn.execute("TRUNCATE readings, rejects")
        conn.commit()
        yield conn
