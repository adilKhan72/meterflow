# ADR-0004: Let TimescaleDB do the time-series work

**Status:** Accepted

## Context
Dashboards need hourly and daily energy per meter over long ranges. Recomputing those from raw 15-minute readings
on every query gets slower as the data grows. Raw data also grows without bound.

## Decision
`readings` is a **hypertable** (1-day chunks). Hourly and daily **continuous aggregates** pre-compute sums with
refresh policies, and the pipeline refreshes the touched window after each load. **Compression** after 7 days and
**retention** of 400 days for raw data. The aggregates are kept longer for year-on-year comparison.

## Consequences
Dashboard queries read small pre-aggregated tables. Storage stays bounded. The trade-off is that this is PostgreSQL
plus an extension, deliberately not a separate time-series database, so one SQL toolchain covers everything.
