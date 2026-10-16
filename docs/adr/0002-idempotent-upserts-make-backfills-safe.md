# ADR-0002: Idempotent upserts make backfills safe

**Status:** Accepted

## Context
Pipelines get re-run: retries after failure, late-arriving batches, backfills after a bug fix. Append-only loads
double-count on every re-run.

## Decision
- `readings` has a unique key on `(meter_id, ts)`. Loads go `COPY → temporary staging table → INSERT … ON CONFLICT
  (meter_id, ts) DO UPDATE`, and the update only wins when the incoming row was delivered later (`delivered_at`).
- Each Airflow run processes exactly one **data interval** (its batch files), never "everything new since last time".

## Consequences
Re-running any interval, in any order, converges to the same table: retries and backfills are routine operations.
Continuous aggregates are refreshed for the affected window after each load, so totals follow corrections.
