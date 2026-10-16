# ADR-0003: A data-quality gate fails the run

**Status:** Accepted

## Context
A pipeline that "succeeds" while loading half the expected data is worse than one that fails: nobody looks.

## Decision
After loading, each run checks: reject rate below a threshold, coverage (readings received vs expected for active
meters in the interval) above a threshold, and freshness of the newest reading. Any breach **fails the task**, so
Airflow's retry and alerting machinery takes over. Every run's metrics are stored in `pipeline_runs`.

## Consequences
Quality problems surface as failed runs with a reason, the right place for on-call attention. Thresholds are
configuration, tuned from the observed distribution of real batches.
