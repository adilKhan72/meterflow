# ADR-0001: Validate before loading, and keep the rejects

**Status:** Accepted

## Context
Meter data arrives with faults: duplicates, negative or impossible readings, malformed meter ids, timestamps from the
future, late deliveries. Loading everything and cleaning later lets bad data reach dashboards and billing. Silently dropping
bad rows hides upstream problems.

## Decision
Every batch passes a **pandera schema** (types, ranges, id format) and rule checks *before* loading. Invalid rows go
to a `rejects` table with the reason and the source batch, and the reject rate is a data-quality metric with a
threshold (ADR-0003).

## Consequences
Bad data never reaches the hypertable or the aggregates, and every dropped row can be explained. A broken upstream
shows up as a spike in rejects, not as a subtle drift in totals.
