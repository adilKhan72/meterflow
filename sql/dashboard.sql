-- Example dashboard queries. All read the continuous aggregates, never raw readings, so they stay fast as data grows.

-- 1. Daily consumption per meter for the last 30 days (Grafana time series: one line per meter).
SELECT day AS time, meter_id, kwh
FROM readings_daily
WHERE day >= now() - INTERVAL '30 days'
ORDER BY day, meter_id;

-- 2. Top 10 consumers this week, with how much of their data was estimated.
SELECT meter_id,
       round(sum(kwh)::numeric, 2)                                         AS kwh,
       round(100.0 * sum(estimated_readings) / nullif(sum(readings), 0), 1) AS estimated_pct
FROM readings_daily
WHERE day >= date_trunc('week', now())
GROUP BY meter_id
ORDER BY kwh DESC
LIMIT 10;

-- 3. Fleet load curve: average consumption by hour of day over the last 7 days.
SELECT extract(hour FROM bucket) AS hour_of_day, round(avg(kwh)::numeric, 4) AS avg_kwh_per_meter
FROM readings_hourly
WHERE bucket >= now() - INTERVAL '7 days'
GROUP BY 1
ORDER BY 1;

-- 4. Meters with spikes or outages in the last 24 hours (for an operations panel).
SELECT meter_id, bucket, kwh, any_spike, any_outage
FROM readings_hourly
WHERE bucket >= now() - INTERVAL '24 hours' AND (any_spike OR any_outage)
ORDER BY bucket DESC;

-- 5. Pipeline health: quality-gate results per run, newest first.
SELECT batch_id, passed, reject_rate, coverage, round(freshness_lag_s / 60) AS lag_min, failures, recorded_at
FROM pipeline_runs
ORDER BY recorded_at DESC
LIMIT 48;
