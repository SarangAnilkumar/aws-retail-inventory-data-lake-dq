-- Athena DQ and monitoring views.
-- If CREATE OR REPLACE VIEW is unsupported in your engine, drop then recreate.

-- 1) Latest DQ status per table (rule-level rollup).
CREATE OR REPLACE VIEW retail_dq_db.vw_latest_dq_status AS
WITH latest AS (
  SELECT
    table_name,
    MAX(checked_at) AS latest_checked_at
  FROM retail_dq_db.dq_rule_results
  GROUP BY table_name
)
SELECT
  r.table_name,
  l.latest_checked_at,
  COUNT(*) AS total_rules,
  SUM(CASE WHEN r.status = 'PASS' THEN 1 ELSE 0 END) AS passed_rules,
  SUM(CASE WHEN r.status = 'FAIL' THEN 1 ELSE 0 END) AS failed_rules,
  CAST(SUM(CASE WHEN r.status = 'PASS' THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) AS quality_score
FROM retail_dq_db.dq_rule_results r
JOIN latest l
  ON r.table_name = l.table_name
 AND r.checked_at = l.latest_checked_at
GROUP BY r.table_name, l.latest_checked_at;

-- 2) Failed rules by severity.
CREATE OR REPLACE VIEW retail_dq_db.vw_failed_rules_by_severity AS
SELECT
  r.table_name,
  r.severity,
  COUNT(*) AS failed_rule_count,
  SUM(r.failed_row_count) AS total_failed_rows
FROM retail_dq_db.dq_rule_results r
WHERE r.status = 'FAIL'
GROUP BY r.table_name, r.severity;

-- 3) Quarantine summary by table + failure reason.
CREATE OR REPLACE VIEW retail_dq_db.vw_quarantine_summary AS
WITH q AS (
  SELECT 'inventory_daily' AS table_name, failure_reason FROM retail_dq_db.quarantine_inventory_daily
  UNION ALL
  SELECT 'purchase_orders' AS table_name, failure_reason FROM retail_dq_db.quarantine_purchase_orders
  UNION ALL
  SELECT 'shipments' AS table_name, failure_reason FROM retail_dq_db.quarantine_shipments
)
SELECT
  q.table_name,
  q.failure_reason,
  COUNT(*) AS quarantined_rows
FROM q
GROUP BY q.table_name, q.failure_reason;

-- 4) Quality score trend from table-level scores.
CREATE OR REPLACE VIEW retail_dq_db.vw_quality_score_trend AS
SELECT
  ingest_run_id,
  table_name,
  checked_at,
  quality_score,
  passed_rules,
  failed_rules
FROM retail_dq_db.dq_table_scores;

-- 5) Dataset freshness status (inventory_daily).
CREATE OR REPLACE VIEW retail_dq_db.vw_dataset_freshness_status AS
SELECT
  CAST('inventory_daily' AS varchar) AS table_name,
  MAX(event_date) AS latest_event_date,
  CAST(current_timestamp AS date) AS as_of_date,
  DATE_DIFF('day', MAX(event_date), CAST(current_timestamp AS date)) AS days_since_latest_data,
  CASE
    WHEN DATE_DIFF('day', MAX(event_date), CAST(current_timestamp AS date)) <= 2 THEN 'Fresh'
    ELSE 'Stale'
  END AS freshness_status
FROM retail_curated_db.inventory_daily
GROUP BY CAST('inventory_daily' AS varchar), CAST(current_timestamp AS date);

-- 6) Row-count anomaly by day (70%-130% threshold vs average).
CREATE OR REPLACE VIEW retail_dq_db.vw_row_count_anomaly AS
WITH daily_counts AS (
  SELECT
    event_date,
    COUNT(*) AS row_count
  FROM retail_curated_db.inventory_daily
  GROUP BY event_date
),
avg_counts AS (
  SELECT AVG(row_count) AS avg_daily_row_count
  FROM daily_counts
)
SELECT
  d.event_date,
  d.row_count,
  a.avg_daily_row_count,
  CAST(d.row_count AS DOUBLE) / NULLIF(a.avg_daily_row_count, 0) AS row_count_ratio,
  CASE
    WHEN CAST(d.row_count AS DOUBLE) / NULLIF(a.avg_daily_row_count, 0) < 0.7 THEN 'Possible Anomaly'
    WHEN CAST(d.row_count AS DOUBLE) / NULLIF(a.avg_daily_row_count, 0) > 1.3 THEN 'Possible Anomaly'
    ELSE 'Normal'
  END AS anomaly_status
FROM daily_counts d
CROSS JOIN avg_counts a;

-- 7) Row-level quality summary for dashboard realism.
CREATE OR REPLACE VIEW retail_dq_db.vw_row_level_quality_summary AS
WITH curated AS (
  SELECT
    'inventory_daily' AS table_name,
    COUNT(*) AS curated_rows
  FROM retail_curated_db.inventory_daily
),
quarantine AS (
  SELECT
    'inventory_daily' AS table_name,
    COUNT(*) AS quarantined_rows
  FROM retail_dq_db.quarantine_inventory_daily
)
SELECT
  c.table_name,
  c.curated_rows,
  COALESCE(q.quarantined_rows, 0) AS quarantined_rows,
  (c.curated_rows + COALESCE(q.quarantined_rows, 0)) AS total_input_rows,
  CAST(c.curated_rows AS DOUBLE) / NULLIF((c.curated_rows + COALESCE(q.quarantined_rows, 0)), 0) AS row_pass_rate
FROM curated c
LEFT JOIN quarantine q
  ON c.table_name = q.table_name;
