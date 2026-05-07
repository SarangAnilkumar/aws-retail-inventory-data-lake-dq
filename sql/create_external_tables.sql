-- Athena external table setup for MVP.
-- IMPORTANT:
-- 1) Replace bucket name `s3://retail-inventory-dq-lake-sarang-2026/` with your real unique bucket.
-- 2) Athena does NOT enforce primary/foreign keys.
--    Keys in this project are logical keys validated by PySpark.

CREATE DATABASE IF NOT EXISTS retail_curated_db;
CREATE DATABASE IF NOT EXISTS retail_dq_db;

-- =========================
-- Curated external tables
-- =========================

-- 1) inventory_daily (partitioned)
-- Partition columns must be defined only in PARTITIONED BY (not duplicated above).
CREATE EXTERNAL TABLE IF NOT EXISTS retail_curated_db.inventory_daily (
  event_date date,
  store_id string,
  product_id string,
  category string,
  region string,
  inventory_on_hand double,
  units_sold double,
  units_ordered double,
  demand_forecast double,
  price double,
  discount double,
  promotion_flag boolean,
  available_stock double,
  stockout_flag boolean,
  ingest_run_id string,
  processed_at timestamp,
  failure_reason string
)
PARTITIONED BY (
  year int,
  month int
)
STORED AS PARQUET
LOCATION 's3://retail-inventory-dq-lake-sarang-2026/curated/inventory_daily/';

-- 2) stores (unpartitioned)
CREATE EXTERNAL TABLE IF NOT EXISTS retail_curated_db.stores (
  store_id string,
  store_name string,
  region string,
  store_type string,
  opened_date string
)
STORED AS PARQUET
LOCATION 's3://retail-inventory-dq-lake-sarang-2026/curated/stores/';

-- 3) products (unpartitioned)
CREATE EXTERNAL TABLE IF NOT EXISTS retail_curated_db.products (
  product_id string,
  product_name string,
  category string,
  unit_price string,
  reorder_level string,
  reorder_quantity string,
  supplier_id string,
  status string
)
STORED AS PARQUET
LOCATION 's3://retail-inventory-dq-lake-sarang-2026/curated/products/';

-- 4) suppliers (unpartitioned)
CREATE EXTERNAL TABLE IF NOT EXISTS retail_curated_db.suppliers (
  supplier_id string,
  supplier_name string,
  lead_time_days string,
  reliability_score string,
  region string
)
STORED AS PARQUET
LOCATION 's3://retail-inventory-dq-lake-sarang-2026/curated/suppliers/';

-- 5) purchase_orders (partitioned)
CREATE EXTERNAL TABLE IF NOT EXISTS retail_curated_db.purchase_orders (
  po_id string,
  product_id string,
  supplier_id string,
  store_id string,
  order_qty double,
  order_date date,
  expected_delivery_date date,
  status string,
  ingest_run_id string,
  processed_at timestamp,
  failure_reason string
)
PARTITIONED BY (
  order_year int,
  order_month int
)
STORED AS PARQUET
LOCATION 's3://retail-inventory-dq-lake-sarang-2026/curated/purchase_orders/';

-- 6) shipments (partitioned)
CREATE EXTERNAL TABLE IF NOT EXISTS retail_curated_db.shipments (
  shipment_id string,
  po_id string,
  supplier_id string,
  store_id string,
  product_id string,
  shipped_date date,
  received_date date,
  quantity_received double,
  delivery_status string,
  delay_days double,
  ingest_run_id string,
  processed_at timestamp,
  failure_reason string
)
PARTITIONED BY (
  received_year int,
  received_month int
)
STORED AS PARQUET
LOCATION 's3://retail-inventory-dq-lake-sarang-2026/curated/shipments/';

-- =========================
-- DQ external tables
-- =========================

-- 7) dq_rule_results
CREATE EXTERNAL TABLE IF NOT EXISTS retail_dq_db.dq_rule_results (
  ingest_run_id string,
  table_name string,
  rule_name string,
  engine string,
  severity string,
  status string,
  failed_row_count bigint,
  checked_at string
)
STORED AS PARQUET
LOCATION 's3://retail-inventory-dq-lake-sarang-2026/dq-results/rule_results/';

-- 8) dq_table_scores
CREATE EXTERNAL TABLE IF NOT EXISTS retail_dq_db.dq_table_scores (
  ingest_run_id string,
  table_name string,
  total_rules bigint,
  passed_rules bigint,
  failed_rules bigint,
  quality_score double,
  checked_at string
)
STORED AS PARQUET
LOCATION 's3://retail-inventory-dq-lake-sarang-2026/dq-results/table_scores/';

-- 9) quarantine_inventory_daily
CREATE EXTERNAL TABLE IF NOT EXISTS retail_dq_db.quarantine_inventory_daily (
  event_date date,
  store_id string,
  product_id string,
  category string,
  region string,
  inventory_on_hand double,
  units_sold double,
  units_ordered double,
  demand_forecast double,
  price double,
  discount double,
  promotion_flag boolean,
  available_stock double,
  stockout_flag boolean,
  ingest_run_id string,
  processed_at timestamp,
  failure_reason string,
  table_name string,
  failed_at string,
  year int,
  month int
)
STORED AS PARQUET
LOCATION 's3://retail-inventory-dq-lake-sarang-2026/quarantine/inventory_daily/';

-- 10) quarantine_purchase_orders
CREATE EXTERNAL TABLE IF NOT EXISTS retail_dq_db.quarantine_purchase_orders (
  po_id string,
  product_id string,
  supplier_id string,
  store_id string,
  order_qty double,
  order_date date,
  expected_delivery_date date,
  status string,
  ingest_run_id string,
  processed_at timestamp,
  failure_reason string,
  table_name string,
  failed_at string,
  order_year int,
  order_month int
)
STORED AS PARQUET
LOCATION 's3://retail-inventory-dq-lake-sarang-2026/quarantine/purchase_orders/';

-- 11) quarantine_shipments
CREATE EXTERNAL TABLE IF NOT EXISTS retail_dq_db.quarantine_shipments (
  shipment_id string,
  po_id string,
  supplier_id string,
  store_id string,
  product_id string,
  shipped_date date,
  received_date date,
  quantity_received double,
  delivery_status string,
  delay_days double,
  ingest_run_id string,
  processed_at timestamp,
  failure_reason string,
  table_name string,
  failed_at string,
  received_year int,
  received_month int
)
STORED AS PARQUET
LOCATION 's3://retail-inventory-dq-lake-sarang-2026/quarantine/shipments/';

-- =========================
-- Partition registration
-- =========================
-- Run these after new partitioned data lands in S3:
MSCK REPAIR TABLE retail_curated_db.inventory_daily;
MSCK REPAIR TABLE retail_curated_db.purchase_orders;
MSCK REPAIR TABLE retail_curated_db.shipments;
