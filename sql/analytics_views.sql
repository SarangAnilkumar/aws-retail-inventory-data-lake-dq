-- Athena analytics views for curated data.
-- If your Athena engine/version does not support CREATE OR REPLACE VIEW,
-- run DROP VIEW IF EXISTS <view_name>; first, then CREATE VIEW.

-- 1) Latest inventory status and value by item/store/day.
CREATE OR REPLACE VIEW retail_curated_db.vw_inventory_health AS
SELECT
  i.event_date,
  i.store_id,
  i.product_id,
  i.category,
  i.region,
  i.inventory_on_hand,
  i.units_sold,
  i.available_stock,
  i.stockout_flag,
  i.price,
  (i.inventory_on_hand * i.price) AS estimated_inventory_value
FROM retail_curated_db.inventory_daily i;

-- 2) Stockout rate by store.
CREATE OR REPLACE VIEW retail_curated_db.vw_stockout_rate_by_store AS
SELECT
  i.store_id,
  COUNT(*) AS total_records,
  SUM(CASE WHEN i.stockout_flag THEN 1 ELSE 0 END) AS stockout_records,
  CAST(SUM(CASE WHEN i.stockout_flag THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) AS stockout_rate
FROM retail_curated_db.inventory_daily i
GROUP BY i.store_id;

-- 3) Stockout rate by category.
CREATE OR REPLACE VIEW retail_curated_db.vw_stockout_rate_by_category AS
SELECT
  i.category,
  COUNT(*) AS total_records,
  SUM(CASE WHEN i.stockout_flag THEN 1 ELSE 0 END) AS stockout_records,
  CAST(SUM(CASE WHEN i.stockout_flag THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) AS stockout_rate
FROM retail_curated_db.inventory_daily i
GROUP BY i.category;

-- 4) Products at/below reorder threshold.
CREATE OR REPLACE VIEW retail_curated_db.vw_products_below_reorder_threshold AS
SELECT
  i.event_date,
  i.store_id,
  i.product_id,
  i.category,
  i.inventory_on_hand,
  CAST(p.reorder_level AS DOUBLE) AS reorder_level,
  CAST(p.reorder_quantity AS DOUBLE) AS reorder_quantity,
  (CAST(p.reorder_level AS DOUBLE) - i.inventory_on_hand) AS units_below_threshold
FROM retail_curated_db.inventory_daily i
JOIN retail_curated_db.products p
  ON i.product_id = p.product_id
WHERE i.inventory_on_hand <= CAST(p.reorder_level AS DOUBLE);

-- 5) Sales velocity by product.
CREATE OR REPLACE VIEW retail_curated_db.vw_sales_velocity AS
SELECT
  i.product_id,
  i.category,
  AVG(i.units_sold) AS avg_units_sold_per_day,
  SUM(i.units_sold) AS total_units_sold,
  COUNT(DISTINCT i.event_date) AS active_days
FROM retail_curated_db.inventory_daily i
GROUP BY i.product_id, i.category;

-- 6) Inventory coverage days.
CREATE OR REPLACE VIEW retail_curated_db.vw_inventory_coverage_days AS
WITH item_daily AS (
  SELECT
    i.store_id,
    i.product_id,
    MAX(i.inventory_on_hand) AS inventory_on_hand,
    AVG(i.units_sold) AS avg_units_sold_per_day
  FROM retail_curated_db.inventory_daily i
  GROUP BY i.store_id, i.product_id
)
SELECT
  d.store_id,
  d.product_id,
  d.inventory_on_hand,
  d.avg_units_sold_per_day,
  d.inventory_on_hand / NULLIF(d.avg_units_sold_per_day, 0) AS coverage_days
FROM item_daily d;

-- 7) Revenue at risk proxy by stockouts.
CREATE OR REPLACE VIEW retail_curated_db.vw_revenue_at_risk AS
WITH base AS (
  SELECT
    i.store_id,
    i.product_id,
    i.category,
    SUM(CASE WHEN i.stockout_flag THEN 1 ELSE 0 END) AS stockout_days,
    AVG(i.units_sold) AS avg_units_sold_per_day,
    AVG(i.price) AS avg_price
  FROM retail_curated_db.inventory_daily i
  GROUP BY i.store_id, i.product_id, i.category
)
SELECT
  b.store_id,
  b.product_id,
  b.category,
  b.stockout_days,
  b.avg_units_sold_per_day,
  b.avg_price,
  (b.stockout_days * b.avg_units_sold_per_day * b.avg_price) AS estimated_revenue_at_risk
FROM base b;

-- 8) Supplier delay rate from shipment outcomes.
CREATE OR REPLACE VIEW retail_curated_db.vw_supplier_delay_rate AS
SELECT
  sh.supplier_id,
  sp.supplier_name,
  COUNT(*) AS total_shipments,
  SUM(CASE WHEN sh.delay_days > 0 THEN 1 ELSE 0 END) AS delayed_shipments,
  AVG(sh.delay_days) AS avg_delay_days,
  CAST(SUM(CASE WHEN sh.delay_days > 0 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) AS delay_rate
FROM retail_curated_db.shipments sh
LEFT JOIN retail_curated_db.suppliers sp
  ON sh.supplier_id = sp.supplier_id
GROUP BY sh.supplier_id, sp.supplier_name;
