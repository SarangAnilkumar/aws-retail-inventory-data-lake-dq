# Amazon QuickSight Dashboard

## Overview

The portfolio dashboard is published in **Amazon QuickSight** and reads from **Athena-backed datasets** over the same databases used for SQL proof in this repo:

- `retail_curated_db` — curated inventory and dimension tables plus analytics views
- `retail_dq_db` — DQ results, quarantine tables, and DQ monitoring views

Athena external tables and views are defined in `sql/create_external_tables.sql`, `sql/analytics_views.sql`, and `sql/dq_views.sql`. QuickSight does not replace the data lake; it consumes the curated and DQ layers already validated in Athena.

## Dashboard sheets

### 1. Retail Inventory Intelligence

Business-facing sheet for inventory and supply-chain KPIs, including:

- Stockout risk (store and category perspectives)
- Supplier delay and delivery performance
- Replenishment priorities (products at or below reorder thresholds)
- Supporting inventory health and sales-velocity style metrics from curated Athena views

Typical Athena sources for this sheet include views such as `vw_inventory_health`, `vw_stockout_rate_by_store`, `vw_stockout_rate_by_category`, `vw_products_below_reorder_threshold`, `vw_supplier_delay_rate`, and related curated tables.

### 2. Data Quality Control Centre

Operations-facing sheet for data quality monitoring, including:

- Row pass rate and input vs quarantined row counts
- Quarantined records by table and failure reason
- Failed DQ rules by severity
- Rule-level DQ status and quality scores from the latest DQ run

Typical Athena sources for this sheet include `vw_row_level_quality_summary`, `vw_quarantine_summary`, `vw_failed_rules_by_severity`, `vw_latest_dq_status`, and underlying DQ/quarantine tables.

## Proof screenshots

| File | What it shows |
| --- | --- |
| `docs/screenshots/12_quicksight_inventory_dashboard.png` | Retail Inventory Intelligence sheet |
| `docs/screenshots/13_quicksight_dq_dashboard.png` | Data Quality Control Centre sheet |

Redact AWS account IDs and sensitive identifiers before sharing screenshots publicly.

## Implementation notes

- **Data source**: Amazon Athena (`AwsDataCatalog`), region `ap-southeast-2` in the deployed account.
- **Datasets**: Built from Athena tables/views (custom SQL or physical tables), not from local files.
- **ETL**: Still runs as local PySpark in the MVP; QuickSight reflects outputs already landed in S3 and registered in Athena.

## Future improvements (not in current MVP)

- AWS Glue job deployment (orchestrated ETL instead of local PySpark)
- Glue Crawlers (automatic partition and schema registration)
- AWS Glue Data Quality / DQDL (native rule execution)
- Amazon EventBridge (scheduled pipeline triggers)
- Amazon SNS (DQ failure and quarantine alerting)
