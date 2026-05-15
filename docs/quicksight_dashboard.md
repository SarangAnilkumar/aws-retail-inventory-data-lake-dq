# Amazon QuickSight Dashboard

## Overview

The portfolio dashboard is **published** in Amazon QuickSight and completes the AWS-native BI phase of the MVP. It reads from **Athena-backed datasets** over:

- `retail_curated_db` — curated inventory and dimension tables plus analytics views
- `retail_dq_db` — DQ results, quarantine tables, and DQ monitoring views

Athena external tables and views are defined in `sql/create_external_tables.sql`, `sql/analytics_views.sql`, and `sql/dq_views.sql`. QuickSight consumes the same curated and DQ layers already proven in Athena; it does not replace the data lake.

**Honest scope:** local PySpark ETL is implemented. Amazon S3, Athena, and QuickSight are implemented. AWS Glue job deployment, Glue Crawlers, AWS Glue Data Quality / DQDL, EventBridge, and SNS remain future improvements.

## Dashboard sheets

### 1. Retail Inventory Intelligence

Business-facing sheet for inventory and supply-chain KPIs. Visuals include:

| Metric / visual | Purpose |
| --- | --- |
| Total stockout records | Aggregate stockout exposure across the curated fact |
| Average supplier delay | Delivery performance summary |
| Products requiring replenishment | Items at or below reorder threshold |
| Average stockout rate | Portfolio-wide stockout KPI |
| Stockout rate by store | Store-level stockout comparison |
| Stockout rate by category | Category-level stockout comparison |
| Supplier delay rate | Supplier delivery reliability |
| Replenishment priority table | Actionable list for restocking decisions |

Typical Athena sources: `vw_inventory_health`, `vw_stockout_rate_by_store`, `vw_stockout_rate_by_category`, `vw_products_below_reorder_threshold`, `vw_supplier_delay_rate`, and related curated tables.

### 2. Data Quality Control Centre

Operations-facing sheet for data quality monitoring. Visuals include:

| Metric / visual | Purpose |
| --- | --- |
| Row pass rate | Share of input rows that passed validation |
| Curated rows | Count of rows in the curated layer |
| Quarantined rows | Count of rows sent to quarantine |
| Total input rows | Curated + quarantined input volume |
| Failed rows by DQ severity | Failures grouped by rule severity |
| Rule-level DQ status | Latest pass/fail status per rule |
| Quarantined records by failure type | Quarantine volume by `failure_reason` |
| DQ rules checked | Total rules evaluated in the latest run |

Typical Athena sources: `vw_row_level_quality_summary`, `vw_quarantine_summary`, `vw_failed_rules_by_severity`, `vw_latest_dq_status`, and underlying DQ/quarantine tables.

## Proof screenshots

| File | Sheet |
| --- | --- |
| `docs/screenshots/12_quicksight_inventory_dashboard.png` | Retail Inventory Intelligence |
| `docs/screenshots/13_quicksight_dq_dashboard.png` | Data Quality Control Centre |

Redact AWS account IDs and sensitive identifiers before sharing screenshots publicly.

## Implementation notes

- **Data source:** Amazon Athena (`AwsDataCatalog`).
- **Datasets:** Athena tables and views (physical tables or custom SQL), not local file uploads.
- **ETL:** Runs as local PySpark in the MVP; QuickSight reflects Parquet and DQ outputs already in S3 and registered in Athena.

## Future improvements (not in current MVP)

- AWS Glue job deployment (orchestrated ETL instead of local PySpark)
- Glue Crawlers (automatic partition and schema registration)
- AWS Glue Data Quality / DQDL (native rule execution)
- Amazon EventBridge (scheduled pipeline triggers)
- Amazon SNS (DQ failure and quarantine alerting)
