# Architecture

![Architecture Diagram](architecture.png)

## Overview

This project implements a retail inventory data lake with automated data quality monitoring. The current MVP runs a **local PySpark ETL** pipeline that validates and partitions inventory data, lands curated, quarantine, and DQ outputs in **Amazon S3**, exposes analytics and DQ monitoring through **Athena** external tables and views, and delivers business consumption through a published **Amazon QuickSight** dashboard. Working state is captured in `docs/screenshots/`. AWS Glue (job, crawlers, Data Quality), EventBridge, and SNS remain future enhancements.

The end-to-end diagram is at `docs/architecture.png`.

## Implemented MVP flow

1. Synthetic source CSVs are generated locally with `scripts/generate_synthetic_tables.py`.
2. Local PySpark ETL (`scripts/glue_etl_inventory_dq.py`, executed locally) reads source data, applies typing and validation, runs DQ checks, and splits records into pass / fail.
3. Curated records are written as partitioned Parquet to `s3://retail-inventory-dq-lake/curated/`.
4. Failed records are written to `s3://retail-inventory-dq-lake/quarantine/` with `failure_reason` metadata.
5. DQ run outputs (rule results, table-level scores) are written to `s3://retail-inventory-dq-lake/dq-results/`.
6. Athena external tables are registered over S3 data using `sql/create_external_tables.sql`.
7. Curated partitions are made queryable using `MSCK REPAIR TABLE` (manual step today, not a Glue Crawler).
8. Athena business and DQ views are created from `sql/analytics_views.sql` and `sql/dq_views.sql`.
9. Amazon QuickSight dashboards consume Athena-backed datasets (Retail Inventory Intelligence and Data Quality Control Centre sheets).
10. Implementation is captured as screenshots `01`–`13` in `docs/screenshots/` (see `docs/quicksight_dashboard.md`).

## Architecture components

- **Source data**: Synthetic retail inventory CSVs (`inventory_daily`, `products`, `stores`, `suppliers`, `purchase_orders`, `shipments`).
- **PySpark ETL + DQ**: Local PySpark job that types, validates, partitions, and quarantines records.
- **Amazon S3 data lake**: Zoned bucket layout for raw, curated, quarantine, DQ results, and Athena query staging.
- **Athena SQL analytics layer**: External tables and curated business + DQ views.
- **Amazon QuickSight**: Published dashboards on Athena-backed datasets (inventory intelligence + DQ control centre).
- **Portfolio outputs**: README, architecture docs, diagram, Athena/S3/QuickSight screenshots in `docs/screenshots/`.
- **Future enhancements**: AWS Glue job, Glue Crawlers, AWS Glue Data Quality (DQDL), Amazon EventBridge, Amazon SNS.

## Data quality flow

1. Source data is read into PySpark.
2. Single-column checks (nulls, ranges, allowed values) and cross-column / cross-table checks run inline.
3. Records that pass all rules go to `curated/`.
4. Records that fail any rule go to `quarantine/` with a `failure_reason` and run timestamp.
5. Per-rule and per-table summaries are persisted under `dq-results/` and exposed through Athena DQ views in `sql/dq_views.sql`.
6. DQ status, quarantine summary, and row-level quality views are queryable in Athena (see screenshots `09`–`11`).
7. QuickSight visualizes the same metrics for business and DQ stakeholders (see screenshots `12`–`13`).
8. Reference DQDL rule sets in `dq_rules/` describe the same intent for a future AWS Glue Data Quality run.

## S3 data lake zones

| Zone | Path | Purpose |
| --- | --- | --- |
| Raw | `s3://retail-inventory-dq-lake/raw/` | Landed source CSVs |
| Curated | `s3://retail-inventory-dq-lake/curated/` | Validated, typed, partitioned Parquet |
| Quarantine | `s3://retail-inventory-dq-lake/quarantine/` | Failed records with failure metadata |
| DQ results | `s3://retail-inventory-dq-lake/dq-results/` | Rule and table-level DQ outputs |
| Athena results | `s3://retail-inventory-dq-lake/athena-results/` | Athena query output staging |

## Athena SQL layer

- **External tables** (`sql/create_external_tables.sql`): register curated, quarantine, and DQ result data over S3.
- **Business views** (`sql/analytics_views.sql`): e.g. `vw_inventory_health`, `vw_stockout_rate_by_store`, `vw_stockout_rate_by_category`, `vw_products_below_reorder_threshold`, `vw_supplier_delay_rate`
- **DQ monitoring views** (`sql/dq_views.sql`): e.g. `vw_latest_dq_status`, `vw_quarantine_summary`, `vw_failed_rules_by_severity`, `vw_row_level_quality_summary`

## Portfolio outputs

- `README.md` — project overview, status, portfolio positioning.
- `docs/architecture.md` — this document.
- `docs/architecture.png` — end-to-end architecture diagram.
- `docs/screenshots/01`–`13` — S3, Athena, and QuickSight proofs of implementation.
- `docs/quicksight_dashboard.md` — QuickSight sheet and metric documentation.
- `sql/` and `dq_rules/` — reference SQL views and DQDL rules.

## Amazon QuickSight layer

Published dashboards use Athena-backed datasets over `retail_curated_db` and `retail_dq_db`.

| Sheet | Key visuals |
| --- | --- |
| Retail Inventory Intelligence | Total stockout records, average supplier delay, products requiring replenishment, average stockout rate, stockout rate by store/category, supplier delay rate, replenishment priority table |
| Data Quality Control Centre | Row pass rate, curated rows, quarantined rows, total input rows, failed rows by DQ severity, rule-level DQ status, quarantined records by failure type, DQ rules checked |

Screenshots: `12_quicksight_inventory_dashboard.png`, `13_quicksight_dq_dashboard.png`.

## Future enhancements

- Deploy the existing Glue-compatible PySpark script as an **AWS Glue job**.
- Add **AWS Glue Crawlers** for automatic schema and partition registration in place of `MSCK REPAIR TABLE`.
- Run **AWS Glue Data Quality (DQDL)** rule sets in `dq_rules/` natively on AWS.
- Use **Amazon EventBridge** for scheduled / event-driven pipeline triggers.
- Use **Amazon SNS** for DQ alerting on rule failures or quarantine spikes.

## What is not implemented yet

- AWS Glue job deployment (the ETL currently runs as local PySpark).
- AWS Glue Crawlers (partitions are registered manually in Athena via `MSCK REPAIR TABLE`).
- AWS Glue Data Quality (DQDL) execution (`dq_rules/*.dqdl` are reference rules only; checks today run in PySpark and Athena SQL).
- Amazon EventBridge scheduling.
- Amazon SNS alerting.

## Diagram explanation

| Diagram section | What it does | Current status |
| --- | --- | --- |
| Source Data | Synthetic retail CSVs (inventory, products, stores, suppliers, purchase orders, shipments) | Implemented (local generator) |
| PySpark ETL + DQ | Validates, types, splits records into pass / fail, writes curated, quarantine, and DQ outputs | Implemented (runs locally) |
| Amazon S3 Data Lake | Stores raw, curated, quarantine, DQ results, and Athena query staging | Implemented |
| Athena SQL Analytics Layer | External tables plus business and DQ views over S3 | Implemented |
| Amazon QuickSight | Athena-backed dashboards for inventory and DQ monitoring | Implemented |
| Portfolio Outputs | README, architecture doc, diagram, screenshots, reference SQL and DQDL | Implemented |
| Future Enhancements | AWS Glue job, Glue Crawlers, Glue Data Quality, EventBridge, SNS | Not implemented (planned) |
