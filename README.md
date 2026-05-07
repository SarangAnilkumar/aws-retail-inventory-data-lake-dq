# AWS Retail Inventory Data Lake with Automated Data Quality Monitoring

Implementation-ready MVP portfolio project for early-career Data Engineering and Data Quality roles.

## Current implementation status

| Component | Status |
| --- | --- |
| Local synthetic data generation | Complete |
| Local PySpark ETL | Complete |
| S3 data lake upload | Complete |
| Athena external tables | Complete |
| Athena analytics views | Complete |
| Athena DQ views | Complete |
| QuickSight dashboard | Not implemented yet |
| AWS Glue job deployment | Future improvement |
| Glue Data Quality/DQDL | Future improvement |

## Project Summary

This project delivers a portfolio-ready retail inventory data platform: local PySpark ETL writes curated and quarantine data, S3 stores lake zones, and Athena serves analytics and DQ monitoring views. The current implementation is practical and auditable; Glue job orchestration, Glue DQ execution, and QuickSight dashboarding are planned improvements.

## Business Problem

Retail teams need trustworthy inventory and replenishment insights. Poor data quality causes bad stock decisions, missed sales, and weak reporting confidence.

## Data Engineering Problem

Design a reliable, rerunnable raw-to-curated pipeline with explicit data quality controls, partitioned storage, and low query cost.

## Architecture (High Level)

- Raw CSVs land in `s3://retail-inventory-dq-lake/raw/`
- Local PySpark ETL transforms + validates records
- PySpark handles cross-column and cross-table checks
- Passed records go to curated Parquet (partitioned)
- Failed records go to quarantine with failure reason
- DQ run summaries are written to DQ monitoring tables
- Athena external tables and views power reporting

Detailed architecture notes: `docs/architecture.md`

## Dataset Strategy

- Source-of-truth fact table: `inventory_daily` (Retail Store Inventory Forecasting dataset)
- Supporting tables (stores/products/suppliers/purchase_orders/shipments) generated from canonical keys in `inventory_daily`
- Synthetic data is explicitly documented as synthetic

## S3 Folder Structure

See `docs/architecture.md` and `docs/data_model.md` for final mapping.

## Data Model Summary

See `docs/data_model.md`.

## Data Quality Strategy

Current implementation uses PySpark + Athena SQL checks and quarantine monitoring.  
See `docs/dq_rules.md` for the full rule split and Glue DQ roadmap.

## Local Run (Planned)

1. Generate supporting synthetic tables.
2. Run ETL locally with PySpark-compatible mode.
3. Validate outputs in local folders before AWS upload.

Detailed commands will be added in later phases.

## AWS Run (Planned)

1. Upload raw files to S3.
2. Run raw crawler.
3. Run Glue ETL job with parameters.
4. Register partitions (`MSCK REPAIR TABLE` or crawler).
5. Execute Athena view SQL.

## Athena Views

- Business views: `sql/analytics_views.sql`
- DQ views: `sql/dq_views.sql`

## Screenshots / Proof of Implementation

The screenshots in `docs/screenshots/` show concrete proof that the MVP is implemented and queryable end-to-end.

- `01_s3_bucket_structure.png` - S3 zone structure is in place.
- `02_s3_curated_partitions.png` - Curated fact data is partitioned.
- `03_athena_databases_tables.png` - Athena external tables are registered.
- `04_inventory_count_query.png` - Curated data is queryable in Athena.
- `05_partition_registration.png` - Partition discovery (`MSCK REPAIR TABLE`) works.
- `06_inventory_health_view.png` - Inventory analytics view is working.
- `07_stockout_rate_view.png` - Stockout KPI view is working.
- `08_supplier_delay_view.png` - Supplier delay analytics join is working.
- `09_dq_latest_status_view.png` - DQ rule-status monitoring is queryable.
- `10_dq_quarantine_summary_view.png` - Quarantine failure summary is queryable.
- `11_dq_row_level_quality_summary.png` - Row-level DQ pass-rate metrics are queryable.

Implementation status reflected by these screenshots:
- ETL is currently run as a local PySpark pipeline.
- Data lake storage and query surfaces are on AWS (S3 + Athena).
- DQ quarantine handling is implemented and queryable.
- The ETL script is Glue-compatible by design, but AWS Glue job deployment, Glue Crawler setup, Glue DQ execution, and QuickSight dashboarding are not implemented yet.

## Cost Control Notes

See `docs/cost_notes.md`.

## Cleanup Checklist

To be finalized in Phase 6 (delete jobs, crawlers, test data, BI resources).

## Resume Bullets

To be finalized in Phase 6.

## Interview Pitch

To be finalized in Phase 6.

## Limitations and Future Improvements

MVP first; streaming, IaC, and ML are out of scope for initial build.

## Definition of Done

- [ ] Raw CSV data is stored in S3.
- [ ] Raw data is cataloged by Glue Crawler.
- [ ] ETL job runs successfully from raw to curated.
- [ ] Curated fact data is written as partitioned Parquet.
- [ ] `inventory_daily` includes `event_date` in the schema.
- [ ] Reruns use partition-scoped overwrite.
- [ ] At least 10 DQ rules are implemented.
- [ ] Failed records are written to quarantine with failure reasons.
- [ ] DQ results are queryable in Athena.
- [ ] At least 5 business Athena views exist.
- [ ] At least 3 DQ Athena views exist.
- [ ] Dashboard has Inventory Intelligence and DQ Control Centre pages.
- [ ] README includes architecture, run steps, data model, DQ rules, costs and teardown.
- [ ] No secrets, credentials or unredacted AWS account IDs are committed.
- [ ] Screenshots redact account IDs and sensitive identifiers.
