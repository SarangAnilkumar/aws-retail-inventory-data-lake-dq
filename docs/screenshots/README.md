# Screenshots / Proof of Implementation

This folder contains concise proof that the MVP pipeline is implemented and queryable.

## Screenshot Index

- `docs/architecture.png` (referenced as `00_architecture_diagram.png`) - Shows the end-to-end project architecture and implemented vs future components.
- `01_s3_bucket_structure.png` - S3 zone layout is in place.
- `02_s3_curated_partitions.png` - Partitioned curated fact storage is in place.
- `03_athena_databases_tables.png` - Athena external tables are registered.
- `04_inventory_count_query.png` - Curated data is queryable.
- `05_partition_registration.png` - Partition registration is working.
- `06_inventory_health_view.png` - Inventory health view is working.
- `07_stockout_rate_view.png` - Stockout KPI view is working.
- `08_supplier_delay_view.png` - Supplier delay view is working.
- `09_dq_latest_status_view.png` - DQ status view is working.
- `10_dq_quarantine_summary_view.png` - Quarantine summary view is working.
- `11_dq_row_level_quality_summary.png` - Row-level DQ summary is working.
- `12_quicksight_inventory_dashboard.png` - QuickSight Retail Inventory Intelligence sheet (Athena-backed).
- `13_quicksight_dq_dashboard.png` - QuickSight Data Quality Control Centre sheet (Athena-backed).

## Notes

- Pipeline execution for MVP is currently local PySpark.
- AWS implementation proof shown here is S3 storage, Athena tables/views, and published QuickSight dashboards.
- Quarantine handling and DQ reporting are implemented and queryable.
- Glue-compatible ETL script exists, but AWS Glue job deployment is still a future improvement.
- Glue Crawler, Glue Data Quality (DQDL execution), EventBridge scheduling, and SNS alerts are future improvements.

## Privacy Requirement

Redact AWS account IDs and sensitive identifiers before committing screenshots.
