# Screenshots / Proof of Implementation

This folder contains concise proof that the MVP pipeline is implemented and queryable.

## Screenshot Index

- `docs/screenshots/architecture_diagram.png` — Compact README architecture diagram (`#architecture-export` only: header, pipeline, metrics, future, legend — no bottom cards).
- `docs/screenshots/architecture_diagram_full.png` — Full HTML page export (`#report-container`: diagram plus explanatory cards and footer).
- `docs/architecture_diagram.html` — Interactive Cocoon-style diagram with browser export toolbar (PNG/PDF).
- Re-export both PNGs: `python3 scripts/export_architecture_diagram.py`
- `docs/architecture.png` — Legacy static diagram (superseded by `architecture_diagram.png` for README).
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
- `12_quicksight_inventory_dashboard.png` - QuickSight Retail Inventory Intelligence sheet: stockout KPIs, supplier delay, replenishment priorities (Athena-backed).
- `13_quicksight_dq_dashboard.png` - QuickSight Data Quality Control Centre sheet: row pass rate, quarantine summary, DQ severity, rule-level status (Athena-backed).
- `14_glue_job_succeeded.png` - AWS Glue Spark ETL job run status shows **Succeeded** in the Glue console.
- `15_glue_job_arguments.png` - Glue job parameters (underscore keys) and S3 paths for input/output zones.
- `16_glue_cloudwatch_logs.png` - CloudWatch logs confirm Glue mode, ETL completion, and row counts.
- `17_s3_glue_curated_outputs.png` - Partitioned curated Parquet under `curated-glue/` after Glue execution.
- `18_s3_glue_quarantine_outputs.png` - Quarantine Parquet outputs under `quarantine-glue/`.
- `19_s3_glue_dq_results.png` - DQ `rule_results` and `table_scores` under `dq-results-glue/`.

## Notes

- **Implemented:** local PySpark ETL (dev/test); AWS Glue Spark ETL (cloud); Amazon S3 data lake; Athena external tables/views; quarantine and DQ reporting; QuickSight dashboards (`12`, `13`); Glue deployment proof (`14`–`19`).
- **Future improvements:** Glue Crawlers, AWS Glue Data Quality / DQDL, EventBridge scheduling, SNS alerts.
- Same ETL script (`scripts/glue_etl_inventory_dq.py`) runs locally and on Glue; see `docs/glue_job_runbook.md`.

## Privacy Requirement

Redact AWS account IDs and sensitive identifiers before committing screenshots.
