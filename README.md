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
| Quarantine handling | Complete |
| Architecture diagram | Complete |
| QuickSight dashboard | Complete |
| AWS Glue Spark ETL job | Complete |
| Glue job execution | Complete |
| CloudWatch Glue logs | Complete |
| S3 Glue output zones | Complete |
| Glue Crawlers | Future improvement |
| AWS Glue Data Quality / DQDL | Future improvement |
| EventBridge scheduling | Future improvement |
| SNS alerts | Future improvement |

## Project Summary

This project delivers a portfolio-ready retail inventory data platform with a clear execution split:

**Local development/testing → AWS Glue Spark ETL → Amazon S3 Data Lake → Athena external tables/views → QuickSight dashboard**

Local PySpark is used for fast iteration and validation. The same ETL script (`scripts/glue_etl_inventory_dq.py`) runs in AWS Glue for cloud execution, writing to dedicated `-glue` S3 prefixes. Athena and QuickSight consume the lake layers for SQL analytics and BI. Glue Crawlers, native Glue Data Quality (DQDL), EventBridge, and SNS remain planned improvements.

## Project flow

```text
Local development/testing
        ↓
AWS Glue Spark ETL (cloud execution)
        ↓
Amazon S3 Data Lake (raw, curated-glue, quarantine-glue, dq-results-glue)
        ↓
Athena external tables / views
        ↓
Amazon QuickSight dashboard
```

## Architecture Diagram

![Architecture Diagram](docs/architecture.png)

This architecture shows the implemented MVP flow: local PySpark for development, AWS Glue Spark ETL for cloud execution, curated/quarantine/DQ outputs in Amazon S3, Athena external tables/views for analytics and data quality reporting, and Amazon QuickSight dashboards for business consumption. Glue Crawlers, Glue Data Quality (DQDL), EventBridge, and SNS remain future enhancements.

## Business Problem

Retail teams need trustworthy inventory and replenishment insights. Poor data quality causes bad stock decisions, missed sales, and weak reporting confidence.

## Data Engineering Problem

Design a reliable, rerunnable raw-to-curated pipeline with explicit data quality controls, partitioned storage, and low query cost.

## Architecture (High Level)

- Raw/generated CSVs land in `s3://retail-inventory-dq-lake-sarang-2026/raw/generated/`
- **Local PySpark** (`scripts/glue_etl_inventory_dq.py`) — development and test runs against local folders
- **AWS Glue Spark ETL** — same script, cloud execution via IAM role `AWSGlueServiceRoleRetailInventoryDQ`
- Passed records → partitioned Parquet under `curated-glue/`; failed records → `quarantine-glue/`; DQ summaries → `dq-results-glue/`
- Athena external tables and views power SQL analytics (local `curated/` path used for MVP Athena proof; Glue outputs in `-glue` prefixes)
- Amazon QuickSight dashboards consume Athena-backed datasets

Detailed architecture notes: `docs/architecture.md`  
Glue operations: `docs/glue_job_runbook.md`

## AWS Glue ETL Deployment

The local PySpark ETL in `scripts/glue_etl_inventory_dq.py` was made **AWS Glue-compatible** (S3 paths, `getResolvedOptions` job parameters, Glue runtime detection). The deployed Glue Spark job:

- Reads generated CSV inputs from `s3://retail-inventory-dq-lake-sarang-2026/raw/generated/`
- Writes curated Parquet to `s3://retail-inventory-dq-lake-sarang-2026/curated-glue/`
- Writes failed records to `s3://retail-inventory-dq-lake-sarang-2026/quarantine-glue/`
- Writes DQ monitoring outputs to `s3://retail-inventory-dq-lake-sarang-2026/dq-results-glue/`
- Uses IAM role **`AWSGlueServiceRoleRetailInventoryDQ`**
- Completes successfully with confirmation in **CloudWatch** logs

### Glue deployment proof

| Screenshot | What it proves |
| --- | --- |
| [`14_glue_job_succeeded.png`](docs/screenshots/14_glue_job_succeeded.png) | Glue job run finished successfully |
| [`15_glue_job_arguments.png`](docs/screenshots/15_glue_job_arguments.png) | Job parameters (underscore-style) passed to the script |
| [`16_glue_cloudwatch_logs.png`](docs/screenshots/16_glue_cloudwatch_logs.png) | CloudWatch logs show successful ETL completion |
| [`17_s3_glue_curated_outputs.png`](docs/screenshots/17_s3_glue_curated_outputs.png) | Curated Parquet landed under `curated-glue/` |
| [`18_s3_glue_quarantine_outputs.png`](docs/screenshots/18_s3_glue_quarantine_outputs.png) | Quarantine outputs under `quarantine-glue/` |
| [`19_s3_glue_dq_results.png`](docs/screenshots/19_s3_glue_dq_results.png) | DQ rule/table score outputs under `dq-results-glue/` |

Full runbook: [`docs/glue_job_runbook.md`](docs/glue_job_runbook.md)

## Amazon QuickSight Dashboard

The published Amazon QuickSight dashboard completes the **AWS-native consumption layer** for this project. It is built from **Athena-backed datasets** over `retail_curated_db` and `retail_dq_db`, using tables and views already validated in Athena (`sql/analytics_views.sql`, `sql/dq_views.sql`). ETL runs locally for development and on **AWS Glue** for cloud execution; S3, Athena, and QuickSight are the implemented AWS surfaces.

The dashboard contains two sheets:

### 1. Retail Inventory Intelligence

- Total stockout records
- Average supplier delay
- Products requiring replenishment
- Average stockout rate
- Stockout rate by store and category
- Supplier delay rate
- Replenishment priority table

### 2. Data Quality Control Centre

- Row pass rate
- Curated rows
- Quarantined rows
- Total input rows
- Failed rows by DQ severity
- Rule-level DQ status
- Quarantined records by failure type
- DQ rules checked

Proof screenshots: `docs/screenshots/12_quicksight_inventory_dashboard.png`, `docs/screenshots/13_quicksight_dq_dashboard.png`.  
Full dashboard notes: `docs/quicksight_dashboard.md`.

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

## AWS Run

1. Upload raw/generated CSVs to S3 (`raw/generated/`).
2. Run the **AWS Glue Spark ETL job** with parameters in `docs/glue_job_runbook.md`.
3. Validate `curated-glue/`, `quarantine-glue/`, and `dq-results-glue/` in S3 and CloudWatch.
4. Register partitions in Athena (`MSCK REPAIR TABLE` or future Glue Crawler).
5. Execute Athena view SQL and refresh QuickSight datasets as needed.

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
- `12_quicksight_inventory_dashboard.png` - QuickSight Retail Inventory Intelligence sheet is published.
- `13_quicksight_dq_dashboard.png` - QuickSight Data Quality Control Centre sheet is published.
- `14_glue_job_succeeded.png` - AWS Glue Spark ETL job completed successfully.
- `15_glue_job_arguments.png` - Glue job parameters configured (underscore-style keys).
- `16_glue_cloudwatch_logs.png` - CloudWatch logs confirm successful Glue execution.
- `17_s3_glue_curated_outputs.png` - Curated Parquet outputs under `curated-glue/`.
- `18_s3_glue_quarantine_outputs.png` - Quarantine outputs under `quarantine-glue/`.
- `19_s3_glue_dq_results.png` - DQ monitoring outputs under `dq-results-glue/`.

Implementation status reflected by these screenshots:
- **Implemented:** local PySpark ETL (dev/test), AWS Glue Spark ETL (cloud), Amazon S3 data lake, Athena external tables/views, quarantine handling, Amazon QuickSight dashboards (Athena-backed).
- **Future improvements:** Glue Crawlers, AWS Glue Data Quality / DQDL, EventBridge scheduling, SNS alerts.

## Cost Control Notes

See `docs/cost_notes.md`.

## Cleanup Checklist

To be finalized in Phase 6 (delete jobs, crawlers, test data, BI resources).

## Resume Bullets

To be finalized in Phase 6.

## Interview Pitch

To be finalized in Phase 6.

## Portfolio Value

- Demonstrates end-to-end data lake design from local development through AWS Glue, S3, Athena, and QuickSight.
- Shows practical data quality engineering through validation, quarantine handling, and DQ monitoring views.
- Uses the same PySpark ETL script locally and on AWS Glue, with proof in CloudWatch and S3 `-glue` zones.
- Documents current implementation honestly while outlining Glue Crawlers, native Glue DQ, and alerting as next steps.

## Limitations and Future Improvements

MVP first; streaming, IaC, and ML are out of scope for initial build.

## Definition of Done

- [ ] Raw CSV data is stored in S3.
- [ ] Raw data is cataloged by Glue Crawler.
- [x] ETL job runs successfully from raw to curated (local PySpark and AWS Glue).
- [ ] Curated fact data is written as partitioned Parquet.
- [ ] `inventory_daily` includes `event_date` in the schema.
- [ ] Reruns use partition-scoped overwrite.
- [ ] At least 10 DQ rules are implemented.
- [ ] Failed records are written to quarantine with failure reasons.
- [ ] DQ results are queryable in Athena.
- [ ] At least 5 business Athena views exist.
- [ ] At least 3 DQ Athena views exist.
- [x] Dashboard has Inventory Intelligence and DQ Control Centre pages.
- [ ] README includes architecture, run steps, data model, DQ rules, costs and teardown.
- [ ] No secrets, credentials or unredacted AWS account IDs are committed.
- [ ] Screenshots redact account IDs and sensitive identifiers.
