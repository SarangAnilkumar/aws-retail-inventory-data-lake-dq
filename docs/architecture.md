# Architecture Notes (MVP)

## Flow in 10-12 Bullets

1. Upload source CSV to `s3://retail-inventory-dq-lake/raw/inventory_daily/`.
2. Run Glue Crawler to catalog raw table(s) in `retail_raw_db`.
3. Glue PySpark ETL reads raw data via Data Catalog.
4. Glue Data Quality (DQDL) applies simple single-table checks.
5. PySpark applies cross-column and cross-table checks.
6. Passed records are transformed and written to curated Parquet.
7. Curated facts are partitioned by date components (`year`, `month`).
8. Failed records are written to quarantine with failure metadata.
9. DQ run outputs are written to `dq-results/rule_results` and `dq-results/table_scores`.
10. Register curated partitions using crawler or `MSCK REPAIR TABLE`.
11. Athena creates business and DQ reporting views.
12. BI tools (QuickSight/Tableau/Power BI) consume Athena views.

## Text Diagram

```text
Raw CSV (S3 raw/) -> Glue Crawler -> Glue Catalog (raw db)
                     -> Glue ETL (PySpark + DQ)
                     -> Curated Parquet (S3 curated/)
                     -> Quarantine (S3 quarantine/)
                     -> DQ outputs (S3 dq-results/)
Curated + DQ tables -> Athena external tables + views -> BI dashboards
```

## Raw -> Curated -> Quarantine -> Athena

- `raw/`: landed source files, minimal transformation.
- `curated/`: validated, typed, partitioned Parquet for analytics.
- `quarantine/`: failed rows with `failure_reason` and run metadata.
- `athena-results/`: query output staging for Athena.

## Dashboard Option

MVP supports Athena-backed outputs for QuickSight, Tableau, or Power BI.
