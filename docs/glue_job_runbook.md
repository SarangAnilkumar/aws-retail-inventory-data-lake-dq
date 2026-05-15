# AWS Glue ETL Job Runbook

## Purpose

Run the retail inventory ETL and data quality pipeline in **AWS Glue** using the same PySpark script as local development (`scripts/glue_etl_inventory_dq.py`). Cloud execution writes to dedicated `-glue` S3 prefixes so local MVP outputs (`curated/`, `quarantine/`, `dq-results/`) remain unchanged.

**Execution modes:**

| Mode | When to use | Output location |
| --- | --- | --- |
| Local PySpark | Development, unit validation, fast iteration | `data/curated`, `data/quarantine`, `data/dq-results` |
| AWS Glue Spark | Portfolio cloud proof, production-style runs | `curated-glue/`, `quarantine-glue/`, `dq-results-glue/` on S3 |

## IAM role

- **Role name:** `AWSGlueServiceRoleRetailInventoryDQ`
- **Used by:** Glue Spark ETL job
- **Must allow:**
  - `s3:GetObject`, `s3:ListBucket` on `s3://retail-inventory-dq-lake-sarang-2026/raw/generated/*`
  - `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` on:
    - `curated-glue/*`
    - `quarantine-glue/*`
    - `dq-results-glue/*`
  - CloudWatch Logs for Glue job output
  - Glue service operations for the job itself

## S3 paths

| Zone | S3 path |
| --- | --- |
| Input (generated CSVs) | `s3://retail-inventory-dq-lake-sarang-2026/raw/generated/` |
| Curated Parquet | `s3://retail-inventory-dq-lake-sarang-2026/curated-glue/` |
| Quarantine | `s3://retail-inventory-dq-lake-sarang-2026/quarantine-glue/` |
| DQ results | `s3://retail-inventory-dq-lake-sarang-2026/dq-results-glue/` |

Expected input files under `raw/generated/`: `stores.csv`, `products.csv`, `suppliers.csv`, `purchase_orders.csv`, `shipments.csv`, and either `inventory_daily_clean.csv` or `inventory_daily_with_bad_records.csv` (when `use_bad_records=true`).

## Glue job parameters

Use **underscore** keys in the Glue console (required for `getResolvedOptions`):

| Key | Example value |
| --- | --- |
| `--input_base_path` | `s3://retail-inventory-dq-lake-sarang-2026/raw/generated` |
| `--output_base_path` | `s3://retail-inventory-dq-lake-sarang-2026/curated-glue` |
| `--quarantine_base_path` | `s3://retail-inventory-dq-lake-sarang-2026/quarantine-glue` |
| `--dq_results_path` | `s3://retail-inventory-dq-lake-sarang-2026/dq-results-glue` |
| `--ingest_run_id` | `glue_run_001` |
| `--use_bad_records` | `true` |

Optional:

| Key | Default |
| --- | --- |
| `--inventory_file` | `inventory_daily_clean.csv` |
| `--bad_inventory_file` | `inventory_daily_with_bad_records.csv` |

`--JOB_NAME` is injected automatically by Glue; do not add it manually.

## Run instructions

1. Upload the latest script to S3, e.g.  
   `s3://retail-inventory-dq-lake-sarang-2026/scripts/glue_etl_inventory_dq.py`
2. Confirm generated CSVs exist under `raw/generated/`.
3. In **AWS Glue → ETL jobs**, open the retail inventory job.
4. Verify **IAM role** = `AWSGlueServiceRoleRetailInventoryDQ`.
5. Verify job parameters match the table above (underscore keys).
6. **Run job** and monitor in the Glue console.
7. Open **CloudWatch** log group for the job run; confirm `Running in Glue mode: true` and `ETL complete for ingest_run_id=...`.
8. Validate S3 prefixes `curated-glue/`, `quarantine-glue/`, `dq-results-glue/`.

## Validation

### S3 (console or CLI)

List top-level outputs after a successful run:

```bash
aws s3 ls s3://retail-inventory-dq-lake-sarang-2026/curated-glue/ --recursive | head
aws s3 ls s3://retail-inventory-dq-lake-sarang-2026/quarantine-glue/ --recursive | head
aws s3 ls s3://retail-inventory-dq-lake-sarang-2026/dq-results-glue/ --recursive | head
```

Expect:

- `curated-glue/inventory_daily/year=.../month=.../` (partitioned Parquet)
- `curated-glue/stores/`, `products/`, `suppliers/`, etc.
- `quarantine-glue/inventory_daily/`, etc.
- `dq-results-glue/rule_results/`, `dq-results-glue/table_scores/`

### CloudWatch

- Job status: **Succeeded**
- Log lines: `Running in Glue mode: true`, passed/failed row counts, `ETL complete`

### Athena (optional, after registering Glue paths)

Point external tables at `-glue` locations or run `MSCK REPAIR TABLE` on tables registered over `curated-glue/` if you extend the catalog.

## Local run (development)

```bash
python scripts/glue_etl_inventory_dq.py \
  --input-base-path data/generated \
  --output-base-path data/curated \
  --quarantine-base-path data/quarantine \
  --dq-results-path data/dq-results \
  --ingest-run-id local_run_001 \
  --use-bad-records true
```

Local mode uses hyphen CLI flags and does not require Glue job parameters.

## Troubleshooting

### Glue argument parsing (`SystemExit: 2`)

**Symptom:** Job fails immediately; CloudWatch shows `parser.parse_args()` / `SystemExit: 2`.

**Cause:** Script did not detect Glue runtime and fell through to local `argparse`.

**Fix:** Ensure updated script with robust Glue detection is deployed. Use **underscore** job parameter keys (`--input_base_path`, not `--input-base-path`) for `getResolvedOptions`.

### S3 `AccessDenied` on PutObject

**Symptom:** Job fails during write to `curated-glue/`, `quarantine-glue/`, or `dq-results-glue/`.

**Cause:** IAM role `AWSGlueServiceRoleRetailInventoryDQ` missing `s3:PutObject` (and optionally `s3:DeleteObject` for partition cleanup) on those prefixes.

**Fix:** Attach an inline policy granting read/write on the bucket paths above. Re-run the job.

### `getResolvedOptions` missing key

**Symptom:** `Argument ... is required` from `awsglue.utils.getResolvedOptions`.

**Fix:** Add every required parameter in the Glue job definition with underscore names. Optional file parameters can be omitted (defaults apply).

## Proof screenshots

| File | Description |
| --- | --- |
| `docs/screenshots/14_glue_job_succeeded.png` | Glue console shows job succeeded |
| `docs/screenshots/15_glue_job_arguments.png` | Job parameters (underscore keys and S3 paths) |
| `docs/screenshots/16_glue_cloudwatch_logs.png` | CloudWatch completion and row counts |
| `docs/screenshots/17_s3_glue_curated_outputs.png` | `curated-glue/` Parquet layout |
| `docs/screenshots/18_s3_glue_quarantine_outputs.png` | `quarantine-glue/` outputs |
| `docs/screenshots/19_s3_glue_dq_results.png` | `dq-results-glue/` rule and table scores |

## Future improvements (not in current MVP)

- **Glue Crawlers** — automatic schema and partition registration instead of manual `MSCK REPAIR TABLE`
- **AWS Glue Data Quality / DQDL** — native execution of `dq_rules/*.dqdl`
- **EventBridge** — scheduled pipeline triggers
- **SNS** — alerts on DQ failures or quarantine spikes
