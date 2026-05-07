# Cost Notes (MVP)

Use official AWS pricing pages as source of truth:

- Glue: https://aws.amazon.com/glue/pricing/
- Athena: https://aws.amazon.com/athena/pricing/
- S3: https://aws.amazon.com/s3/pricing/
- QuickSight: https://aws.amazon.com/quicksight/pricing/
- AWS Pricing Calculator: https://calculator.aws/

Always estimate using **ap-southeast-2 (Sydney)** in AWS Pricing Calculator before running workloads.

## MVP Cost-Control Checklist

- Keep dataset size small (target 1-5 GB during development).
- Use Parquet for curated tables.
- Partition curated fact tables by date.
- Avoid `SELECT *` in Athena.
- Run Glue jobs manually during development.
- Stop and delete unused resources (Glue jobs, crawlers, sessions, S3 test data).
- Use QuickSight only for portfolio screenshots or use local BI tooling.
