#!/usr/bin/env python3
"""
PySpark ETL for inventory + data quality pipeline (local or AWS Glue).

- Read generated CSV inputs from local paths or s3://
- Apply PySpark data quality validations
- Split passed/failed rows
- Write curated, quarantine, and DQ summary Parquet outputs
- Keep fact-table writes idempotent via partition-scoped cleanup
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _running_on_glue() -> bool:
    return "JOB_NAME" in sys.argv or os.environ.get("AWS_EXECUTION_ENV") == "AWS_Glue"


def _is_s3_path(path: str | Path) -> bool:
    return str(path).startswith("s3://")


def join_uri(base: str | Path, *parts: str) -> str:
    """Join path segments without breaking s3:// URIs (pathlib breaks s3://)."""
    uri = str(base).rstrip("/")
    for part in parts:
        uri = f"{uri}/{str(part).lstrip('/')}"
    return uri


def parse_args() -> argparse.Namespace:
    if _running_on_glue():
        from awsglue.utils import getResolvedOptions

        glue_keys = [
            "JOB_NAME",
            "input-base-path",
            "output-base-path",
            "quarantine-base-path",
            "dq-results-path",
            "ingest-run-id",
            "use-bad-records",
        ]
        for optional_key in ("inventory-file", "bad-inventory-file"):
            if f"--{optional_key}" in sys.argv:
                glue_keys.append(optional_key)
        opts = getResolvedOptions(sys.argv, glue_keys)
        return argparse.Namespace(
            input_base_path=opts["input-base-path"],
            output_base_path=opts["output-base-path"],
            quarantine_base_path=opts["quarantine-base-path"],
            dq_results_path=opts["dq-results-path"],
            ingest_run_id=opts["ingest-run-id"],
            use_bad_records=opts["use-bad-records"],
            inventory_file=opts.get("inventory-file", "inventory_daily_clean.csv"),
            bad_inventory_file=opts.get(
                "bad-inventory-file", "inventory_daily_with_bad_records.csv"
            ),
        )

    parser = argparse.ArgumentParser(description="PySpark ETL + DQ for retail inventory.")
    parser.add_argument("--input-base-path", required=True)
    parser.add_argument("--output-base-path", required=True)
    parser.add_argument("--quarantine-base-path", required=True)
    parser.add_argument("--dq-results-path", required=True)
    parser.add_argument("--ingest-run-id", required=True)
    parser.add_argument("--use-bad-records", default="false")
    parser.add_argument("--inventory-file", default="inventory_daily_clean.csv")
    parser.add_argument("--bad-inventory-file", default="inventory_daily_with_bad_records.csv")
    return parser.parse_args()


def create_spark_session() -> SparkSession:
    builder = SparkSession.builder.appName("retail-inventory-etl-dq").config(
        "spark.sql.session.timeZone", "UTC"
    )
    if not _running_on_glue():
        builder = builder.master("local[*]")
    return builder.getOrCreate()


def read_csv_inputs(spark: SparkSession, args: argparse.Namespace) -> dict[str, DataFrame]:
    base = args.input_base_path
    use_bad = str(args.use_bad_records).strip().lower() == "true"
    inventory_name = args.bad_inventory_file if use_bad else args.inventory_file

    inventory = spark.read.option("header", True).csv(join_uri(base, inventory_name))
    stores = spark.read.option("header", True).csv(join_uri(base, "stores.csv"))
    products = spark.read.option("header", True).csv(join_uri(base, "products.csv"))
    suppliers = spark.read.option("header", True).csv(join_uri(base, "suppliers.csv"))
    purchase_orders = spark.read.option("header", True).csv(join_uri(base, "purchase_orders.csv"))
    shipments = spark.read.option("header", True).csv(join_uri(base, "shipments.csv"))

    return {
        "inventory": inventory,
        "stores": stores,
        "products": products,
        "suppliers": suppliers,
        "purchase_orders": purchase_orders,
        "shipments": shipments,
    }


def prepare_inventory(df: DataFrame, ingest_run_id: str) -> DataFrame:
    prepared = (
        df.withColumn("event_date", F.to_date("event_date"))
        # Use try_cast to avoid ANSI mode cast failures on malformed numeric strings.
        .withColumn("inventory_on_hand", F.expr("try_cast(inventory_on_hand as double)"))
        .withColumn("units_sold", F.expr("try_cast(units_sold as double)"))
        .withColumn("units_ordered", F.expr("try_cast(units_ordered as double)"))
        .withColumn("demand_forecast", F.expr("try_cast(demand_forecast as double)"))
        .withColumn("price", F.expr("try_cast(price as double)"))
        .withColumn("discount", F.expr("try_cast(discount as double)"))
        .withColumn(
            "promotion_flag",
            F.lower(F.trim(F.col("promotion_flag").cast("string"))).isin(
                "1", "true", "yes", "y"
            ),
        )
        .withColumn("available_stock", F.expr("try_cast(available_stock as double)"))
        .withColumn(
            "stockout_flag",
            F.lower(F.trim(F.col("stockout_flag").cast("string"))).isin(
                "1", "true", "yes", "y"
            ),
        )
        .withColumn("ingest_run_id", F.lit(ingest_run_id))
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("year", F.date_format("event_date", "yyyy"))
        .withColumn("month", F.date_format("event_date", "MM"))
    )
    return prepared


def prepare_purchase_orders(df: DataFrame, ingest_run_id: str) -> DataFrame:
    prepared = (
        df.withColumn("order_qty", F.expr("try_cast(order_qty as double)"))
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("expected_delivery_date", F.to_date("expected_delivery_date"))
        .withColumn("ingest_run_id", F.lit(ingest_run_id))
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("order_year", F.date_format("order_date", "yyyy"))
        .withColumn("order_month", F.date_format("order_date", "MM"))
    )
    return prepared


def prepare_shipments(df: DataFrame, ingest_run_id: str) -> DataFrame:
    prepared = (
        df.withColumn("shipped_date", F.to_date("shipped_date"))
        .withColumn("received_date", F.to_date("received_date"))
        .withColumn("quantity_received", F.expr("try_cast(quantity_received as double)"))
        .withColumn("delay_days", F.expr("try_cast(delay_days as double)"))
        .withColumn("ingest_run_id", F.lit(ingest_run_id))
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("received_year", F.date_format("received_date", "yyyy"))
        .withColumn("received_month", F.date_format("received_date", "MM"))
    )
    return prepared


def _apply_rule_array(df: DataFrame, rules: list[tuple[str, Any]]) -> DataFrame:
    reason_exprs = [F.when(cond, F.lit(name)) for name, cond in rules]
    with_reasons = df.withColumn("__reasons", F.array(*reason_exprs))
    with_reasons = with_reasons.withColumn(
        "failure_reason", F.concat_ws("; ", F.expr("filter(__reasons, x -> x is not null)"))
    )
    return with_reasons


def validate_inventory(
    inventory_df: DataFrame, products_df: DataFrame, stores_df: DataFrame
) -> tuple[DataFrame, DataFrame, list[dict[str, Any]]]:
    w = Window.partitionBy("event_date", "store_id", "product_id")
    keyed = inventory_df.withColumn("__dup_count", F.count("*").over(w))

    prod_ref = products_df.select(F.col("product_id").alias("__p_product_id")).distinct()
    store_ref = stores_df.select(F.col("store_id").alias("__s_store_id")).distinct()

    joined = (
        keyed.join(prod_ref, keyed.product_id == prod_ref.__p_product_id, "left")
        .join(store_ref, keyed.store_id == store_ref.__s_store_id, "left")
        .withColumn("__expected_available_stock", F.col("inventory_on_hand") - F.col("units_sold"))
        .withColumn("__expected_stockout_flag", F.col("__expected_available_stock") <= F.lit(0))
    )

    rules: list[tuple[str, Any]] = [
        ("event_date_not_null", F.col("event_date").isNull()),
        ("product_id_not_null", F.col("product_id").isNull() | (F.trim("product_id") == "")),
        ("store_id_not_null", F.col("store_id").isNull() | (F.trim("store_id") == "")),
        ("inventory_on_hand_non_negative", F.col("inventory_on_hand") < 0),
        ("units_sold_non_negative", F.col("units_sold") < 0),
        ("price_positive", F.col("price") <= 0),
        ("duplicate_logical_key", F.col("__dup_count") > 1),
        (
            "available_stock_matches_formula",
            F.col("available_stock") != F.col("__expected_available_stock"),
        ),
        ("stockout_flag_matches_formula", F.col("stockout_flag") != F.col("__expected_stockout_flag")),
        ("product_id_exists_in_products", F.col("__p_product_id").isNull()),
        ("store_id_exists_in_stores", F.col("__s_store_id").isNull()),
    ]

    checked = _apply_rule_array(joined, rules)
    failed = checked.filter(F.col("failure_reason") != "")
    passed = checked.filter(F.col("failure_reason") == "")

    stats = []
    for rule_name, _ in rules:
        failed_count = failed.filter(F.array_contains(F.col("__reasons"), F.lit(rule_name))).count()
        stats.append({"table_name": "inventory_daily", "rule_name": rule_name, "failed_row_count": failed_count})

    helper_cols = [
        "__reasons",
        "__dup_count",
        "__p_product_id",
        "__s_store_id",
        "__expected_available_stock",
        "__expected_stockout_flag",
    ]
    return passed.drop(*helper_cols), failed.drop(*helper_cols), stats


def validate_purchase_orders(
    po_df: DataFrame, products_df: DataFrame, stores_df: DataFrame, suppliers_df: DataFrame
) -> tuple[DataFrame, DataFrame, list[dict[str, Any]]]:
    prod_ref = products_df.select(F.col("product_id").alias("__p_product_id")).distinct()
    store_ref = stores_df.select(F.col("store_id").alias("__s_store_id")).distinct()
    supp_ref = suppliers_df.select(F.col("supplier_id").alias("__sup_supplier_id")).distinct()

    joined = (
        po_df.join(prod_ref, po_df.product_id == prod_ref.__p_product_id, "left")
        .join(store_ref, po_df.store_id == store_ref.__s_store_id, "left")
        .join(supp_ref, po_df.supplier_id == supp_ref.__sup_supplier_id, "left")
    )

    rules: list[tuple[str, Any]] = [
        ("po_id_not_null", F.col("po_id").isNull() | (F.trim("po_id") == "")),
        ("product_id_exists_in_products", F.col("__p_product_id").isNull()),
        ("store_id_exists_in_stores", F.col("__s_store_id").isNull()),
        ("supplier_id_exists_in_suppliers", F.col("__sup_supplier_id").isNull()),
        ("expected_delivery_not_before_order_date", F.col("expected_delivery_date") < F.col("order_date")),
        ("order_qty_positive", F.col("order_qty") <= 0),
    ]

    checked = _apply_rule_array(joined, rules)
    failed = checked.filter(F.col("failure_reason") != "")
    passed = checked.filter(F.col("failure_reason") == "")

    stats = []
    for rule_name, _ in rules:
        failed_count = failed.filter(F.array_contains(F.col("__reasons"), F.lit(rule_name))).count()
        stats.append({"table_name": "purchase_orders", "rule_name": rule_name, "failed_row_count": failed_count})

    helper_cols = ["__reasons", "__p_product_id", "__s_store_id", "__sup_supplier_id"]
    return passed.drop(*helper_cols), failed.drop(*helper_cols), stats


def validate_shipments(
    shipments_df: DataFrame,
    purchase_orders_df: DataFrame,
    products_df: DataFrame,
    stores_df: DataFrame,
    suppliers_df: DataFrame,
) -> tuple[DataFrame, DataFrame, list[dict[str, Any]]]:
    po_ref = purchase_orders_df.select(F.col("po_id").alias("__po_id")).distinct()
    prod_ref = products_df.select(F.col("product_id").alias("__p_product_id")).distinct()
    store_ref = stores_df.select(F.col("store_id").alias("__s_store_id")).distinct()
    supp_ref = suppliers_df.select(F.col("supplier_id").alias("__sup_supplier_id")).distinct()

    joined = (
        shipments_df.join(po_ref, shipments_df.po_id == po_ref.__po_id, "left")
        .join(prod_ref, shipments_df.product_id == prod_ref.__p_product_id, "left")
        .join(store_ref, shipments_df.store_id == store_ref.__s_store_id, "left")
        .join(supp_ref, shipments_df.supplier_id == supp_ref.__sup_supplier_id, "left")
    )

    rules: list[tuple[str, Any]] = [
        ("shipment_id_not_null", F.col("shipment_id").isNull() | (F.trim("shipment_id") == "")),
        ("po_id_exists_in_purchase_orders", F.col("__po_id").isNull()),
        ("supplier_id_exists_in_suppliers", F.col("__sup_supplier_id").isNull()),
        ("product_id_exists_in_products", F.col("__p_product_id").isNull()),
        ("store_id_exists_in_stores", F.col("__s_store_id").isNull()),
        ("received_date_not_before_shipped_date", F.col("received_date") < F.col("shipped_date")),
        ("quantity_received_non_negative", F.col("quantity_received") < 0),
    ]

    checked = _apply_rule_array(joined, rules)
    failed = checked.filter(F.col("failure_reason") != "")
    passed = checked.filter(F.col("failure_reason") == "")

    stats = []
    for rule_name, _ in rules:
        failed_count = failed.filter(F.array_contains(F.col("__reasons"), F.lit(rule_name))).count()
        stats.append({"table_name": "shipments", "rule_name": rule_name, "failed_row_count": failed_count})

    helper_cols = ["__reasons", "__po_id", "__p_product_id", "__s_store_id", "__sup_supplier_id"]
    return passed.drop(*helper_cols), failed.drop(*helper_cols), stats


def build_rule_results(
    spark: SparkSession, ingest_run_id: str, rule_stats: list[dict[str, Any]]
) -> DataFrame:
    severity_map = {
        "event_date_not_null": "Critical",
        "product_id_not_null": "Critical",
        "store_id_not_null": "Critical",
        "inventory_on_hand_non_negative": "Critical",
        "units_sold_non_negative": "Medium",
        "price_positive": "Critical",
        "duplicate_logical_key": "High",
        "available_stock_matches_formula": "High",
        "stockout_flag_matches_formula": "High",
        "product_id_exists_in_products": "Critical",
        "store_id_exists_in_stores": "Critical",
        "po_id_not_null": "Critical",
        "supplier_id_exists_in_suppliers": "Critical",
        "expected_delivery_not_before_order_date": "High",
        "order_qty_positive": "Medium",
        "shipment_id_not_null": "Critical",
        "po_id_exists_in_purchase_orders": "Critical",
        "received_date_not_before_shipped_date": "High",
        "quantity_received_non_negative": "Medium",
    }
    checked_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    rows = []
    for row in rule_stats:
        failed_count = int(row["failed_row_count"])
        rows.append(
            {
                "ingest_run_id": ingest_run_id,
                "table_name": row["table_name"],
                "rule_name": row["rule_name"],
                "engine": "PySpark",
                "severity": severity_map.get(row["rule_name"], "Medium"),
                "status": "FAIL" if failed_count > 0 else "PASS",
                "failed_row_count": failed_count,
                "checked_at": checked_at,
            }
        )
    return spark.createDataFrame(rows)


def build_table_scores(spark: SparkSession, ingest_run_id: str, rule_results_df: DataFrame) -> DataFrame:
    checked_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    scored = (
        rule_results_df.groupBy("table_name")
        .agg(
            F.count("*").alias("total_rules"),
            F.sum(F.when(F.col("status") == "PASS", F.lit(1)).otherwise(F.lit(0))).alias("passed_rules"),
            F.sum(F.when(F.col("status") == "FAIL", F.lit(1)).otherwise(F.lit(0))).alias("failed_rules"),
        )
        .withColumn(
            "quality_score",
            F.when(F.col("total_rules") == 0, F.lit(0.0)).otherwise(
                F.col("passed_rules") / F.col("total_rules")
            ),
        )
        .withColumn("ingest_run_id", F.lit(ingest_run_id))
        .withColumn("checked_at", F.lit(checked_at))
        .select(
            "ingest_run_id",
            "table_name",
            "total_rules",
            "passed_rules",
            "failed_rules",
            "quality_score",
            "checked_at",
        )
    )
    return scored


def delete_affected_partitions(
    spark: SparkSession, base_path: str | Path, partition_cols: list[str], df: DataFrame
) -> None:
    if df.rdd.isEmpty():
        return

    base = str(base_path)
    distinct_parts = df.select(*partition_cols).distinct().collect()

    if _is_s3_path(base):
        jvm = spark._jvm
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem
        uri = jvm.java.net.URI
        path_cls = jvm.org.apache.hadoop.fs.Path

        for row in distinct_parts:
            segments = [f"{col}={row[col]}" for col in partition_cols]
            part_path = join_uri(base, *segments)
            fs_obj = fs.get(uri(part_path), hadoop_conf)
            part_obj = path_cls(part_path)
            if fs_obj.exists(part_obj):
                fs_obj.delete(part_obj, True)
        return

    local_base = Path(base)
    if not local_base.exists():
        return

    for row in distinct_parts:
        part_path = local_base
        for col in partition_cols:
            part_path = part_path / f"{col}={row[col]}"
        if part_path.exists():
            shutil.rmtree(part_path, ignore_errors=True)


def write_curated_outputs(
    spark: SparkSession, output_base: str | Path, data: dict[str, DataFrame]
) -> None:
    output_base_str = str(output_base)
    if not _is_s3_path(output_base_str):
        Path(output_base_str).mkdir(parents=True, exist_ok=True)

    # Dimensions: small tables, overwrite fully.
    data["stores"].write.mode("overwrite").parquet(join_uri(output_base_str, "stores"))
    data["products"].write.mode("overwrite").parquet(join_uri(output_base_str, "products"))
    data["suppliers"].write.mode("overwrite").parquet(join_uri(output_base_str, "suppliers"))

    inventory_path = join_uri(output_base_str, "inventory_daily")
    po_path = join_uri(output_base_str, "purchase_orders")
    shipments_path = join_uri(output_base_str, "shipments")

    delete_affected_partitions(spark, inventory_path, ["year", "month"], data["inventory_daily"])
    delete_affected_partitions(
        spark, po_path, ["order_year", "order_month"], data["purchase_orders"]
    )
    delete_affected_partitions(
        spark, shipments_path, ["received_year", "received_month"], data["shipments"]
    )

    data["inventory_daily"].write.mode("append").partitionBy("year", "month").parquet(
        inventory_path, compression="snappy"
    )
    data["purchase_orders"].write.mode("append").partitionBy("order_year", "order_month").parquet(
        po_path, compression="snappy"
    )
    data["shipments"].write.mode("append").partitionBy("received_year", "received_month").parquet(
        shipments_path, compression="snappy"
    )


def write_quarantine_outputs(
    quarantine_base: str | Path, failed_data: dict[str, DataFrame], ingest_run_id: str
) -> None:
    quarantine_base_str = str(quarantine_base)
    if not _is_s3_path(quarantine_base_str):
        Path(quarantine_base_str).mkdir(parents=True, exist_ok=True)
    failed_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    for table_name, df in failed_data.items():
        if df.rdd.isEmpty():
            continue
        out = (
            df.withColumn("ingest_run_id", F.lit(ingest_run_id))
            .withColumn("table_name", F.lit(table_name))
            .withColumn("failed_at", F.lit(failed_at))
        )
        out.write.mode("overwrite").parquet(join_uri(quarantine_base_str, table_name))


def write_dq_results(
    dq_path: str | Path, rule_results_df: DataFrame, table_scores_df: DataFrame
) -> None:
    dq_path_str = str(dq_path)
    if not _is_s3_path(dq_path_str):
        Path(dq_path_str).mkdir(parents=True, exist_ok=True)
    rule_results_df.write.mode("overwrite").parquet(join_uri(dq_path_str, "rule_results"))
    table_scores_df.write.mode("overwrite").parquet(join_uri(dq_path_str, "table_scores"))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    args = parse_args()
    spark = create_spark_session()

    try:
        inputs = read_csv_inputs(spark, args)
        ingest_run_id = args.ingest_run_id

        stores_df = inputs["stores"]
        products_df = inputs["products"]
        suppliers_df = inputs["suppliers"]

        inventory_df = prepare_inventory(inputs["inventory"], ingest_run_id=ingest_run_id)
        purchase_orders_df = prepare_purchase_orders(
            inputs["purchase_orders"], ingest_run_id=ingest_run_id
        )
        shipments_df = prepare_shipments(inputs["shipments"], ingest_run_id=ingest_run_id)

        inv_passed, inv_failed, inv_stats = validate_inventory(inventory_df, products_df, stores_df)
        po_passed, po_failed, po_stats = validate_purchase_orders(
            purchase_orders_df, products_df, stores_df, suppliers_df
        )
        shp_passed, shp_failed, shp_stats = validate_shipments(
            shipments_df, po_passed, products_df, stores_df, suppliers_df
        )

        curated_data = {
            "inventory_daily": inv_passed,
            "stores": stores_df,
            "products": products_df,
            "suppliers": suppliers_df,
            "purchase_orders": po_passed,
            "shipments": shp_passed,
        }
        failed_data = {
            "inventory_daily": inv_failed,
            "purchase_orders": po_failed,
            "shipments": shp_failed,
        }

        all_stats = inv_stats + po_stats + shp_stats
        rule_results_df = build_rule_results(spark, ingest_run_id, all_stats)
        table_scores_df = build_table_scores(spark, ingest_run_id, rule_results_df)

        write_curated_outputs(spark, args.output_base_path, curated_data)
        write_quarantine_outputs(args.quarantine_base_path, failed_data, ingest_run_id)
        write_dq_results(args.dq_results_path, rule_results_df, table_scores_df)

        logging.info("ETL complete for ingest_run_id=%s", ingest_run_id)
        logging.info(
            "Passed counts | inventory=%d purchase_orders=%d shipments=%d",
            inv_passed.count(),
            po_passed.count(),
            shp_passed.count(),
        )
        logging.info(
            "Failed counts | inventory=%d purchase_orders=%d shipments=%d",
            inv_failed.count(),
            po_failed.count(),
            shp_failed.count(),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
