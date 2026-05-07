#!/usr/bin/env python3
"""
Generate synthetic supporting tables for the retail inventory MVP.

Phase 2 scope:
- Local pandas-based generation only
- Deterministic output using random_seed
- No AWS dependencies
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


CANONICAL_COLUMNS = [
    "event_date",
    "store_id",
    "product_id",
    "category",
    "region",
    "inventory_on_hand",
    "units_sold",
    "units_ordered",
    "demand_forecast",
    "price",
    "discount",
    "promotion_flag",
]

STORE_TYPES = ["Metro", "Suburban", "Regional"]
PRODUCT_STATUS = ["Active", "Discontinued", "Backordered"]
PO_STATUS = ["Open", "Received", "Delayed", "Cancelled"]
DELIVERY_STATUS = ["On Time", "Delayed", "Partial", "Not Received"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic supporting tables for retail inventory MVP."
    )
    parser.add_argument("--input-path", required=True, help="Path to raw inventory CSV.")
    parser.add_argument(
        "--output-path", required=True, help="Output directory for generated files."
    )
    parser.add_argument(
        "--bad-record-rate",
        type=float,
        default=0.0,
        help="Fraction of rows to inject with data quality issues (0.0 to 0.2 recommended).",
    )
    parser.add_argument(
        "--random-seed", type=int, default=42, help="Random seed for deterministic output."
    )
    parser.add_argument(
        "--num-suppliers",
        type=int,
        default=12,
        help="Number of synthetic suppliers to generate.",
    )
    parser.add_argument(
        "--min-lead-time-days", type=int, default=2, help="Minimum supplier lead time."
    )
    parser.add_argument(
        "--max-lead-time-days", type=int, default=10, help="Maximum supplier lead time."
    )
    return parser.parse_args()


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def standardize_columns(columns: list[str]) -> list[str]:
    std = []
    for col in columns:
        col = col.strip().lower()
        col = re.sub(r"[^a-z0-9]+", "_", col)
        col = re.sub(r"_+", "_", col).strip("_")
        std.append(col)
    return std


def validate_required_columns(df: pd.DataFrame) -> None:
    required = set(CANONICAL_COLUMNS)
    available = set(df.columns)
    missing = sorted(required - available)
    if missing:
        raise ValueError(
            "Required columns are missing after standardization.\n"
            f"Required columns: {sorted(required)}\n"
            f"Available columns: {sorted(available)}\n"
            f"Missing columns: {missing}"
        )


def load_inventory(input_path: Path) -> pd.DataFrame:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df = pd.read_csv(input_path)
    original_columns = df.columns.tolist()
    standardized_columns = standardize_columns(df.columns.tolist())
    df.columns = standardized_columns

    rename_map: dict[str, str] = {
        "date": "event_date",
        "inventory_level": "inventory_on_hand",
    }

    # Promotion fallback mapping priority.
    if "promotion_flag" not in df.columns:
        for fallback in [
            "promotion",
            "holiday_promotion",
            "holiday_or_promotion",
            "holiday",
            "promo",
        ]:
            if fallback in df.columns:
                rename_map[fallback] = "promotion_flag"
                break

    logging.debug("Original columns: %s", original_columns)
    logging.debug("Standardized columns: %s", standardized_columns)
    logging.debug("Applied rename_map: %s", rename_map)
    df = df.rename(columns=rename_map)
    logging.debug("Final columns before required validation: %s", df.columns.tolist())
    validate_required_columns(df)

    df = df[CANONICAL_COLUMNS].copy()
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
    if df["event_date"].isna().all():
        raise ValueError("Unable to parse event_date values from input dataset.")

    numeric_cols = [
        "inventory_on_hand",
        "units_sold",
        "units_ordered",
        "demand_forecast",
        "price",
        "discount",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["store_id"] = df["store_id"].astype(str).str.strip()
    df["product_id"] = df["product_id"].astype(str).str.strip()
    df["category"] = df["category"].astype(str).str.strip()
    df["region"] = df["region"].astype(str).str.strip()

    # Standardize promotion flag into boolean
    bool_map = {
        "1": True,
        "0": False,
        "true": True,
        "false": False,
        "yes": True,
        "no": False,
        "y": True,
        "n": False,
    }
    promo_str = df["promotion_flag"].astype(str).str.strip().str.lower()
    df["promotion_flag"] = promo_str.map(bool_map)
    df["promotion_flag"] = df["promotion_flag"].fillna(False)

    df["available_stock"] = df["inventory_on_hand"] - df["units_sold"]
    df["stockout_flag"] = df["available_stock"] <= 0

    df = df.dropna(subset=["event_date", "store_id", "product_id"]).reset_index(drop=True)
    return df


def generate_suppliers(
    num_suppliers: int,
    min_lead_time_days: int,
    max_lead_time_days: int,
    rng: np.random.Generator,
) -> pd.DataFrame:
    if num_suppliers < 1:
        raise ValueError("num_suppliers must be >= 1")
    if min_lead_time_days < 1 or max_lead_time_days < min_lead_time_days:
        raise ValueError("Invalid lead time bounds.")

    regions = ["North", "South", "East", "West", "Central"]
    rows: list[dict[str, Any]] = []
    for i in range(1, num_suppliers + 1):
        rows.append(
            {
                "supplier_id": f"SUP{i:03d}",
                "supplier_name": f"Supplier {i:03d}",
                "lead_time_days": int(
                    rng.integers(min_lead_time_days, max_lead_time_days + 1)
                ),
                "reliability_score": float(np.round(rng.uniform(0.75, 0.99), 2)),
                "region": str(rng.choice(regions)),
            }
        )
    return pd.DataFrame(rows)


def generate_stores(inventory_df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    min_date = inventory_df["event_date"].min().date()
    max_open_offset = 3650  # 10 years before earliest event

    # Choose deterministic primary region per store:
    # 1) most frequent region
    # 2) alphabetical region in ties
    region_ranked = (
        inventory_df.groupby(["store_id", "region"], as_index=False)
        .size()
        .rename(columns={"size": "region_count"})
        .sort_values(["store_id", "region_count", "region"], ascending=[True, False, True])
    )
    primary_region = (
        region_ranked.groupby("store_id", as_index=False).first()[["store_id", "region"]]
    )
    stores = primary_region.sort_values("store_id").reset_index(drop=True)

    stores = stores.assign(
        store_name=stores["store_id"].apply(lambda x: f"Store {x}"),
        store_type=rng.choice(STORE_TYPES, size=len(stores), p=[0.45, 0.4, 0.15]),
    )

    offsets = rng.integers(180, max_open_offset, size=len(stores))
    stores["opened_date"] = [
        (min_date - timedelta(days=int(offset))).isoformat() for offset in offsets
    ]

    return stores[["store_id", "store_name", "region", "store_type", "opened_date"]]


def generate_products(
    inventory_df: pd.DataFrame, suppliers_df: pd.DataFrame, rng: np.random.Generator
) -> pd.DataFrame:
    product_base = (
        inventory_df.sort_values("event_date")
        .groupby("product_id", as_index=False)
        .agg(
            category=("category", "last"),
            unit_price=("price", "median"),
            avg_units_sold=("units_sold", "mean"),
        )
    )

    reorder_level = np.maximum(np.ceil(product_base["avg_units_sold"] * 7), 10).astype(int)
    reorder_qty = np.maximum(np.ceil(product_base["avg_units_sold"] * 14), reorder_level + 5)
    reorder_qty = reorder_qty.astype(int)

    supplier_ids = suppliers_df["supplier_id"].tolist()
    status = rng.choice(PRODUCT_STATUS, size=len(product_base), p=[0.82, 0.08, 0.10])

    products = pd.DataFrame(
        {
            "product_id": product_base["product_id"],
            "product_name": product_base["product_id"].apply(lambda x: f"Product {x}"),
            "category": product_base["category"].fillna("Unknown"),
            "unit_price": np.round(product_base["unit_price"].fillna(0.0), 2),
            "reorder_level": reorder_level,
            "reorder_quantity": reorder_qty,
            "supplier_id": rng.choice(supplier_ids, size=len(product_base)),
            "status": status,
        }
    )
    return products


def generate_purchase_orders(
    inventory_df: pd.DataFrame,
    products_df: pd.DataFrame,
    suppliers_df: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    merged = inventory_df.merge(
        products_df[["product_id", "reorder_level", "reorder_quantity", "supplier_id"]],
        on="product_id",
        how="left",
    )
    low_stock = merged[merged["inventory_on_hand"] <= merged["reorder_level"]].copy()
    if low_stock.empty:
        return pd.DataFrame(
            columns=[
                "po_id",
                "product_id",
                "supplier_id",
                "store_id",
                "order_qty",
                "order_date",
                "expected_delivery_date",
                "status",
            ]
        )

    low_stock = low_stock.sort_values(["event_date", "store_id", "product_id"])
    low_stock = low_stock.drop_duplicates(["event_date", "store_id", "product_id"])

    # Keep reasonable PO volume for MVP.
    sample_frac = 0.35
    sampled = low_stock.sample(frac=sample_frac, random_state=int(rng.integers(1, 1_000_000)))
    sampled = sampled.sort_values(["event_date", "store_id", "product_id"]).reset_index(drop=True)

    supplier_lead_map = suppliers_df.set_index("supplier_id")["lead_time_days"].to_dict()
    statuses = rng.choice(PO_STATUS, size=len(sampled), p=[0.25, 0.60, 0.12, 0.03])

    rows: list[dict[str, Any]] = []
    for idx, row in sampled.iterrows():
        supplier_id = row["supplier_id"]
        lead_time = int(supplier_lead_map.get(supplier_id, 5))
        order_date = row["event_date"].date()
        expected_delivery = order_date + timedelta(days=lead_time)
        rows.append(
            {
                "po_id": f"PO{idx + 1:08d}",
                "product_id": row["product_id"],
                "supplier_id": supplier_id,
                "store_id": row["store_id"],
                "order_qty": int(row["reorder_quantity"]),
                "order_date": order_date.isoformat(),
                "expected_delivery_date": expected_delivery.isoformat(),
                "status": str(statuses[idx]),
            }
        )
    return pd.DataFrame(rows)


def generate_shipments(
    purchase_orders_df: pd.DataFrame, rng: np.random.Generator
) -> pd.DataFrame:
    if purchase_orders_df.empty:
        return pd.DataFrame(
            columns=[
                "shipment_id",
                "po_id",
                "supplier_id",
                "store_id",
                "product_id",
                "shipped_date",
                "received_date",
                "quantity_received",
                "delivery_status",
                "delay_days",
            ]
        )

    rows: list[dict[str, Any]] = []
    for idx, row in purchase_orders_df.iterrows():
        order_date = datetime.fromisoformat(row["order_date"]).date()
        expected = datetime.fromisoformat(row["expected_delivery_date"]).date()
        shipped_gap = int(rng.integers(0, 3))
        shipped_date = order_date + timedelta(days=shipped_gap)

        status = str(rng.choice(DELIVERY_STATUS, p=[0.68, 0.17, 0.12, 0.03]))
        if status == "On Time":
            received_date = expected
            qty = int(row["order_qty"])
        elif status == "Delayed":
            delay = int(rng.integers(1, 6))
            received_date = expected + timedelta(days=delay)
            qty = int(row["order_qty"])
        elif status == "Partial":
            delay = int(rng.integers(0, 4))
            received_date = expected + timedelta(days=delay)
            qty = max(1, int(np.floor(row["order_qty"] * rng.uniform(0.5, 0.9))))
        else:  # Not Received
            received_date = expected + timedelta(days=int(rng.integers(3, 8)))
            qty = 0

        delay_days = (received_date - expected).days
        rows.append(
            {
                "shipment_id": f"SHP{idx + 1:08d}",
                "po_id": row["po_id"],
                "supplier_id": row["supplier_id"],
                "store_id": row["store_id"],
                "product_id": row["product_id"],
                "shipped_date": shipped_date.isoformat(),
                "received_date": received_date.isoformat(),
                "quantity_received": int(qty),
                "delivery_status": status,
                "delay_days": int(delay_days),
            }
        )
    return pd.DataFrame(rows)


def inject_bad_records(
    clean_df: pd.DataFrame, bad_record_rate: float, rng: np.random.Generator
) -> tuple[pd.DataFrame, dict[str, int]]:
    bad_df = clean_df.copy()
    issue_counts: dict[str, int] = {
        "null_product_id": 0,
        "null_store_id": 0,
        "negative_inventory_on_hand": 0,
        "negative_units_sold": 0,
        "non_positive_price": 0,
        "future_event_date": 0,
        "duplicate_logical_key_row": 0,
    }

    if bad_record_rate <= 0:
        return bad_df, issue_counts

    if not 0 <= bad_record_rate <= 0.5:
        raise ValueError("bad_record_rate must be between 0.0 and 0.5")

    n_rows = len(bad_df)
    n_bad = max(1, int(np.floor(n_rows * bad_record_rate)))
    per_issue = max(1, n_bad // 6)

    idx_pool = np.arange(n_rows)
    rng.shuffle(idx_pool)
    next_index = 0

    def take_indices(count: int) -> np.ndarray:
        nonlocal next_index
        end = min(next_index + count, n_rows)
        chosen = idx_pool[next_index:end]
        next_index = end
        return chosen

    idx = take_indices(per_issue)
    bad_df.loc[idx, "product_id"] = np.nan
    issue_counts["null_product_id"] = len(idx)

    idx = take_indices(per_issue)
    bad_df.loc[idx, "store_id"] = np.nan
    issue_counts["null_store_id"] = len(idx)

    idx = take_indices(per_issue)
    bad_df.loc[idx, "inventory_on_hand"] = -np.abs(bad_df.loc[idx, "inventory_on_hand"]).fillna(1)
    issue_counts["negative_inventory_on_hand"] = len(idx)

    idx = take_indices(per_issue)
    bad_df.loc[idx, "units_sold"] = -np.abs(bad_df.loc[idx, "units_sold"]).fillna(1)
    issue_counts["negative_units_sold"] = len(idx)

    idx = take_indices(per_issue)
    bad_df.loc[idx, "price"] = -np.abs(bad_df.loc[idx, "price"]).fillna(1)
    issue_counts["non_positive_price"] = len(idx)

    idx = take_indices(per_issue)
    bad_df.loc[idx, "event_date"] = (pd.Timestamp.today().normalize() + pd.Timedelta(days=45))
    issue_counts["future_event_date"] = len(idx)

    # Duplicate logical key rows by appending sampled rows.
    dup_n = max(1, n_bad - sum(issue_counts.values()))
    dup_rows = bad_df.sample(n=dup_n, random_state=int(rng.integers(1, 1_000_000)))
    bad_df = pd.concat([bad_df, dup_rows], ignore_index=True)
    issue_counts["duplicate_logical_key_row"] = dup_n

    return bad_df, issue_counts


def validate_key_integrity(
    inventory_df: pd.DataFrame,
    stores_df: pd.DataFrame,
    products_df: pd.DataFrame,
    suppliers_df: pd.DataFrame,
) -> None:
    inv_stores = set(inventory_df["store_id"].dropna().unique().tolist())
    inv_products = set(inventory_df["product_id"].dropna().unique().tolist())
    gen_stores = set(stores_df["store_id"].dropna().unique().tolist())
    gen_products = set(products_df["product_id"].dropna().unique().tolist())
    gen_suppliers = set(suppliers_df["supplier_id"].dropna().unique().tolist())
    product_suppliers = set(products_df["supplier_id"].dropna().unique().tolist())

    if not gen_stores.issubset(inv_stores):
        raise ValueError("stores contains store_id values not present in inventory_daily.")
    if not gen_products.issubset(inv_products):
        raise ValueError("products contains product_id values not present in inventory_daily.")
    if not product_suppliers.issubset(gen_suppliers):
        raise ValueError("products contains supplier_id values missing from suppliers.")


def validate_no_negative_generated_quantities(
    products_df: pd.DataFrame, purchase_orders_df: pd.DataFrame, shipments_df: pd.DataFrame
) -> None:
    if (products_df["reorder_level"] < 0).any():
        raise ValueError("products.reorder_level contains negative values.")
    if (products_df["reorder_quantity"] <= 0).any():
        raise ValueError("products.reorder_quantity contains non-positive values.")
    if not purchase_orders_df.empty and (purchase_orders_df["order_qty"] <= 0).any():
        raise ValueError("purchase_orders.order_qty contains non-positive values.")
    if not shipments_df.empty and (shipments_df["quantity_received"] < 0).any():
        raise ValueError("shipments.quantity_received contains negative values.")


def validate_purchase_order_relationships(
    purchase_orders_df: pd.DataFrame,
    stores_df: pd.DataFrame,
    products_df: pd.DataFrame,
    suppliers_df: pd.DataFrame,
) -> None:
    if purchase_orders_df.empty:
        return
    valid_store = set(stores_df["store_id"])
    valid_product = set(products_df["product_id"])
    valid_supplier = set(suppliers_df["supplier_id"])

    if not set(purchase_orders_df["store_id"]).issubset(valid_store):
        raise ValueError("purchase_orders has invalid store_id references.")
    if not set(purchase_orders_df["product_id"]).issubset(valid_product):
        raise ValueError("purchase_orders has invalid product_id references.")
    if not set(purchase_orders_df["supplier_id"]).issubset(valid_supplier):
        raise ValueError("purchase_orders has invalid supplier_id references.")


def validate_shipment_relationships(
    shipments_df: pd.DataFrame, purchase_orders_df: pd.DataFrame
) -> None:
    if shipments_df.empty:
        return
    valid_po = set(purchase_orders_df["po_id"])
    if not set(shipments_df["po_id"]).issubset(valid_po):
        raise ValueError("shipments has invalid po_id references.")


def validate_unique_keys(
    stores_df: pd.DataFrame,
    products_df: pd.DataFrame,
    suppliers_df: pd.DataFrame,
    purchase_orders_df: pd.DataFrame,
    shipments_df: pd.DataFrame,
) -> None:
    key_checks = [
        ("stores.store_id", stores_df, "store_id"),
        ("products.product_id", products_df, "product_id"),
        ("suppliers.supplier_id", suppliers_df, "supplier_id"),
        ("purchase_orders.po_id", purchase_orders_df, "po_id"),
        ("shipments.shipment_id", shipments_df, "shipment_id"),
    ]

    for label, frame, col in key_checks:
        if frame.empty:
            continue
        dup_count = int(frame[col].duplicated().sum())
        if dup_count > 0:
            raise ValueError(f"{label} contains duplicates. duplicate_count={dup_count}")


def write_outputs(
    output_path: Path,
    inventory_clean_df: pd.DataFrame,
    stores_df: pd.DataFrame,
    products_df: pd.DataFrame,
    suppliers_df: pd.DataFrame,
    purchase_orders_df: pd.DataFrame,
    shipments_df: pd.DataFrame,
    inventory_bad_df: pd.DataFrame | None,
) -> None:
    output_path.mkdir(parents=True, exist_ok=True)

    inventory_to_write = inventory_clean_df.copy()
    inventory_to_write["event_date"] = inventory_to_write["event_date"].dt.date
    inventory_to_write.to_csv(output_path / "inventory_daily_clean.csv", index=False)
    stores_df.to_csv(output_path / "stores.csv", index=False)
    products_df.to_csv(output_path / "products.csv", index=False)
    suppliers_df.to_csv(output_path / "suppliers.csv", index=False)
    purchase_orders_df.to_csv(output_path / "purchase_orders.csv", index=False)
    shipments_df.to_csv(output_path / "shipments.csv", index=False)

    if inventory_bad_df is not None:
        bad_to_write = inventory_bad_df.copy()
        bad_to_write["event_date"] = pd.to_datetime(
            bad_to_write["event_date"], errors="coerce"
        ).dt.date
        bad_to_write.to_csv(output_path / "inventory_daily_with_bad_records.csv", index=False)


def write_generation_summary(output_path: Path, summary: dict[str, Any]) -> None:
    with (output_path / "generation_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=True)


def main() -> None:
    configure_logging()
    args = parse_args()
    rng = np.random.default_rng(args.random_seed)

    input_path = Path(args.input_path)
    output_path = Path(args.output_path)

    logging.info("Loading and standardizing inventory source data from %s", input_path)
    inventory_df = load_inventory(input_path)
    input_row_count = len(inventory_df)

    logging.info("Generating suppliers...")
    suppliers_df = generate_suppliers(
        num_suppliers=args.num_suppliers,
        min_lead_time_days=args.min_lead_time_days,
        max_lead_time_days=args.max_lead_time_days,
        rng=rng,
    )

    logging.info("Generating stores and products from canonical keys...")
    stores_df = generate_stores(inventory_df, rng=rng)
    products_df = generate_products(inventory_df, suppliers_df=suppliers_df, rng=rng)

    logging.info("Generating purchase orders from low-stock logic...")
    purchase_orders_df = generate_purchase_orders(
        inventory_df=inventory_df,
        products_df=products_df,
        suppliers_df=suppliers_df,
        rng=rng,
    )

    logging.info("Generating shipments from purchase orders...")
    shipments_df = generate_shipments(purchase_orders_df, rng=rng)

    logging.info("Running validation checks...")
    validate_key_integrity(
        inventory_df=inventory_df,
        stores_df=stores_df,
        products_df=products_df,
        suppliers_df=suppliers_df,
    )
    validate_purchase_order_relationships(
        purchase_orders_df=purchase_orders_df,
        stores_df=stores_df,
        products_df=products_df,
        suppliers_df=suppliers_df,
    )
    validate_shipment_relationships(
        shipments_df=shipments_df, purchase_orders_df=purchase_orders_df
    )
    validate_no_negative_generated_quantities(
        products_df=products_df,
        purchase_orders_df=purchase_orders_df,
        shipments_df=shipments_df,
    )
    validate_unique_keys(
        stores_df=stores_df,
        products_df=products_df,
        suppliers_df=suppliers_df,
        purchase_orders_df=purchase_orders_df,
        shipments_df=shipments_df,
    )

    inventory_bad_df = None
    issue_counts: dict[str, int] = {
        "null_product_id": 0,
        "null_store_id": 0,
        "negative_inventory_on_hand": 0,
        "negative_units_sold": 0,
        "non_positive_price": 0,
        "future_event_date": 0,
        "duplicate_logical_key_row": 0,
    }
    if args.bad_record_rate > 0:
        logging.info("Injecting bad records with rate %.4f", args.bad_record_rate)
        inventory_bad_df, issue_counts = inject_bad_records(
            clean_df=inventory_df,
            bad_record_rate=args.bad_record_rate,
            rng=rng,
        )

    write_outputs(
        output_path=output_path,
        inventory_clean_df=inventory_df,
        stores_df=stores_df,
        products_df=products_df,
        suppliers_df=suppliers_df,
        purchase_orders_df=purchase_orders_df,
        shipments_df=shipments_df,
        inventory_bad_df=inventory_bad_df,
    )

    summary = {
        "input_row_count": int(input_row_count),
        "clean_row_count": int(len(inventory_df)),
        "number_of_stores": int(len(stores_df)),
        "number_of_unique_stores": int(stores_df["store_id"].nunique()),
        "number_of_products": int(len(products_df)),
        "number_of_unique_products": int(products_df["product_id"].nunique()),
        "number_of_suppliers": int(len(suppliers_df)),
        "number_of_purchase_orders": int(len(purchase_orders_df)),
        "number_of_shipments": int(len(shipments_df)),
        "duplicate_store_id_count": int(stores_df["store_id"].duplicated().sum()),
        "duplicate_product_id_count": int(products_df["product_id"].duplicated().sum()),
        "bad_record_rate": float(args.bad_record_rate),
        "injected_issue_counts": issue_counts,
        "random_seed": int(args.random_seed),
        "generated_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }
    write_generation_summary(output_path=output_path, summary=summary)

    logging.info("Generation complete. Outputs written to %s", output_path)
    logging.info(
        "Counts | rows=%d stores=%d products=%d suppliers=%d pos=%d shipments=%d",
        len(inventory_df),
        len(stores_df),
        len(products_df),
        len(suppliers_df),
        len(purchase_orders_df),
        len(shipments_df),
    )


if __name__ == "__main__":
    main()
