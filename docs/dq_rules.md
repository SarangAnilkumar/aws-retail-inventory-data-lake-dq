# Data Quality Rules (MVP Matrix)

## Execution Split

### Glue Data Quality (DQDL)

- `product_id IS NOT NULL` (`inventory_daily`)
- `store_id IS NOT NULL` (`inventory_daily`)
- `inventory_on_hand >= 0` (`inventory_daily`)
- `units_sold >= 0` (`inventory_daily`)
- `units_ordered >= 0` (`inventory_daily`)
- `price > 0` (`inventory_daily`)
- `event_date IS NOT NULL` (`inventory_daily`)
- `product_id IS NOT NULL` (`products`)
- `supplier_id IS NOT NULL` (`products`)
- `reorder_level >= 0` (`products`)
- `reorder_quantity > 0` (`products`)

### PySpark

- Duplicate logical key check for `inventory_daily`: (`event_date`, `store_id`, `product_id`)
- `available_stock = inventory_on_hand - units_sold`
- `stockout_flag = available_stock <= 0`
- `product_id` in `inventory_daily` exists in `products`
- `store_id` in `inventory_daily` exists in `stores`
- `supplier_id` in `products` exists in `suppliers`
- `supplier_id` in `purchase_orders` exists in `suppliers`
- `expected_delivery_date >= order_date`
- `order_qty > 0`
- No orphan `purchase_orders`
- No orphan `shipments`

### Athena SQL

- Latest partition freshness check
- Daily row-count anomaly check
- Failed rules by severity
- Table-level quality score trend
- Quarantine count by table and failure reason

## Fallback Logic

- Use Glue DQ for simple single-table checks.
- Use PySpark for cross-column and cross-table logic.
- Use Athena SQL for freshness and trend/aggregate checks.

## Notes

- DQDL files are under `dq_rules/`.
- Validate DQDL syntax against AWS Glue DQDL docs before deployment.
