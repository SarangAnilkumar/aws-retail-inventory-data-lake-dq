# Data Model (MVP)

## Important Modeling Rule

Athena external tables do **not** enforce primary key or foreign key constraints.

- Keys in this project are **logical keys** for documentation and testing.
- Relationship integrity is validated in PySpark and DQ checks, not enforced by Athena.

## Tables

### Curated

- `inventory_daily` (fact): daily inventory/sales by product and store.
- `stores` (dimension): distinct stores generated from canonical source keys.
- `products` (dimension): distinct products generated from canonical source keys.
- `suppliers` (dimension): synthetic supplier table, assigned to products.
- `purchase_orders` (fact): synthetic replenishment orders.
- `shipments` (fact): synthetic deliveries generated from purchase orders.

### DQ Monitoring

- `dq_rule_results`: per-rule run output.
- `dq_table_scores`: aggregated table-level quality scores.
- `quarantine_records`: failed rows with failure reason and run metadata.

## Logical Keys

- `inventory_daily`: (`event_date`, `store_id`, `product_id`)
- `stores`: (`store_id`)
- `products`: (`product_id`)
- `suppliers`: (`supplier_id`)
- `purchase_orders`: (`po_id`)
- `shipments`: (`shipment_id`)

## Key Relationships (Validated in PySpark)

- `inventory_daily.product_id` must exist in `products.product_id`
- `inventory_daily.store_id` must exist in `stores.store_id`
- `products.supplier_id` must exist in `suppliers.supplier_id`
- `purchase_orders.supplier_id` must exist in `suppliers.supplier_id`
- `shipments.po_id` must exist in `purchase_orders.po_id`

## Partition Strategy (MVP)

- `inventory_daily`: partition by `year`, `month`
- `purchase_orders`: partition by `order_year`, `order_month`
- `shipments`: partition by `received_year`, `received_month`

Do not partition by `product_id` in MVP.
