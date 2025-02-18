import os
import polars as pl

gold_path = "./lakehouse/gold"

bridge_df = pl.read_delta(f"{gold_path}/_bridge__as_of_event")
customers_df = pl.read_delta(f"{gold_path}/customers")
sales_order_headers_df = pl.read_delta(f"{gold_path}/sales_order_headers")

joined_df = bridge_df.join(
    customers_df,
    on="_pit_hook__customer",
    how="left"
).join(
    sales_order_headers_df,
    on="_pit_hook__sales_order",
    how="left"
)

orders_placed = (
    joined_df.select(
        pl.col("sales_order__order_date").alias("date"),
        "sales_order__sales_order_number"
    )
    .filter(pl.col("date").is_not_null())
    .group_by("date")
    .agg(pl.col("sales_order__sales_order_number").n_unique().alias("orders_placed"))
)

orders_due = (
    joined_df.select(
        pl.col("sales_order__due_date").alias("date"),
        "sales_order__sales_order_number"
    )
    .filter(pl.col("date").is_not_null())
    .group_by("date")
    .agg(pl.col("sales_order__sales_order_number").n_unique().alias("orders_due"))
)

orders_shipped = (
    joined_df.select(
        pl.col("sales_order__ship_date").alias("date"),
        "sales_order__sales_order_number"
    )
    .filter(pl.col("date").is_not_null())
    .group_by("date")
    .agg(pl.col("sales_order__sales_order_number").n_unique().alias("orders_shipped"))
)

orders_shipped_on_time = (
    joined_df.filter(
        pl.col("sales_order__ship_date") <= pl.col("sales_order__due_date")
    )
    .select(
        pl.col("sales_order__due_date").alias("date"),
        "sales_order__sales_order_number"
    )
    .filter(
        pl.col("date").is_not_null()
    )
    .group_by("date")
    .agg(
        pl.col("sales_order__sales_order_number").n_unique().alias("orders_shipped_on_time")
    )
)

orders_due_lead_time = (
    joined_df.select(
        pl.col("sales_order__order_date").alias("date"),
        (pl.col("sales_order__due_date").cast(pl.Date) - pl.col("sales_order__order_date").cast(pl.Date)).alias("sales_order_due_lead_time")
    )
    .filter(pl.col("date").is_not_null())
    .group_by("date")
    .agg(pl.col("sales_order_due_lead_time").mean().alias("mean_sales_order_due_lead_time"))
)

orders_shipping_capacity = (
    joined_df.select(
        pl.col("sales_order__order_date").alias("date"),
        (pl.col("sales_order__ship_date").cast(pl.Date) - pl.col("sales_order__order_date").cast(pl.Date)).alias("sales_order_shipping_lead_time")
    )
    .filter(pl.col("date").is_not_null())
    .group_by("date")
    .agg(pl.col("sales_order_shipping_lead_time").quantile(0.9).alias("sales_order_shipping_capacity"))
)

# Combine all measures into one table using the complete date range
measures_df = (
    orders_placed
    .join(orders_due, on="date", how="left")
    .join(orders_shipped, on="date", how="left")
    .join(orders_shipped_on_time, on="date", how="left")
    .join(orders_due_lead_time, on="date", how="left")
    .join(orders_shipping_capacity, on="date", how="left")
    .fill_null(0)
    .sort("date")
)

measures_df