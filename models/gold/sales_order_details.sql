MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column sales_order_detail__record_updated_at
  ),
  enabled TRUE
);

SELECT
  *
  EXCLUDE (_hook__product, _hook__sales_order, _hook__sales_order_detail, _hook__special_offer)
FROM silver.bag__adventure_works__sales_order_details
WHERE 1 = 1
AND sales_order_detail__record_updated_at BETWEEN @start_ts AND @end_ts