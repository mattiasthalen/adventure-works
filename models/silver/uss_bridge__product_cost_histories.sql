MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__product)
);

WITH cte__bridge AS (
  SELECT
    'product_cost_histories' AS peripheral,
    _pit_hook__product,
    _hook__product,
    _hook__epoch__date,
    measure__product_cost_histories_started,
    measure__product_cost_histories_finished,
    measure__product_cost_histories_modified,
    product_cost_history__record_loaded_at AS bridge__record_loaded_at,
    product_cost_history__record_updated_at AS bridge__record_updated_at,
    product_cost_history__record_valid_from AS bridge__record_valid_from,
    product_cost_history__record_valid_to AS bridge__record_valid_to,
    product_cost_history__is_current_record AS bridge__is_current_record
  FROM silver.bag__adventure_works__product_cost_histories
  LEFT JOIN silver.measure__adventure_works__product_cost_histories USING (_pit_hook__product)
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from'||bridge__record_valid_from,
      _pit_hook__product
    ) AS _pit_hook__bridge
  FROM cte__bridge
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__product::BLOB,
  _hook__product::BLOB,
  _hook__epoch__date::BLOB,
  measure__product_cost_histories_started::INT,
  measure__product_cost_histories_finished::INT,
  measure__product_cost_histories_modified::INT,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts