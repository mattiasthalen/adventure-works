MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__reference__product_model),
  description 'Bridge viewpoint of product_model reference data: Product model classification.',
  column_descriptions (
    _pit_hook__reference__product_model = 'Point-in-time hook for product_model reference',
    _hook__reference__product_model = 'Primary hook to product_model reference',
    peripheral = 'Name of the peripheral this bridge represents',
    _pit_hook__bridge = 'Unified bridge point-in-time hook that combines peripheral and validity period',
    bridge__record_loaded_at = 'Timestamp when this bridge record was loaded',
    bridge__record_updated_at = 'Timestamp when this bridge record was last updated',
    bridge__record_valid_from = 'Timestamp from which this bridge record is valid',
    bridge__record_valid_to = 'Timestamp until which this bridge record is valid',
    bridge__is_current_record = 'Flag indicating if this is the current valid version of the bridge record'
  )
);

WITH cte__bridge AS (
  SELECT
    'product_models' AS peripheral,
    _pit_hook__reference__product_model,
    _hook__reference__product_model,
    product_model__record_loaded_at AS bridge__record_loaded_at,
    product_model__record_updated_at AS bridge__record_updated_at,
    product_model__record_valid_from AS bridge__record_valid_from,
    product_model__record_valid_to AS bridge__record_valid_to,
    product_model__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__product_models
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      'peripheral|'||peripheral,
      'epoch__valid_from|'||bridge__record_valid_from,
      _pit_hook__reference__product_model::TEXT
    ) AS _pit_hook__bridge
  FROM cte__bridge
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__reference__product_model::BLOB,
  _hook__reference__product_model::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts