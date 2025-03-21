MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__product_model_illustration, _pit_hook__reference__illustration, _pit_hook__reference__product_model),
  description 'Bridge viewpoint of product_model_illustration data: Cross-reference table mapping product models and illustrations.',
  column_descriptions (
    _pit_hook__product_model_illustration = 'Point-in-time hook for product_model_illustration',
    _pit_hook__reference__illustration = 'Point-in-time hook for illustration reference',
    _pit_hook__reference__product_model = 'Point-in-time hook for product_model reference',
    _hook__product_model_illustration = 'Primary hook to product_model_illustration',
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
    'product_model_illustrations' AS peripheral,
    _pit_hook__product_model_illustration,
    _hook__product_model_illustration,
    _hook__reference__illustration,
    _hook__reference__product_model,
    product_model_illustration__record_loaded_at AS bridge__record_loaded_at,
    product_model_illustration__record_updated_at AS bridge__record_updated_at,
    product_model_illustration__record_valid_from AS bridge__record_valid_from,
    product_model_illustration__record_valid_to AS bridge__record_valid_to,
    product_model_illustration__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__product_model_illustrations
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__product_model_illustration,
    bridge__illustrations._pit_hook__reference__illustration,
    bridge__product_models._pit_hook__reference__product_model,
    cte__bridge._hook__product_model_illustration,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        bridge__illustrations.bridge__record_loaded_at,
        bridge__product_models.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        bridge__illustrations.bridge__record_updated_at,
        bridge__product_models.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        bridge__illustrations.bridge__record_valid_from,
        bridge__product_models.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        bridge__illustrations.bridge__record_valid_to,
        bridge__product_models.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record,
          bridge__illustrations.bridge__is_current_record,
          bridge__product_models.bridge__is_current_record
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN dar__staging.bridge__illustrations
  ON cte__bridge._hook__reference__illustration = bridge__illustrations._hook__reference__illustration
  AND cte__bridge.bridge__record_valid_from >= bridge__illustrations.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__illustrations.bridge__record_valid_to
  LEFT JOIN dar__staging.bridge__product_models
  ON cte__bridge._hook__reference__product_model = bridge__product_models._hook__reference__product_model
  AND cte__bridge.bridge__record_valid_from >= bridge__product_models.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__product_models.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      'peripheral|'||peripheral,
      'epoch__valid_from|'||bridge__record_valid_from,
      _pit_hook__product_model_illustration::TEXT,
      _pit_hook__reference__illustration::TEXT,
      _pit_hook__reference__product_model::TEXT
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__product_model_illustration::BLOB,
  _pit_hook__reference__illustration::BLOB,
  _pit_hook__reference__product_model::BLOB,
  _hook__product_model_illustration::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts