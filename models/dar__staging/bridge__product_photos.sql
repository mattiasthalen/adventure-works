MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__reference__product_photo)
);

WITH cte__bridge AS (
  SELECT
    'product_photos' AS peripheral,
    _pit_hook__reference__product_photo,
    _hook__reference__product_photo,
    product_photo__record_loaded_at AS bridge__record_loaded_at,
    product_photo__record_updated_at AS bridge__record_updated_at,
    product_photo__record_valid_from AS bridge__record_valid_from,
    product_photo__record_valid_to AS bridge__record_valid_to,
    product_photo__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__product_photos
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from|'||bridge__record_valid_from,
      _pit_hook__reference__product_photo::TEXT
    ) AS _pit_hook__bridge
  FROM cte__bridge
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__reference__product_photo::BLOB,
  _hook__reference__product_photo::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts