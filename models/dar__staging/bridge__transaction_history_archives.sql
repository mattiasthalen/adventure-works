MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__product, _pit_hook__product_category, _pit_hook__product_subcategory, _pit_hook__reference__product_model, _pit_hook__transaction_history_archive),
  description 'Bridge viewpoint of transaction_history_archive data: Transactions for previous years.',
  column_descriptions (
    _pit_hook__product = 'Point-in-time hook for product',
    _pit_hook__product_category = 'Point-in-time hook for product_category',
    _pit_hook__product_subcategory = 'Point-in-time hook for product_subcategory',
    _pit_hook__reference__product_model = 'Point-in-time hook for product_model reference',
    _pit_hook__transaction_history_archive = 'Point-in-time hook for transaction_history_archive',
    _hook__transaction_history_archive = 'Primary hook to transaction_history_archive',
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
    'transaction_history_archives' AS peripheral,
    _pit_hook__transaction_history_archive,
    _hook__transaction_history_archive,
    _hook__product,
    transaction_history_archive__record_loaded_at AS bridge__record_loaded_at,
    transaction_history_archive__record_updated_at AS bridge__record_updated_at,
    transaction_history_archive__record_valid_from AS bridge__record_valid_from,
    transaction_history_archive__record_valid_to AS bridge__record_valid_to,
    transaction_history_archive__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__transaction_history_archives
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__transaction_history_archive,
    bridge__products._pit_hook__product,
    bridge__products._pit_hook__product_category,
    bridge__products._pit_hook__product_subcategory,
    bridge__products._pit_hook__reference__product_model,
    cte__bridge._hook__transaction_history_archive,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        bridge__products.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        bridge__products.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        bridge__products.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        bridge__products.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record,
          bridge__products.bridge__is_current_record
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN dar__staging.bridge__products
  ON cte__bridge._hook__product = bridge__products._hook__product
  AND cte__bridge.bridge__record_valid_from >= bridge__products.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__products.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      'peripheral|'||peripheral,
      'epoch__valid_from|'||bridge__record_valid_from,
      _pit_hook__product::TEXT,
      _pit_hook__product_category::TEXT,
      _pit_hook__product_subcategory::TEXT,
      _pit_hook__reference__product_model::TEXT,
      _pit_hook__transaction_history_archive::TEXT
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__product::BLOB,
  _pit_hook__product_category::BLOB,
  _pit_hook__product_subcategory::BLOB,
  _pit_hook__reference__product_model::BLOB,
  _pit_hook__transaction_history_archive::BLOB,
  _hook__transaction_history_archive::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts