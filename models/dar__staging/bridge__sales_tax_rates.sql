MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__reference__country_region, _pit_hook__reference__sales_tax_rate, _pit_hook__reference__state_province, _pit_hook__territory__sales),
  description 'Bridge viewpoint of sales_tax_rate reference data: Tax rate lookup table.',
  column_descriptions (
    _pit_hook__reference__country_region = 'Point-in-time hook for country_region reference',
    _pit_hook__reference__sales_tax_rate = 'Point-in-time hook for sales_tax_rate reference',
    _pit_hook__reference__state_province = 'Point-in-time hook for state_province reference',
    _pit_hook__territory__sales = 'Point-in-time hook for sales territory',
    _hook__reference__sales_tax_rate = 'Primary hook to sales_tax_rate reference',
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
    'sales_tax_rates' AS peripheral,
    _pit_hook__reference__sales_tax_rate,
    _hook__reference__sales_tax_rate,
    _hook__reference__state_province,
    sales_tax_rate__record_loaded_at AS bridge__record_loaded_at,
    sales_tax_rate__record_updated_at AS bridge__record_updated_at,
    sales_tax_rate__record_valid_from AS bridge__record_valid_from,
    sales_tax_rate__record_valid_to AS bridge__record_valid_to,
    sales_tax_rate__is_current_record AS bridge__is_current_record
  FROM dab.bag__adventure_works__sales_tax_rates
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__reference__sales_tax_rate,
    bridge__state_provinces._pit_hook__reference__country_region,
    bridge__state_provinces._pit_hook__reference__state_province,
    bridge__state_provinces._pit_hook__territory__sales,
    cte__bridge._hook__reference__sales_tax_rate,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        bridge__state_provinces.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        bridge__state_provinces.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        bridge__state_provinces.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        bridge__state_provinces.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record,
          bridge__state_provinces.bridge__is_current_record
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN dar__staging.bridge__state_provinces
  ON cte__bridge._hook__reference__state_province = bridge__state_provinces._hook__reference__state_province
  AND cte__bridge.bridge__record_valid_from >= bridge__state_provinces.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= bridge__state_provinces.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      'peripheral|'||peripheral,
      'epoch__valid_from|'||bridge__record_valid_from,
      _pit_hook__reference__country_region::TEXT,
      _pit_hook__reference__sales_tax_rate::TEXT,
      _pit_hook__reference__state_province::TEXT,
      _pit_hook__territory__sales::TEXT
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__reference__country_region::BLOB,
  _pit_hook__reference__sales_tax_rate::BLOB,
  _pit_hook__reference__state_province::BLOB,
  _pit_hook__territory__sales::BLOB,
  _hook__reference__sales_tax_rate::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts