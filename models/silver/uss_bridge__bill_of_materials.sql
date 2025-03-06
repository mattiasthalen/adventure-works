MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__bill_of_materials, _pit_hook__reference__unit_measure)
);

WITH cte__bridge AS (
  SELECT
    'bill_of_materials' AS peripheral,
    _pit_hook__bill_of_materials,
    _hook__bill_of_materials,
    _hook__reference__unit_measure,
    bill_of_material__record_loaded_at AS bridge__record_loaded_at,
    bill_of_material__record_updated_at AS bridge__record_updated_at,
    bill_of_material__record_valid_from AS bridge__record_valid_from,
    bill_of_material__record_valid_to AS bridge__record_valid_to,
    bill_of_material__is_current_record AS bridge__is_current_record
  FROM silver.bag__adventure_works__bill_of_materials
),
cte__pit_lookup AS (
  SELECT
    cte__bridge.peripheral,
    cte__bridge._pit_hook__bill_of_materials,
    uss_bridge__unit_measures._pit_hook__reference__unit_measure,
    cte__bridge._hook__bill_of_materials,
    GREATEST(
        cte__bridge.bridge__record_loaded_at,
        uss_bridge__unit_measures.bridge__record_loaded_at
    ) AS bridge__record_loaded_at,
    GREATEST(
        cte__bridge.bridge__record_updated_at,
        uss_bridge__unit_measures.bridge__record_updated_at
    ) AS bridge__record_updated_at,
    GREATEST(
        cte__bridge.bridge__record_valid_from,
        uss_bridge__unit_measures.bridge__record_valid_from
    ) AS bridge__record_valid_from,
    LEAST(
        cte__bridge.bridge__record_valid_to,
        uss_bridge__unit_measures.bridge__record_valid_to
    ) AS bridge__record_valid_to,
    LIST_HAS_ALL(
      ARRAY[True],
        ARRAY[
          cte__bridge.bridge__is_current_record::BOOL,
          uss_bridge__unit_measures.bridge__is_current_record::BOOL
        ]
    ) AS bridge__is_current_record
  FROM cte__bridge
  LEFT JOIN silver.uss_bridge__unit_measures
  ON cte__bridge._hook__reference__unit_measure = uss_bridge__unit_measures._hook__reference__unit_measure
  AND cte__bridge.bridge__record_valid_from >= uss_bridge__unit_measures.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= uss_bridge__unit_measures.bridge__record_valid_to
),
cte__bridge_pit_hook AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      peripheral,
      'epoch__valid_from'||bridge__record_valid_from,
      _pit_hook__bill_of_materials,
      _pit_hook__reference__unit_measure
    ) AS _pit_hook__bridge
  FROM cte__pit_lookup
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__bill_of_materials::BLOB,
  _pit_hook__reference__unit_measure::BLOB,
  _hook__bill_of_materials::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts