MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit_hook__bill_of_materials),
  references (_hook__product__assembly, _hook__product__component, _hook__reference__unit_measure)
);

SELECT
  'bill_of_materials' AS peripheral,
  _hook__bill_of_materials::BLOB,
  _hook__product__assembly::BLOB,
  _hook__product__component::BLOB,
  _hook__reference__unit_measure::BLOB,
  bill_of_material__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  bill_of_material__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  bill_of_material__record_version::TEXT AS bridge__record_version,
  bill_of_material__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  bill_of_material__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  bill_of_material__is_current_record::TEXT AS bridge__is_current_record
FROM silver.bag__adventure_works__bill_of_materials
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts