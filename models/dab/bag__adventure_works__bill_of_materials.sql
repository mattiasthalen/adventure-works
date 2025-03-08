MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bill_of_materials,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__bill_of_materials, _hook__bill_of_materials),
  references (_hook__product__assembly, _hook__product__component, _hook__reference__unit_measure)
);

WITH staging AS (
  SELECT
    bill_of_materials_id AS bill_of_material__bill_of_materials_id,
    product_assembly_id AS bill_of_material__product_assembly_id,
    component_id AS bill_of_material__component_id,
    start_date AS bill_of_material__start_date,
    unit_measure_code AS bill_of_material__unit_measure_code,
    bomlevel AS bill_of_material__bomlevel,
    per_assembly_qty AS bill_of_material__per_assembly_qty,
    end_date AS bill_of_material__end_date,
    modified_date AS bill_of_material__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS bill_of_material__record_loaded_at
  FROM das.raw__adventure_works__bill_of_materials
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY bill_of_material__bill_of_materials_id ORDER BY bill_of_material__record_loaded_at) AS bill_of_material__record_version,
    CASE
      WHEN bill_of_material__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE bill_of_material__record_loaded_at
    END AS bill_of_material__record_valid_from,
    COALESCE(
      LEAD(bill_of_material__record_loaded_at) OVER (PARTITION BY bill_of_material__bill_of_materials_id ORDER BY bill_of_material__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS bill_of_material__record_valid_to,
    bill_of_material__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bill_of_material__is_current_record,
    CASE
      WHEN bill_of_material__is_current_record
      THEN bill_of_material__record_loaded_at
      ELSE bill_of_material__record_valid_to
    END AS bill_of_material__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'product__adventure_works|',
      bill_of_material__bill_of_materials_id,
      '~epoch|valid_from|',
      bill_of_material__record_valid_from
    )::BLOB AS _pit_hook__bill_of_materials,
    CONCAT('product__adventure_works|', bill_of_material__bill_of_materials_id) AS _hook__bill_of_materials,
    CONCAT('product__adventure_works|', bill_of_material__product_assembly_id) AS _hook__product__assembly,
    CONCAT('product__adventure_works|', bill_of_material__component_id) AS _hook__product__component,
    CONCAT('reference__unit_measure__adventure_works|', bill_of_material__unit_measure_code) AS _hook__reference__unit_measure,
    *
  FROM validity
)
SELECT
  _pit_hook__bill_of_materials::BLOB,
  _hook__bill_of_materials::BLOB,
  _hook__product__assembly::BLOB,
  _hook__product__component::BLOB,
  _hook__reference__unit_measure::BLOB,
  bill_of_material__bill_of_materials_id::BIGINT,
  bill_of_material__product_assembly_id::BIGINT,
  bill_of_material__component_id::BIGINT,
  bill_of_material__start_date::DATE,
  bill_of_material__unit_measure_code::TEXT,
  bill_of_material__bomlevel::BIGINT,
  bill_of_material__per_assembly_qty::DOUBLE,
  bill_of_material__end_date::DATE,
  bill_of_material__modified_date::DATE,
  bill_of_material__record_loaded_at::TIMESTAMP,
  bill_of_material__record_updated_at::TIMESTAMP,
  bill_of_material__record_version::TEXT,
  bill_of_material__record_valid_from::TIMESTAMP,
  bill_of_material__record_valid_to::TIMESTAMP,
  bill_of_material__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND bill_of_material__record_updated_at BETWEEN @start_ts AND @end_ts