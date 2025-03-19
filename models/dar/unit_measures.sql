MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__unit_measure),
  description 'Business viewpoint of unit_measures data: Unit of measure lookup table.',
  column_descriptions (
    unit_measure__unit_measure_code = 'Primary key.',
    unit_measure__name = 'Unit of measure description.',
    unit_measure__modified_date = 'Date when this record was last modified',
    unit_measure__record_loaded_at = 'Timestamp when this record was loaded into the system',
    unit_measure__record_updated_at = 'Timestamp when this record was last updated',
    unit_measure__record_version = 'Version number for this record',
    unit_measure__record_valid_from = 'Timestamp from which this record version is valid',
    unit_measure__record_valid_to = 'Timestamp until which this record version is valid',
    unit_measure__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__reference__unit_measure,
    unit_measure__unit_measure_code,
    unit_measure__name,
    unit_measure__modified_date,
    unit_measure__record_loaded_at,
    unit_measure__record_updated_at,
    unit_measure__record_version,
    unit_measure__record_valid_from,
    unit_measure__record_valid_to,
    unit_measure__is_current_record
  FROM dab.bag__adventure_works__unit_measures
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__reference__unit_measure,
    'N/A' AS unit_measure__unit_measure_code,
    'N/A' AS unit_measure__name,
    NULL AS unit_measure__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS unit_measure__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS unit_measure__record_updated_at,
    0 AS unit_measure__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS unit_measure__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS unit_measure__record_valid_to,
    TRUE AS unit_measure__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__reference__unit_measure::BLOB,
  unit_measure__unit_measure_code::TEXT,
  unit_measure__name::TEXT,
  unit_measure__modified_date::DATE,
  unit_measure__record_loaded_at::TIMESTAMP,
  unit_measure__record_updated_at::TIMESTAMP,
  unit_measure__record_version::TEXT,
  unit_measure__record_valid_from::TIMESTAMP,
  unit_measure__record_valid_to::TIMESTAMP,
  unit_measure__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.unit_measures TO './export/dar/unit_measures.parquet' (FORMAT parquet, COMPRESSION zstd)
);