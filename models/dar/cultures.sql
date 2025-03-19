MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__culture),
  description 'Business viewpoint of cultures data: Lookup table containing the languages in which some AdventureWorks data is stored.',
  column_descriptions (
    culture__culture_id = 'Primary key for Culture records.',
    culture__name = 'Culture description.',
    culture__modified_date = 'Date when this record was last modified',
    culture__record_loaded_at = 'Timestamp when this record was loaded into the system',
    culture__record_updated_at = 'Timestamp when this record was last updated',
    culture__record_version = 'Version number for this record',
    culture__record_valid_from = 'Timestamp from which this record version is valid',
    culture__record_valid_to = 'Timestamp until which this record version is valid',
    culture__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__reference__culture,
    culture__culture_id,
    culture__name,
    culture__modified_date,
    culture__record_loaded_at,
    culture__record_updated_at,
    culture__record_version,
    culture__record_valid_from,
    culture__record_valid_to,
    culture__is_current_record
  FROM dab.bag__adventure_works__cultures
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__reference__culture,
    'N/A' AS culture__culture_id,
    'N/A' AS culture__name,
    NULL AS culture__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS culture__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS culture__record_updated_at,
    0 AS culture__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS culture__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS culture__record_valid_to,
    TRUE AS culture__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__reference__culture::BLOB,
  culture__culture_id::TEXT,
  culture__name::TEXT,
  culture__modified_date::DATE,
  culture__record_loaded_at::TIMESTAMP,
  culture__record_updated_at::TIMESTAMP,
  culture__record_version::TEXT,
  culture__record_valid_from::TIMESTAMP,
  culture__record_valid_to::TIMESTAMP,
  culture__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.cultures TO './export/dar/cultures.parquet' (FORMAT parquet, COMPRESSION zstd)
);