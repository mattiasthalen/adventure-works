MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__currency),
  description 'Business viewpoint of currencies data: Lookup table containing standard ISO currencies.',
  column_descriptions (
    currency__currency_code = 'The ISO code for the Currency.',
    currency__name = 'Currency name.',
    currency__modified_date = 'Date when this record was last modified',
    currency__record_loaded_at = 'Timestamp when this record was loaded into the system',
    currency__record_updated_at = 'Timestamp when this record was last updated',
    currency__record_version = 'Version number for this record',
    currency__record_valid_from = 'Timestamp from which this record version is valid',
    currency__record_valid_to = 'Timestamp until which this record version is valid',
    currency__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__currency,
    currency__currency_code,
    currency__name,
    currency__modified_date,
    currency__record_loaded_at,
    currency__record_updated_at,
    currency__record_version,
    currency__record_valid_from,
    currency__record_valid_to,
    currency__is_current_record
  FROM dab.bag__adventure_works__currencies
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__currency,
    'N/A' AS currency__currency_code,
    'N/A' AS currency__name,
    NULL AS currency__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS currency__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS currency__record_updated_at,
    0 AS currency__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS currency__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS currency__record_valid_to,
    TRUE AS currency__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__currency::BLOB,
  currency__currency_code::TEXT,
  currency__name::TEXT,
  currency__modified_date::DATE,
  currency__record_loaded_at::TIMESTAMP,
  currency__record_updated_at::TIMESTAMP,
  currency__record_version::TEXT,
  currency__record_valid_from::TIMESTAMP,
  currency__record_valid_to::TIMESTAMP,
  currency__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.currencies TO './export/dar/currencies.parquet' (FORMAT parquet, COMPRESSION zstd)
);