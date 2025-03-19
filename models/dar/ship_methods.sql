MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__ship_method),
  description 'Business viewpoint of ship_methods data: Shipping company lookup table.',
  column_descriptions (
    ship_method__ship_method_id = 'Primary key for ShipMethod records.',
    ship_method__name = 'Shipping company name.',
    ship_method__ship_base = 'Minimum shipping charge.',
    ship_method__ship_rate = 'Shipping charge per pound.',
    ship_method__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    ship_method__modified_date = 'Date when this record was last modified',
    ship_method__record_loaded_at = 'Timestamp when this record was loaded into the system',
    ship_method__record_updated_at = 'Timestamp when this record was last updated',
    ship_method__record_version = 'Version number for this record',
    ship_method__record_valid_from = 'Timestamp from which this record version is valid',
    ship_method__record_valid_to = 'Timestamp until which this record version is valid',
    ship_method__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__ship_method,
    ship_method__ship_method_id,
    ship_method__name,
    ship_method__ship_base,
    ship_method__ship_rate,
    ship_method__rowguid,
    ship_method__modified_date,
    ship_method__record_loaded_at,
    ship_method__record_updated_at,
    ship_method__record_version,
    ship_method__record_valid_from,
    ship_method__record_valid_to,
    ship_method__is_current_record
  FROM dab.bag__adventure_works__ship_methods
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__ship_method,
    NULL AS ship_method__ship_method_id,
    'N/A' AS ship_method__name,
    NULL AS ship_method__ship_base,
    NULL AS ship_method__ship_rate,
    'N/A' AS ship_method__rowguid,
    NULL AS ship_method__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS ship_method__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS ship_method__record_updated_at,
    0 AS ship_method__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS ship_method__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS ship_method__record_valid_to,
    TRUE AS ship_method__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__ship_method::BLOB,
  ship_method__ship_method_id::BIGINT,
  ship_method__name::TEXT,
  ship_method__ship_base::DOUBLE,
  ship_method__ship_rate::DOUBLE,
  ship_method__rowguid::TEXT,
  ship_method__modified_date::DATE,
  ship_method__record_loaded_at::TIMESTAMP,
  ship_method__record_updated_at::TIMESTAMP,
  ship_method__record_version::TEXT,
  ship_method__record_valid_from::TIMESTAMP,
  ship_method__record_valid_to::TIMESTAMP,
  ship_method__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.ship_methods TO './export/dar/ship_methods.parquet' (FORMAT parquet, COMPRESSION zstd)
);