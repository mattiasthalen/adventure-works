MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__location),
  description 'Business viewpoint of locations data: Product inventory and manufacturing locations.',
  column_descriptions (
    location__location_id = 'Primary key for Location records.',
    location__name = 'Location description.',
    location__cost_rate = 'Standard hourly cost of the manufacturing location.',
    location__availability = 'Work capacity (in hours) of the manufacturing location.',
    location__modified_date = 'Date when this record was last modified',
    location__record_loaded_at = 'Timestamp when this record was loaded into the system',
    location__record_updated_at = 'Timestamp when this record was last updated',
    location__record_version = 'Version number for this record',
    location__record_valid_from = 'Timestamp from which this record version is valid',
    location__record_valid_to = 'Timestamp until which this record version is valid',
    location__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__reference__location,
    location__location_id,
    location__name,
    location__cost_rate,
    location__availability,
    location__modified_date,
    location__record_loaded_at,
    location__record_updated_at,
    location__record_version,
    location__record_valid_from,
    location__record_valid_to,
    location__is_current_record
  FROM dab.bag__adventure_works__locations
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__reference__location,
    NULL AS location__location_id,
    'N/A' AS location__name,
    NULL AS location__cost_rate,
    NULL AS location__availability,
    NULL AS location__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS location__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS location__record_updated_at,
    0 AS location__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS location__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS location__record_valid_to,
    TRUE AS location__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__reference__location::BLOB,
  location__location_id::BIGINT,
  location__name::TEXT,
  location__cost_rate::DOUBLE,
  location__availability::DOUBLE,
  location__modified_date::DATE,
  location__record_loaded_at::TIMESTAMP,
  location__record_updated_at::TIMESTAMP,
  location__record_version::TEXT,
  location__record_valid_from::TIMESTAMP,
  location__record_valid_to::TIMESTAMP,
  location__is_current_record::BOOLEAN
FROM cte__final