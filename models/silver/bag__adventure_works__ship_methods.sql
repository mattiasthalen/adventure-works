MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    ship_method_id AS ship_method__ship_method_id,
    modified_date AS ship_method__modified_date,
    name AS ship_method__name,
    rowguid AS ship_method__rowguid,
    ship_base AS ship_method__ship_base,
    ship_rate AS ship_method__ship_rate,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS ship_method__record_loaded_at
  FROM bronze.raw__adventure_works__ship_methods
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY ship_method__ship_method_id ORDER BY ship_method__record_loaded_at) AS ship_method__record_version,
    CASE
      WHEN ship_method__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE ship_method__record_loaded_at
    END AS ship_method__record_valid_from,
    COALESCE(
      LEAD(ship_method__record_loaded_at) OVER (PARTITION BY ship_method__ship_method_id ORDER BY ship_method__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS ship_method__record_valid_to,
    ship_method__record_valid_to = @max_ts::TIMESTAMP AS ship_method__is_current_record,
    CASE
      WHEN ship_method__is_current_record
      THEN ship_method__record_loaded_at
      ELSE ship_method__record_valid_to
    END AS ship_method__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'ship_method|adventure_works|',
      ship_method__ship_method_id,
      '~epoch|valid_from|',
      ship_method__record_valid_from
    ) AS _pit_hook__ship_method,
    CONCAT('ship_method|adventure_works|', ship_method__ship_method_id) AS _hook__ship_method,
    *
  FROM validity
)
SELECT
  _pit_hook__ship_method::BLOB,
  _hook__ship_method::BLOB,
  ship_method__ship_method_id::VARCHAR,
  ship_method__modified_date::VARCHAR,
  ship_method__name::VARCHAR,
  ship_method__rowguid::VARCHAR,
  ship_method__ship_base::VARCHAR,
  ship_method__ship_rate::VARCHAR,
  ship_method__record_loaded_at::TIMESTAMP,
  ship_method__record_version::INT,
  ship_method__record_valid_from::TIMESTAMP,
  ship_method__record_valid_to::TIMESTAMP,
  ship_method__is_current_record::BOOLEAN,
  ship_method__record_updated_at::TIMESTAMP
FROM hooks