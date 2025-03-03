MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    location_id AS location__location_id,
    product_id AS location__product_id,
    bin AS location__bin,
    modified_date AS location__modified_date,
    quantity AS location__quantity,
    rowguid AS location__rowguid,
    shelf AS location__shelf,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS location__record_loaded_at
  FROM bronze.raw__adventure_works__product_inventories
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY location__location_id ORDER BY location__record_loaded_at) AS location__record_version,
    CASE
      WHEN location__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE location__record_loaded_at
    END AS location__record_valid_from,
    COALESCE(
      LEAD(location__record_loaded_at) OVER (PARTITION BY location__location_id ORDER BY location__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS location__record_valid_to,
    location__record_valid_to = @max_ts::TIMESTAMP AS location__is_current_record,
    CASE
      WHEN location__is_current_record
      THEN location__record_loaded_at
      ELSE location__record_valid_to
    END AS location__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'location|adventure_works|',
      location__location_id,
      '~epoch|valid_from|',
      location__record_valid_from
    ) AS _pit_hook__location,
    CONCAT('location|adventure_works|', location__location_id) AS _hook__location,
    CONCAT('product|adventure_works|', location__product_id) AS _hook__product,
    *
  FROM validity
)
SELECT
  _pit_hook__location::BLOB,
  _hook__location::BLOB,
  _hook__product::BLOB,
  location__location_id::VARCHAR,
  location__product_id::VARCHAR,
  location__bin::VARCHAR,
  location__modified_date::VARCHAR,
  location__quantity::VARCHAR,
  location__rowguid::VARCHAR,
  location__shelf::VARCHAR,
  location__record_loaded_at::TIMESTAMP,
  location__record_version::INT,
  location__record_valid_from::TIMESTAMP,
  location__record_valid_to::TIMESTAMP,
  location__is_current_record::BOOLEAN,
  location__record_updated_at::TIMESTAMP
FROM hooks