MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    customer_id AS customer__customer_id,
    person_id AS customer__person_id,
    store_id AS customer__store_id,
    territory_id AS customer__territory_id,
    account_number AS customer__account_number,
    modified_date AS customer__modified_date,
    rowguid AS customer__rowguid,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS customer__record_loaded_at
  FROM bronze.raw__adventure_works__customers
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY customer__customer_id ORDER BY customer__record_loaded_at) AS customer__record_version,
    CASE
      WHEN customer__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE customer__record_loaded_at
    END AS customer__record_valid_from,
    COALESCE(
      LEAD(customer__record_loaded_at) OVER (PARTITION BY customer__customer_id ORDER BY customer__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS customer__record_valid_to,
    customer__record_valid_to = @max_ts::TIMESTAMP AS customer__is_current_record,
    CASE
      WHEN customer__is_current_record
      THEN customer__record_loaded_at
      ELSE customer__record_valid_to
    END AS customer__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'customer|adventure_works|',
      customer__customer_id,
      '~epoch|valid_from|',
      customer__record_valid_from
    ) AS _pit_hook__customer,
    CONCAT('customer|adventure_works|', customer__customer_id) AS _hook__customer,
    CONCAT('person|adventure_works|', customer__person_id) AS _hook__person,
    CONCAT('store|adventure_works|', customer__store_id) AS _hook__store,
    CONCAT('territory|adventure_works|', customer__territory_id) AS _hook__territory,
    *
  FROM validity
)
SELECT
  _pit_hook__customer::BLOB,
  _hook__customer::BLOB,
  _hook__person::BLOB,
  _hook__store::BLOB,
  _hook__territory::BLOB,
  customer__customer_id::VARCHAR,
  customer__person_id::VARCHAR,
  customer__store_id::VARCHAR,
  customer__territory_id::VARCHAR,
  customer__account_number::VARCHAR,
  customer__modified_date::VARCHAR,
  customer__rowguid::VARCHAR,
  customer__record_loaded_at::TIMESTAMP,
  customer__record_version::INT,
  customer__record_valid_from::TIMESTAMP,
  customer__record_valid_to::TIMESTAMP,
  customer__is_current_record::BOOLEAN,
  customer__record_updated_at::TIMESTAMP
FROM hooks