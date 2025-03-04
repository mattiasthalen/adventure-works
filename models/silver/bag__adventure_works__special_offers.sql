MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column special_offer__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    special_offer_id AS special_offer__special_offer_id,
    category AS special_offer__category,
    description AS special_offer__description,
    discount_percentage AS special_offer__discount_percentage,
    end_date AS special_offer__end_date,
    maximum_quantity AS special_offer__maximum_quantity,
    minimum_quantity AS special_offer__minimum_quantity,
    modified_date AS special_offer__modified_date,
    rowguid AS special_offer__rowguid,
    start_date AS special_offer__start_date,
    type AS special_offer__type,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS special_offer__record_loaded_at
  FROM bronze.raw__adventure_works__special_offers
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY special_offer__special_offer_id ORDER BY special_offer__record_loaded_at) AS special_offer__record_version,
    CASE
      WHEN special_offer__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE special_offer__record_loaded_at
    END AS special_offer__record_valid_from,
    COALESCE(
      LEAD(special_offer__record_loaded_at) OVER (PARTITION BY special_offer__special_offer_id ORDER BY special_offer__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS special_offer__record_valid_to,
    special_offer__record_valid_to = @max_ts::TIMESTAMP AS special_offer__is_current_record,
    CASE
      WHEN special_offer__is_current_record
      THEN special_offer__record_loaded_at
      ELSE special_offer__record_valid_to
    END AS special_offer__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'special_offer|adventure_works|',
      special_offer__special_offer_id,
      '~epoch|valid_from|',
      special_offer__record_valid_from
    ) AS _pit_hook__special_offer,
    CONCAT('special_offer|adventure_works|', special_offer__special_offer_id) AS _hook__special_offer,
    *
  FROM validity
)
SELECT
  _pit_hook__special_offer::BLOB,
  _hook__special_offer::BLOB,
  special_offer__special_offer_id::BIGINT,
  special_offer__category::VARCHAR,
  special_offer__description::VARCHAR,
  special_offer__discount_percentage::DOUBLE,
  special_offer__end_date::VARCHAR,
  special_offer__maximum_quantity::BIGINT,
  special_offer__minimum_quantity::BIGINT,
  special_offer__modified_date::VARCHAR,
  special_offer__rowguid::VARCHAR,
  special_offer__start_date::VARCHAR,
  special_offer__type::VARCHAR,
  special_offer__record_loaded_at::TIMESTAMP,
  special_offer__record_updated_at::TIMESTAMP,
  special_offer__record_valid_from::TIMESTAMP,
  special_offer__record_valid_to::TIMESTAMP,
  special_offer__record_version::INT,
  special_offer__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND special_offer__record_updated_at BETWEEN @start_ts AND @end_ts