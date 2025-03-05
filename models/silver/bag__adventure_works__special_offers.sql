MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column special_offer__record_updated_at
  ),
  tags hook,
  grain (_pit_hook__reference__special_offer, _hook__reference__special_offer)
);

WITH staging AS (
  SELECT
    special_offer_id AS special_offer__special_offer_id,
    description AS special_offer__description,
    discount_percentage AS special_offer__discount_percentage,
    type AS special_offer__type,
    category AS special_offer__category,
    start_date AS special_offer__start_date,
    end_date AS special_offer__end_date,
    minimum_quantity AS special_offer__minimum_quantity,
    rowguid AS special_offer__rowguid,
    maximum_quantity AS special_offer__maximum_quantity,
    modified_date AS special_offer__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS special_offer__record_loaded_at
  FROM bronze.raw__adventure_works__special_offers
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY special_offer__special_offer_id ORDER BY special_offer__record_loaded_at) AS special_offer__record_version,
    CASE
      WHEN special_offer__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE special_offer__record_loaded_at
    END AS special_offer__record_valid_from,
    COALESCE(
      LEAD(special_offer__record_loaded_at) OVER (PARTITION BY special_offer__special_offer_id ORDER BY special_offer__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS special_offer__record_valid_to,
    special_offer__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS special_offer__is_current_record,
    CASE
      WHEN special_offer__is_current_record
      THEN special_offer__record_loaded_at
      ELSE special_offer__record_valid_to
    END AS special_offer__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'reference__special_offer__adventure_works|',
      special_offer__special_offer_id,
      '~epoch|valid_from|',
      special_offer__record_valid_from
    )::BLOB AS _pit_hook__reference__special_offer,
    CONCAT('reference__special_offer__adventure_works|', special_offer__special_offer_id) AS _hook__reference__special_offer,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__special_offer::BLOB,
  _hook__reference__special_offer::BLOB,
  special_offer__special_offer_id::BIGINT,
  special_offer__description::TEXT,
  special_offer__discount_percentage::DOUBLE,
  special_offer__type::TEXT,
  special_offer__category::TEXT,
  special_offer__start_date::TEXT,
  special_offer__end_date::TEXT,
  special_offer__minimum_quantity::BIGINT,
  special_offer__rowguid::UUID,
  special_offer__maximum_quantity::BIGINT,
  special_offer__modified_date::DATE,
  special_offer__record_loaded_at::TIMESTAMP,
  special_offer__record_updated_at::TIMESTAMP,
  special_offer__record_version::TEXT,
  special_offer__record_valid_from::TIMESTAMP,
  special_offer__record_valid_to::TIMESTAMP,
  special_offer__is_current_record::TEXT
FROM hooks
WHERE 1 = 1
AND special_offer__record_updated_at BETWEEN @start_ts AND @end_ts