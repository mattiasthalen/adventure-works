MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    sales_reason_id AS sales_reason__sales_reason_id,
    modified_date AS sales_reason__modified_date,
    name AS sales_reason__name,
    reason_type AS sales_reason__reason_type,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS sales_reason__record_loaded_at
  FROM bronze.raw__adventure_works__sales_reasons
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_reason__sales_reason_id ORDER BY sales_reason__record_loaded_at) AS sales_reason__record_version,
    CASE
      WHEN sales_reason__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE sales_reason__record_loaded_at
    END AS sales_reason__record_valid_from,
    COALESCE(
      LEAD(sales_reason__record_loaded_at) OVER (PARTITION BY sales_reason__sales_reason_id ORDER BY sales_reason__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS sales_reason__record_valid_to,
    sales_reason__record_valid_to = @max_ts::TIMESTAMP AS sales_reason__is_current_record,
    CASE
      WHEN sales_reason__is_current_record
      THEN sales_reason__record_loaded_at
      ELSE sales_reason__record_valid_to
    END AS sales_reason__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'sales_reason|adventure_works|',
      sales_reason__sales_reason_id,
      '~epoch|valid_from|',
      sales_reason__record_valid_from
    ) AS _pit_hook__sales_reason,
    CONCAT('sales_reason|adventure_works|', sales_reason__sales_reason_id) AS _hook__sales_reason,
    *
  FROM validity
)
SELECT
  _pit_hook__sales_reason::BLOB,
  _hook__sales_reason::BLOB,
  sales_reason__sales_reason_id::VARCHAR,
  sales_reason__modified_date::VARCHAR,
  sales_reason__name::VARCHAR,
  sales_reason__reason_type::VARCHAR,
  sales_reason__record_loaded_at::TIMESTAMP,
  sales_reason__record_version::INT,
  sales_reason__record_valid_from::TIMESTAMP,
  sales_reason__record_valid_to::TIMESTAMP,
  sales_reason__is_current_record::BOOLEAN,
  sales_reason__record_updated_at::TIMESTAMP
FROM hooks