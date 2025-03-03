MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    sales_tax_rate_id AS sales_tax_rate__sales_tax_rate_id,
    state_province_id AS sales_tax_rate__state_province_id,
    modified_date AS sales_tax_rate__modified_date,
    name AS sales_tax_rate__name,
    rowguid AS sales_tax_rate__rowguid,
    tax_rate AS sales_tax_rate__tax_rate,
    tax_type AS sales_tax_rate__tax_type,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS sales_tax_rate__record_loaded_at
  FROM bronze.raw__adventure_works__sales_tax_rates
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_tax_rate__sales_tax_rate_id ORDER BY sales_tax_rate__record_loaded_at) AS sales_tax_rate__record_version,
    CASE
      WHEN sales_tax_rate__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE sales_tax_rate__record_loaded_at
    END AS sales_tax_rate__record_valid_from,
    COALESCE(
      LEAD(sales_tax_rate__record_loaded_at) OVER (PARTITION BY sales_tax_rate__sales_tax_rate_id ORDER BY sales_tax_rate__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS sales_tax_rate__record_valid_to,
    sales_tax_rate__record_valid_to = @max_ts::TIMESTAMP AS sales_tax_rate__is_current_record,
    CASE
      WHEN sales_tax_rate__is_current_record
      THEN sales_tax_rate__record_loaded_at
      ELSE sales_tax_rate__record_valid_to
    END AS sales_tax_rate__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'sales_tax_rate|adventure_works|',
      sales_tax_rate__sales_tax_rate_id,
      '~epoch|valid_from|',
      sales_tax_rate__record_valid_from
    ) AS _pit_hook__sales_tax_rate,
    CONCAT('sales_tax_rate|adventure_works|', sales_tax_rate__sales_tax_rate_id) AS _hook__sales_tax_rate,
    CONCAT('state_province|adventure_works|', sales_tax_rate__state_province_id) AS _hook__state_province,
    *
  FROM validity
)
SELECT
  _pit_hook__sales_tax_rate::BLOB,
  _hook__sales_tax_rate::BLOB,
  _hook__state_province::BLOB,
  sales_tax_rate__sales_tax_rate_id::VARCHAR,
  sales_tax_rate__state_province_id::VARCHAR,
  sales_tax_rate__modified_date::VARCHAR,
  sales_tax_rate__name::VARCHAR,
  sales_tax_rate__rowguid::VARCHAR,
  sales_tax_rate__tax_rate::VARCHAR,
  sales_tax_rate__tax_type::VARCHAR,
  sales_tax_rate__record_loaded_at::TIMESTAMP,
  sales_tax_rate__record_version::INT,
  sales_tax_rate__record_valid_from::TIMESTAMP,
  sales_tax_rate__record_valid_to::TIMESTAMP,
  sales_tax_rate__is_current_record::BOOLEAN,
  sales_tax_rate__record_updated_at::TIMESTAMP
FROM hooks