MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__reference__sales_tax_rate
  ),
  tags hook,
  grain (_pit_hook__reference__sales_tax_rate, _hook__reference__sales_tax_rate),
  description 'Hook viewpoint of sales_tax_rates data: Tax rate lookup table.',
  references (_hook__reference__state_province),
  column_descriptions (
    sales_tax_rate__sales_tax_rate_id = 'Primary key for SalesTaxRate records.',
    sales_tax_rate__state_province_id = 'State, province, or country/region the sales tax applies to.',
    sales_tax_rate__tax_type = '1 = Tax applied to retail transactions, 2 = Tax applied to wholesale transactions, 3 = Tax applied to all sales (retail and wholesale) transactions.',
    sales_tax_rate__tax_rate = 'Tax rate amount.',
    sales_tax_rate__name = 'Tax rate description.',
    sales_tax_rate__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    sales_tax_rate__record_loaded_at = 'Timestamp when this record was loaded into the system',
    sales_tax_rate__record_updated_at = 'Timestamp when this record was last updated',
    sales_tax_rate__record_version = 'Version number for this record',
    sales_tax_rate__record_valid_from = 'Timestamp from which this record version is valid',
    sales_tax_rate__record_valid_to = 'Timestamp until which this record version is valid',
    sales_tax_rate__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__reference__sales_tax_rate = 'Reference hook to sales_tax_rate reference',
    _hook__reference__state_province = 'Reference hook to state_province reference',
    _pit_hook__reference__sales_tax_rate = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    sales_tax_rate_id AS sales_tax_rate__sales_tax_rate_id,
    state_province_id AS sales_tax_rate__state_province_id,
    tax_type AS sales_tax_rate__tax_type,
    tax_rate AS sales_tax_rate__tax_rate,
    name AS sales_tax_rate__name,
    rowguid AS sales_tax_rate__rowguid,
    modified_date AS sales_tax_rate__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS sales_tax_rate__record_loaded_at
  FROM das.raw__adventure_works__sales_tax_rates
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_tax_rate__sales_tax_rate_id ORDER BY sales_tax_rate__record_loaded_at) AS sales_tax_rate__record_version,
    CASE
      WHEN sales_tax_rate__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE sales_tax_rate__record_loaded_at
    END AS sales_tax_rate__record_valid_from,
    COALESCE(
      LEAD(sales_tax_rate__record_loaded_at) OVER (PARTITION BY sales_tax_rate__sales_tax_rate_id ORDER BY sales_tax_rate__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS sales_tax_rate__record_valid_to,
    sales_tax_rate__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS sales_tax_rate__is_current_record,
    CASE
      WHEN sales_tax_rate__is_current_record
      THEN sales_tax_rate__record_loaded_at
      ELSE sales_tax_rate__record_valid_to
    END AS sales_tax_rate__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('reference__sales_tax_rate__adventure_works|', sales_tax_rate__sales_tax_rate_id) AS _hook__reference__sales_tax_rate,
    CONCAT('reference__state_province__adventure_works|', sales_tax_rate__state_province_id) AS _hook__reference__state_province,
    CONCAT_WS('~',
      _hook__reference__sales_tax_rate,
      'epoch__valid_from|'||sales_tax_rate__record_valid_from
    ) AS _pit_hook__reference__sales_tax_rate,
    *
  FROM validity
)
SELECT
  _pit_hook__reference__sales_tax_rate::BLOB,
  _hook__reference__sales_tax_rate::BLOB,
  _hook__reference__state_province::BLOB,
  sales_tax_rate__sales_tax_rate_id::BIGINT,
  sales_tax_rate__state_province_id::BIGINT,
  sales_tax_rate__tax_type::BIGINT,
  sales_tax_rate__tax_rate::DOUBLE,
  sales_tax_rate__name::TEXT,
  sales_tax_rate__rowguid::TEXT,
  sales_tax_rate__modified_date::DATE,
  sales_tax_rate__record_loaded_at::TIMESTAMP,
  sales_tax_rate__record_updated_at::TIMESTAMP,
  sales_tax_rate__record_version::TEXT,
  sales_tax_rate__record_valid_from::TIMESTAMP,
  sales_tax_rate__record_valid_to::TIMESTAMP,
  sales_tax_rate__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND sales_tax_rate__record_updated_at BETWEEN @start_ts AND @end_ts