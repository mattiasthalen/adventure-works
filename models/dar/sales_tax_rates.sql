MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__sales_tax_rate),
  description 'Business viewpoint of sales_tax_rates data: Tax rate lookup table.',
  column_descriptions (
    sales_tax_rate__sales_tax_rate_id = 'Primary key for SalesTaxRate records.',
    sales_tax_rate__state_province_id = 'State, province, or country/region the sales tax applies to.',
    sales_tax_rate__tax_type = '1 = Tax applied to retail transactions, 2 = Tax applied to wholesale transactions, 3 = Tax applied to all sales (retail and wholesale) transactions.',
    sales_tax_rate__tax_rate = 'Tax rate amount.',
    sales_tax_rate__name = 'Tax rate description.',
    sales_tax_rate__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    sales_tax_rate__modified_date = 'Date when this record was last modified',
    sales_tax_rate__record_loaded_at = 'Timestamp when this record was loaded into the system',
    sales_tax_rate__record_updated_at = 'Timestamp when this record was last updated',
    sales_tax_rate__record_version = 'Version number for this record',
    sales_tax_rate__record_valid_from = 'Timestamp from which this record version is valid',
    sales_tax_rate__record_valid_to = 'Timestamp until which this record version is valid',
    sales_tax_rate__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__reference__sales_tax_rate,
    sales_tax_rate__sales_tax_rate_id,
    sales_tax_rate__state_province_id,
    sales_tax_rate__tax_type,
    sales_tax_rate__tax_rate,
    sales_tax_rate__name,
    sales_tax_rate__rowguid,
    sales_tax_rate__modified_date,
    sales_tax_rate__record_loaded_at,
    sales_tax_rate__record_updated_at,
    sales_tax_rate__record_version,
    sales_tax_rate__record_valid_from,
    sales_tax_rate__record_valid_to,
    sales_tax_rate__is_current_record
  FROM dab.bag__adventure_works__sales_tax_rates
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__reference__sales_tax_rate,
    NULL AS sales_tax_rate__sales_tax_rate_id,
    NULL AS sales_tax_rate__state_province_id,
    NULL AS sales_tax_rate__tax_type,
    NULL AS sales_tax_rate__tax_rate,
    'N/A' AS sales_tax_rate__name,
    'N/A' AS sales_tax_rate__rowguid,
    NULL AS sales_tax_rate__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_tax_rate__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_tax_rate__record_updated_at,
    0 AS sales_tax_rate__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_tax_rate__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS sales_tax_rate__record_valid_to,
    TRUE AS sales_tax_rate__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__reference__sales_tax_rate::BLOB,
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
  sales_tax_rate__is_current_record::BOOLEAN
FROM cte__final