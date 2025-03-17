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

SELECT
  *
  EXCLUDE (_hook__reference__sales_tax_rate, _hook__reference__state_province)
FROM dab.bag__adventure_works__sales_tax_rates