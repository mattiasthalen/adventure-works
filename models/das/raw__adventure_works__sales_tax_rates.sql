MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of sales_tax_rates data: Tax rate lookup table.',
  column_descriptions (
    sales_tax_rate_id = 'Primary key for SalesTaxRate records.',
    state_province_id = 'State, province, or country/region the sales tax applies to.',
    tax_type = '1 = Tax applied to retail transactions, 2 = Tax applied to wholesale transactions, 3 = Tax applied to all sales (retail and wholesale) transactions.',
    tax_rate = 'Tax rate amount.',
    name = 'Tax rate description.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    sales_tax_rate_id::BIGINT,
    state_province_id::BIGINT,
    tax_type::BIGINT,
    tax_rate::DOUBLE,
    name::TEXT,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__sales_tax_rates"
)
;