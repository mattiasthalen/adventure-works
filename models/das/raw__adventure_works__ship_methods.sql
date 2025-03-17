MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of ship_methods data: Shipping company lookup table.',
  column_descriptions (
    ship_method_id = 'Primary key for ShipMethod records.',
    name = 'Shipping company name.',
    ship_base = 'Minimum shipping charge.',
    ship_rate = 'Shipping charge per pound.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    ship_method_id::BIGINT,
    name::TEXT,
    ship_base::DOUBLE,
    ship_rate::DOUBLE,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__ship_methods"
)
;