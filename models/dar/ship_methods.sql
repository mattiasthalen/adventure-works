MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__ship_method),
  description 'Business viewpoint of ship_methods data: Shipping company lookup table.',
  column_descriptions (
    ship_method__ship_method_id = 'Primary key for ShipMethod records.',
    ship_method__name = 'Shipping company name.',
    ship_method__ship_base = 'Minimum shipping charge.',
    ship_method__ship_rate = 'Shipping charge per pound.',
    ship_method__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    ship_method__modified_date = 'Date when this record was last modified',
    ship_method__record_loaded_at = 'Timestamp when this record was loaded into the system',
    ship_method__record_updated_at = 'Timestamp when this record was last updated',
    ship_method__record_version = 'Version number for this record',
    ship_method__record_valid_from = 'Timestamp from which this record version is valid',
    ship_method__record_valid_to = 'Timestamp until which this record version is valid',
    ship_method__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__ship_method)
FROM dab.bag__adventure_works__ship_methods