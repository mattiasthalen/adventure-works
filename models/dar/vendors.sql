MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__vendor),
  description 'Business viewpoint of vendors data: Companies from whom Adventure Works Cycles purchases parts or other goods.',
  column_descriptions (
    vendor__business_entity_id = 'Primary key for Vendor records. Foreign key to BusinessEntity.BusinessEntityID.',
    vendor__account_number = 'Vendor account (identification) number.',
    vendor__name = 'Company name.',
    vendor__credit_rating = '1 = Superior, 2 = Excellent, 3 = Above average, 4 = Average, 5 = Below average.',
    vendor__preferred_vendor_status = '0 = Do not use if another vendor is available. 1 = Preferred over other vendors supplying the same product.',
    vendor__active_flag = '0 = Vendor no longer used. 1 = Vendor is actively used.',
    vendor__purchasing_web_service_url = 'Vendor URL.',
    vendor__modified_date = 'Date when this record was last modified',
    vendor__record_loaded_at = 'Timestamp when this record was loaded into the system',
    vendor__record_updated_at = 'Timestamp when this record was last updated',
    vendor__record_version = 'Version number for this record',
    vendor__record_valid_from = 'Timestamp from which this record version is valid',
    vendor__record_valid_to = 'Timestamp until which this record version is valid',
    vendor__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__vendor)
FROM dab.bag__adventure_works__vendors