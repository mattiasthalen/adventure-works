MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__special_offer),
  description 'Business viewpoint of special_offers data: Sale discounts lookup table.',
  column_descriptions (
    special_offer__special_offer_id = 'Primary key for SpecialOffer records.',
    special_offer__description = 'Discount description.',
    special_offer__discount_percentage = 'Discount percentage.',
    special_offer__type = 'Discount type category.',
    special_offer__category = 'Group the discount applies to such as Reseller or Customer.',
    special_offer__start_date = 'Discount start date.',
    special_offer__end_date = 'Discount end date.',
    special_offer__minimum_quantity = 'Minimum discount percent allowed.',
    special_offer__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    special_offer__maximum_quantity = 'Maximum discount percent allowed.',
    special_offer__modified_date = 'Date when this record was last modified',
    special_offer__record_loaded_at = 'Timestamp when this record was loaded into the system',
    special_offer__record_updated_at = 'Timestamp when this record was last updated',
    special_offer__record_version = 'Version number for this record',
    special_offer__record_valid_from = 'Timestamp from which this record version is valid',
    special_offer__record_valid_to = 'Timestamp until which this record version is valid',
    special_offer__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__reference__special_offer,
    special_offer__special_offer_id,
    special_offer__description,
    special_offer__discount_percentage,
    special_offer__type,
    special_offer__category,
    special_offer__start_date,
    special_offer__end_date,
    special_offer__minimum_quantity,
    special_offer__rowguid,
    special_offer__maximum_quantity,
    special_offer__modified_date,
    special_offer__record_loaded_at,
    special_offer__record_updated_at,
    special_offer__record_version,
    special_offer__record_valid_from,
    special_offer__record_valid_to,
    special_offer__is_current_record
  FROM dab.bag__adventure_works__special_offers
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__reference__special_offer,
    NULL AS special_offer__special_offer_id,
    'N/A' AS special_offer__description,
    NULL AS special_offer__discount_percentage,
    'N/A' AS special_offer__type,
    'N/A' AS special_offer__category,
    NULL AS special_offer__start_date,
    NULL AS special_offer__end_date,
    NULL AS special_offer__minimum_quantity,
    'N/A' AS special_offer__rowguid,
    NULL AS special_offer__maximum_quantity,
    NULL AS special_offer__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS special_offer__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS special_offer__record_updated_at,
    0 AS special_offer__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS special_offer__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS special_offer__record_valid_to,
    TRUE AS special_offer__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__reference__special_offer::BLOB,
  special_offer__special_offer_id::BIGINT,
  special_offer__description::TEXT,
  special_offer__discount_percentage::DOUBLE,
  special_offer__type::TEXT,
  special_offer__category::TEXT,
  special_offer__start_date::DATE,
  special_offer__end_date::DATE,
  special_offer__minimum_quantity::BIGINT,
  special_offer__rowguid::TEXT,
  special_offer__maximum_quantity::BIGINT,
  special_offer__modified_date::DATE,
  special_offer__record_loaded_at::TIMESTAMP,
  special_offer__record_updated_at::TIMESTAMP,
  special_offer__record_version::TEXT,
  special_offer__record_valid_from::TIMESTAMP,
  special_offer__record_valid_to::TIMESTAMP,
  special_offer__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.special_offers TO './export/dar/special_offers.parquet' (FORMAT parquet, COMPRESSION zstd)
);