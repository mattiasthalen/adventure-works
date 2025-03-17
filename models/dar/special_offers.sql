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

SELECT
  *
  EXCLUDE (_hook__reference__special_offer)
FROM dab.bag__adventure_works__special_offers