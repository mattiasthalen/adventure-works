MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of special_offers data: Sale discounts lookup table.',
  column_descriptions (
    special_offer_id = 'Primary key for SpecialOffer records.',
    description = 'Discount description.',
    discount_percentage = 'Discount percentage.',
    type = 'Discount type category.',
    category = 'Group the discount applies to such as Reseller or Customer.',
    start_date = 'Discount start date.',
    end_date = 'Discount end date.',
    minimum_quantity = 'Minimum discount percent allowed.',
    rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.',
    maximum_quantity = 'Maximum discount percent allowed.'
  )
);

SELECT
    special_offer_id::BIGINT,
    description::TEXT,
    discount_percentage::DOUBLE,
    type::TEXT,
    category::TEXT,
    start_date::DATE,
    end_date::DATE,
    minimum_quantity::BIGINT,
    rowguid::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT,
    maximum_quantity::BIGINT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__special_offers"
)
;