MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of credit_cards data: Customer credit card information.',
  column_descriptions (
    credit_card_id = 'Primary key for CreditCard records.',
    card_type = 'Credit card name.',
    card_number = 'Credit card number.',
    exp_month = 'Credit card expiration month.',
    exp_year = 'Credit card expiration year.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    credit_card_id::BIGINT,
    card_type::TEXT,
    card_number::TEXT,
    exp_month::BIGINT,
    exp_year::BIGINT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__credit_cards"
)
;