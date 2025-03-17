MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__credit_card),
  description 'Business viewpoint of credit_cards data: Customer credit card information.',
  column_descriptions (
    credit_card__credit_card_id = 'Primary key for CreditCard records.',
    credit_card__card_type = 'Credit card name.',
    credit_card__card_number = 'Credit card number.',
    credit_card__exp_month = 'Credit card expiration month.',
    credit_card__exp_year = 'Credit card expiration year.',
    credit_card__modified_date = 'Date when this record was last modified',
    credit_card__record_loaded_at = 'Timestamp when this record was loaded into the system',
    credit_card__record_updated_at = 'Timestamp when this record was last updated',
    credit_card__record_version = 'Version number for this record',
    credit_card__record_valid_from = 'Timestamp from which this record version is valid',
    credit_card__record_valid_to = 'Timestamp until which this record version is valid',
    credit_card__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__credit_card)
FROM dab.bag__adventure_works__credit_cards