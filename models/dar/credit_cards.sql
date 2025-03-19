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

WITH cte__source AS (
  SELECT
    _pit_hook__credit_card,
    credit_card__credit_card_id,
    credit_card__card_type,
    credit_card__card_number,
    credit_card__exp_month,
    credit_card__exp_year,
    credit_card__modified_date,
    credit_card__record_loaded_at,
    credit_card__record_updated_at,
    credit_card__record_version,
    credit_card__record_valid_from,
    credit_card__record_valid_to,
    credit_card__is_current_record
  FROM dab.bag__adventure_works__credit_cards
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__credit_card,
    NULL AS credit_card__credit_card_id,
    'N/A' AS credit_card__card_type,
    'N/A' AS credit_card__card_number,
    NULL AS credit_card__exp_month,
    NULL AS credit_card__exp_year,
    NULL AS credit_card__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS credit_card__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS credit_card__record_updated_at,
    0 AS credit_card__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS credit_card__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS credit_card__record_valid_to,
    TRUE AS credit_card__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__credit_card::BLOB,
  credit_card__credit_card_id::BIGINT,
  credit_card__card_type::TEXT,
  credit_card__card_number::TEXT,
  credit_card__exp_month::BIGINT,
  credit_card__exp_year::BIGINT,
  credit_card__modified_date::DATE,
  credit_card__record_loaded_at::TIMESTAMP,
  credit_card__record_updated_at::TIMESTAMP,
  credit_card__record_version::TEXT,
  credit_card__record_valid_from::TIMESTAMP,
  credit_card__record_valid_to::TIMESTAMP,
  credit_card__is_current_record::BOOLEAN
FROM cte__final