MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__credit_card
  ),
  tags hook,
  grain (_pit_hook__credit_card, _hook__credit_card),
  description 'Hook viewpoint of credit_cards data: Customer credit card information.',
  column_descriptions (
    credit_card__credit_card_id = 'Primary key for CreditCard records.',
    credit_card__card_type = 'Credit card name.',
    credit_card__card_number = 'Credit card number.',
    credit_card__exp_month = 'Credit card expiration month.',
    credit_card__exp_year = 'Credit card expiration year.',
    credit_card__record_loaded_at = 'Timestamp when this record was loaded into the system',
    credit_card__record_updated_at = 'Timestamp when this record was last updated',
    credit_card__record_version = 'Version number for this record',
    credit_card__record_valid_from = 'Timestamp from which this record version is valid',
    credit_card__record_valid_to = 'Timestamp until which this record version is valid',
    credit_card__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__credit_card = 'Reference hook to credit_card',
    _pit_hook__credit_card = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
);

WITH staging AS (
  SELECT
    credit_card_id AS credit_card__credit_card_id,
    card_type AS credit_card__card_type,
    card_number AS credit_card__card_number,
    exp_month AS credit_card__exp_month,
    exp_year AS credit_card__exp_year,
    modified_date AS credit_card__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS credit_card__record_loaded_at
  FROM das.raw__adventure_works__credit_cards
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY credit_card__credit_card_id ORDER BY credit_card__record_loaded_at) AS credit_card__record_version,
    CASE
      WHEN credit_card__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE credit_card__record_loaded_at
    END AS credit_card__record_valid_from,
    COALESCE(
      LEAD(credit_card__record_loaded_at) OVER (PARTITION BY credit_card__credit_card_id ORDER BY credit_card__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS credit_card__record_valid_to,
    credit_card__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS credit_card__is_current_record,
    CASE
      WHEN credit_card__is_current_record
      THEN credit_card__record_loaded_at
      ELSE credit_card__record_valid_to
    END AS credit_card__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT('credit_card__adventure_works|', credit_card__credit_card_id) AS _hook__credit_card,
    CONCAT_WS('~',
      _hook__credit_card,
      'epoch__valid_from|'||credit_card__record_valid_from
    ) AS _pit_hook__credit_card,
    *
  FROM validity
)
SELECT
  _pit_hook__credit_card::BLOB,
  _hook__credit_card::BLOB,
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
  credit_card__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND credit_card__record_updated_at BETWEEN @start_ts AND @end_ts