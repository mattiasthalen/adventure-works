MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column credit_card__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    credit_card_id AS credit_card__credit_card_id,
    card_number AS credit_card__card_number,
    card_type AS credit_card__card_type,
    exp_month AS credit_card__exp_month,
    exp_year AS credit_card__exp_year,
    modified_date AS credit_card__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS credit_card__record_loaded_at
  FROM bronze.raw__adventure_works__credit_cards
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY credit_card__credit_card_id ORDER BY credit_card__record_loaded_at) AS credit_card__record_version,
    CASE
      WHEN credit_card__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE credit_card__record_loaded_at
    END AS credit_card__record_valid_from,
    COALESCE(
      LEAD(credit_card__record_loaded_at) OVER (PARTITION BY credit_card__credit_card_id ORDER BY credit_card__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS credit_card__record_valid_to,
    credit_card__record_valid_to = @max_ts::TIMESTAMP AS credit_card__is_current_record,
    CASE
      WHEN credit_card__is_current_record
      THEN credit_card__record_loaded_at
      ELSE credit_card__record_valid_to
    END AS credit_card__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'credit_card|adventure_works|',
      credit_card__credit_card_id,
      '~epoch|valid_from|',
      credit_card__record_valid_from
    ) AS _pit_hook__credit_card,
    CONCAT('credit_card|adventure_works|', credit_card__credit_card_id) AS _hook__credit_card,
    *
  FROM validity
)
SELECT
  _pit_hook__credit_card::BLOB,
  _hook__credit_card::BLOB,
  credit_card__credit_card_id::BIGINT,
  credit_card__card_number::VARCHAR,
  credit_card__card_type::VARCHAR,
  credit_card__exp_month::BIGINT,
  credit_card__exp_year::BIGINT,
  credit_card__modified_date::VARCHAR,
  credit_card__record_loaded_at::TIMESTAMP,
  credit_card__record_updated_at::TIMESTAMP,
  credit_card__record_valid_from::TIMESTAMP,
  credit_card__record_valid_to::TIMESTAMP,
  credit_card__record_version::INT,
  credit_card__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND credit_card__record_updated_at BETWEEN @start_ts AND @end_ts