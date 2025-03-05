MODEL (
  kind VIEW,
  enabled TRUE
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
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__credit_cards"
)
;