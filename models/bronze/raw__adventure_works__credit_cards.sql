MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  credit_card_id::BIGINT,
  card_number::VARCHAR,
  card_type::VARCHAR,
  exp_month::BIGINT,
  exp_year::BIGINT,
  modified_date::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__credit_cards"
);
