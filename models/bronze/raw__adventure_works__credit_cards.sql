MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  credit_card_id,
  card_number,
  card_type,
  exp_month,
  exp_year,
  modified_date,
  _dlt_load_id
FROM ICEBERG_SCAN(
    "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__credit_cards"
)