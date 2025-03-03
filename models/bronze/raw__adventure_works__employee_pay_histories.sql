MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  business_entity_id,
  modified_date,
  pay_frequency,
  rate,
  rate_change_date,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__employee_pay_histories"
)
