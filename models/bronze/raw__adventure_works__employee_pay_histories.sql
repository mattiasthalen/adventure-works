MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  business_entity_id::BIGINT,
  rate_change_date::TIMESTAMP,
  modified_date::VARCHAR,
  pay_frequency::BIGINT,
  rate::DOUBLE,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__employee_pay_histories"
);
