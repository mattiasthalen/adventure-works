MODEL (
  kind VIEW,
  enabled FALSE
);

WITH bridge AS (
  SELECT
    'sales_persons' AS stage,
    bag__adventure_works__sales_persons._pit_hook__sales_person,
    bag__adventure_works__sales_persons._hook__sales_person,
    bag__adventure_works__sales_persons.sales_person__record_loaded_at AS bridge__record_loaded_at,
    bag__adventure_works__sales_persons.sales_person__record_updated_at AS bridge__record_updated_at,
    bag__adventure_works__sales_persons.sales_person__record_valid_from AS bridge__record_valid_from,
    bag__adventure_works__sales_persons.sales_person__record_valid_to AS bridge__record_valid_to
  FROM silver.bag__adventure_works__sales_persons
)
SELECT
  *,
  bridge__record_valid_to = @MAX_TS::TIMESTAMP AS bridge__is_current_record
FROM bridge