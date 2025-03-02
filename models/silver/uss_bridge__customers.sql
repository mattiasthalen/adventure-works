MODEL (
  kind FULL,
  enabled TRUE
);

WITH bridge AS (
  SELECT
    'customers' AS stage,
    bag__adventure_works__customers._pit_hook__customer,
    bag__adventure_works__customers._hook__customer,
    bag__adventure_works__customers.customer__record_loaded_at AS bridge__record_loaded_at,
    bag__adventure_works__customers.customer__record_updated_at AS bridge__record_updated_at,
    bag__adventure_works__customers.customer__record_valid_from AS bridge__record_valid_from,
    bag__adventure_works__customers.customer__record_valid_to AS bridge__record_valid_to
  FROM silver.bag__adventure_works__customers
)
SELECT
  *,
  bridge__record_valid_to = @MAX_TS::TIMESTAMP AS bridge__is_current_record
FROM bridge