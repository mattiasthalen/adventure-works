MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__credit_card)
);

SELECT
  *
  EXCLUDE (_hook__credit_card)
FROM silver.bag__adventure_works__credit_cards