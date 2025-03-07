MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__credit_card)
);

SELECT
  *
  EXCLUDE (_hook__credit_card)
FROM dab.bag__adventure_works__credit_cards