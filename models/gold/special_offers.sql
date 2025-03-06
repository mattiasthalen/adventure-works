MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__reference__special_offer)
);

SELECT
  *
  EXCLUDE (_hook__reference__special_offer)
FROM silver.bag__adventure_works__special_offers