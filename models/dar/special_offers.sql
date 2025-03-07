MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__reference__special_offer)
);

SELECT
  *
  EXCLUDE (_hook__reference__special_offer)
FROM dab.bag__adventure_works__special_offers