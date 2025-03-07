MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__address)
);

SELECT
  *
  EXCLUDE (_hook__address, _hook__business_entity, _hook__reference__address_type)
FROM dab.bag__adventure_works__business_entity_addresses