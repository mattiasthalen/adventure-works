MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__business_entity)
);

SELECT
  *
  EXCLUDE (_hook__business_entity, _hook__person__contact, _hook__reference__contact_type)
FROM silver.bag__adventure_works__business_entity_contacts