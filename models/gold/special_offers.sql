MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column special_offer__record_updated_at
  ),
  enabled TRUE
);

SELECT
  *
  EXCLUDE (_hook__special_offer)
FROM silver.bag__adventure_works__special_offers
WHERE 1 = 1
AND special_offer__record_updated_at BETWEEN @start_ts AND @end_ts