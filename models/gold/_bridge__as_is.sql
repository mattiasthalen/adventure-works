MODEL (
  kind VIEW,
  enabled FALSE
);

SELECT
  *
FROM silver.uss_bridge
WHERE
  bridge__is_current_record = TRUE