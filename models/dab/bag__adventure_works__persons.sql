MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__person__individual,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__person__individual, _hook__person__individual)
);

WITH staging AS (
  SELECT
    business_entity_id AS person__business_entity_id,
    person_type AS person__person_type,
    name_style AS person__name_style,
    first_name AS person__first_name,
    middle_name AS person__middle_name,
    last_name AS person__last_name,
    email_promotion AS person__email_promotion,
    demographics AS person__demographics,
    rowguid AS person__rowguid,
    title AS person__title,
    suffix AS person__suffix,
    additional_contact_info AS person__additional_contact_info,
    modified_date AS person__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS person__record_loaded_at
  FROM das.raw__adventure_works__persons
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY person__business_entity_id ORDER BY person__record_loaded_at) AS person__record_version,
    CASE
      WHEN person__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE person__record_loaded_at
    END AS person__record_valid_from,
    COALESCE(
      LEAD(person__record_loaded_at) OVER (PARTITION BY person__business_entity_id ORDER BY person__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS person__record_valid_to,
    person__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS person__is_current_record,
    CASE
      WHEN person__is_current_record
      THEN person__record_loaded_at
      ELSE person__record_valid_to
    END AS person__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'person__individual__adventure_works|',
      person__business_entity_id,
      '~epoch|valid_from|',
      person__record_valid_from
    )::BLOB AS _pit_hook__person__individual,
    CONCAT('person__individual__adventure_works|', person__business_entity_id) AS _hook__person__individual,
    *
  FROM validity
)
SELECT
  _pit_hook__person__individual::BLOB,
  _hook__person__individual::BLOB,
  person__business_entity_id::BIGINT,
  person__person_type::TEXT,
  person__name_style::BOOLEAN,
  person__first_name::TEXT,
  person__middle_name::TEXT,
  person__last_name::TEXT,
  person__email_promotion::BIGINT,
  person__demographics::TEXT,
  person__rowguid::TEXT,
  person__title::TEXT,
  person__suffix::TEXT,
  person__additional_contact_info::TEXT,
  person__modified_date::DATE,
  person__record_loaded_at::TIMESTAMP,
  person__record_updated_at::TIMESTAMP,
  person__record_version::TEXT,
  person__record_valid_from::TIMESTAMP,
  person__record_valid_to::TIMESTAMP,
  person__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND person__record_updated_at BETWEEN @start_ts AND @end_ts