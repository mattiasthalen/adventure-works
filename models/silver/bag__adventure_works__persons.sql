MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column person__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    business_entity_id AS person__business_entity_id,
    demographics AS person__demographics,
    email_promotion AS person__email_promotion,
    first_name AS person__first_name,
    last_name AS person__last_name,
    middle_name AS person__middle_name,
    modified_date AS person__modified_date,
    name_style AS person__name_style,
    person_type AS person__person_type,
    rowguid AS person__rowguid,
    suffix AS person__suffix,
    title AS person__title,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS person__record_loaded_at
  FROM bronze.raw__adventure_works__persons
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY person__business_entity_id ORDER BY person__record_loaded_at) AS person__record_version,
    CASE
      WHEN person__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE person__record_loaded_at
    END AS person__record_valid_from,
    COALESCE(
      LEAD(person__record_loaded_at) OVER (PARTITION BY person__business_entity_id ORDER BY person__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS person__record_valid_to,
    person__record_valid_to = @max_ts::TIMESTAMP AS person__is_current_record,
    CASE
      WHEN person__is_current_record
      THEN person__record_loaded_at
      ELSE person__record_valid_to
    END AS person__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'business_entity|adventure_works|',
      person__business_entity_id,
      '~epoch|valid_from|',
      person__record_valid_from
    ) AS _pit_hook__business_entity,
    CONCAT('business_entity|adventure_works|', person__business_entity_id) AS _hook__business_entity,
    *
  FROM validity
)
SELECT
  _pit_hook__business_entity::BLOB,
  _hook__business_entity::BLOB,
  person__business_entity_id::BIGINT,
  person__demographics::VARCHAR,
  person__email_promotion::BIGINT,
  person__first_name::VARCHAR,
  person__last_name::VARCHAR,
  person__middle_name::VARCHAR,
  person__modified_date::VARCHAR,
  person__name_style::BOOLEAN,
  person__person_type::VARCHAR,
  person__rowguid::VARCHAR,
  person__suffix::VARCHAR,
  person__title::VARCHAR,
  person__record_loaded_at::TIMESTAMP,
  person__record_updated_at::TIMESTAMP,
  person__record_valid_from::TIMESTAMP,
  person__record_valid_to::TIMESTAMP,
  person__record_version::INT,
  person__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND person__record_updated_at BETWEEN @start_ts AND @end_ts