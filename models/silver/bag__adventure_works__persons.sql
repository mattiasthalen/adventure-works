MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    business_entity_id AS business_entity__business_entity_id,
    demographics AS business_entity__demographics,
    email_promotion AS business_entity__email_promotion,
    first_name AS business_entity__first_name,
    last_name AS business_entity__last_name,
    middle_name AS business_entity__middle_name,
    modified_date AS business_entity__modified_date,
    name_style AS business_entity__name_style,
    person_type AS business_entity__person_type,
    rowguid AS business_entity__rowguid,
    suffix AS business_entity__suffix,
    title AS business_entity__title,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS business_entity__record_loaded_at
  FROM bronze.raw__adventure_works__persons
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY business_entity__business_entity_id ORDER BY business_entity__record_loaded_at) AS business_entity__record_version,
    CASE
      WHEN business_entity__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE business_entity__record_loaded_at
    END AS business_entity__record_valid_from,
    COALESCE(
      LEAD(business_entity__record_loaded_at) OVER (PARTITION BY business_entity__business_entity_id ORDER BY business_entity__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS business_entity__record_valid_to,
    business_entity__record_valid_to = @max_ts::TIMESTAMP AS business_entity__is_current_record,
    CASE
      WHEN business_entity__is_current_record
      THEN business_entity__record_loaded_at
      ELSE business_entity__record_valid_to
    END AS business_entity__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'business_entity|adventure_works|',
      business_entity__business_entity_id,
      '~epoch|valid_from|',
      business_entity__record_valid_from
    ) AS _pit_hook__business_entity,
    CONCAT('business_entity|adventure_works|', business_entity__business_entity_id) AS _hook__business_entity,
    *
  FROM validity
)
SELECT
  _pit_hook__business_entity::BLOB,
  _hook__business_entity::BLOB,
  business_entity__business_entity_id::VARCHAR,
  business_entity__demographics::VARCHAR,
  business_entity__email_promotion::VARCHAR,
  business_entity__first_name::VARCHAR,
  business_entity__last_name::VARCHAR,
  business_entity__middle_name::VARCHAR,
  business_entity__modified_date::VARCHAR,
  business_entity__name_style::VARCHAR,
  business_entity__person_type::VARCHAR,
  business_entity__rowguid::VARCHAR,
  business_entity__suffix::VARCHAR,
  business_entity__title::VARCHAR,
  business_entity__record_loaded_at::TIMESTAMP,
  business_entity__record_version::INT,
  business_entity__record_valid_from::TIMESTAMP,
  business_entity__record_valid_to::TIMESTAMP,
  business_entity__is_current_record::BOOLEAN,
  business_entity__record_updated_at::TIMESTAMP
FROM hooks