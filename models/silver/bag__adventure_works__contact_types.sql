MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    contact_type_id AS contact_type__contact_type_id,
    modified_date AS contact_type__modified_date,
    name AS contact_type__name,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS contact_type__record_loaded_at
  FROM bronze.raw__adventure_works__contact_types
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY contact_type__contact_type_id ORDER BY contact_type__record_loaded_at) AS contact_type__record_version,
    CASE
      WHEN contact_type__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE contact_type__record_loaded_at
    END AS contact_type__record_valid_from,
    COALESCE(
      LEAD(contact_type__record_loaded_at) OVER (PARTITION BY contact_type__contact_type_id ORDER BY contact_type__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS contact_type__record_valid_to,
    contact_type__record_valid_to = @max_ts::TIMESTAMP AS contact_type__is_current_record,
    CASE
      WHEN contact_type__is_current_record
      THEN contact_type__record_loaded_at
      ELSE contact_type__record_valid_to
    END AS contact_type__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'contact_type|adventure_works|',
      contact_type__contact_type_id,
      '~epoch|valid_from|',
      contact_type__record_valid_from
    ) AS _pit_hook__contact_type,
    CONCAT('contact_type|adventure_works|', contact_type__contact_type_id) AS _hook__contact_type,
    *
  FROM validity
)
SELECT
  _pit_hook__contact_type::BLOB,
  _hook__contact_type::BLOB,
  contact_type__contact_type_id::VARCHAR,
  contact_type__modified_date::VARCHAR,
  contact_type__name::VARCHAR,
  contact_type__record_loaded_at::TIMESTAMP,
  contact_type__record_version::INT,
  contact_type__record_valid_from::TIMESTAMP,
  contact_type__record_valid_to::TIMESTAMP,
  contact_type__is_current_record::BOOLEAN,
  contact_type__record_updated_at::TIMESTAMP
FROM hooks