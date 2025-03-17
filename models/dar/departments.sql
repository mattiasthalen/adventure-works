MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__department),
  description 'Business viewpoint of departments data: Lookup table containing the departments within the Adventure Works Cycles company.',
  column_descriptions (
    department__department_id = 'Primary key for Department records.',
    department__name = 'Name of the department.',
    department__group_name = 'Name of the group to which the department belongs.',
    department__modified_date = 'Date when this record was last modified',
    department__record_loaded_at = 'Timestamp when this record was loaded into the system',
    department__record_updated_at = 'Timestamp when this record was last updated',
    department__record_version = 'Version number for this record',
    department__record_valid_from = 'Timestamp from which this record version is valid',
    department__record_valid_to = 'Timestamp until which this record version is valid',
    department__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

SELECT
  *
  EXCLUDE (_hook__department)
FROM dab.bag__adventure_works__departments