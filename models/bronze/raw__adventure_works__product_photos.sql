MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
  product_photo_id::BIGINT,
  large_photo::VARCHAR,
  large_photo_file_name::VARCHAR,
  modified_date::VARCHAR,
  thumb_nail_photo::VARCHAR,
  thumbnail_photo_file_name::VARCHAR,
  _dlt_id::VARCHAR,
  _dlt_load_id::VARCHAR
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_photos"
);
