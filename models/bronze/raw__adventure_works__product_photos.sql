MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  product_photo_id,
  large_photo,
  large_photo_file_name,
  modified_date,
  thumb_nail_photo,
  thumbnail_photo_file_name,
  _dlt_load_id
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/raw__adventure_works__product_photos"
)
