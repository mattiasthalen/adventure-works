MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
    product_photo_id::BIGINT,
    thumb_nail_photo::TEXT,
    thumbnail_photo_file_name::TEXT,
    large_photo::TEXT,
    large_photo_file_name::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__product_photos"
)
;