MODEL (
  kind VIEW,
  enabled TRUE,
  description 'Raw viewpoint of product_photos data: Product images.',
  column_descriptions (
    product_photo_id = 'Primary key for ProductPhoto records.',
    thumb_nail_photo = 'Small image of the product.',
    thumbnail_photo_file_name = 'Small image file name.',
    large_photo = 'Large image of the product.',
    large_photo_file_name = 'Large image file name.',
    modified_date = 'Date and time the record was last updated.',
    _dlt_load_id = 'Internal data loading identifier.'
  )
);

SELECT
    product_photo_id::BIGINT,
    thumb_nail_photo::BINARY,
    thumbnail_photo_file_name::TEXT,
    large_photo::BINARY,
    large_photo_file_name::TEXT,
    modified_date::DATE,
    _dlt_load_id::TEXT
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/das/raw__adventure_works__product_photos"
)
;