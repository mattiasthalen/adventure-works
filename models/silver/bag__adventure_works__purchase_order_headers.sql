MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    purchase_order_id AS purchase_order__purchase_order_id,
    employee_id AS purchase_order__employee_id,
    ship_method_id AS purchase_order__ship_method_id,
    vendor_id AS purchase_order__vendor_id,
    freight AS purchase_order__freight,
    modified_date AS purchase_order__modified_date,
    order_date AS purchase_order__order_date,
    revision_number AS purchase_order__revision_number,
    ship_date AS purchase_order__ship_date,
    status AS purchase_order__status,
    sub_total AS purchase_order__sub_total,
    tax_amt AS purchase_order__tax_amt,
    total_due AS purchase_order__total_due,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS purchase_order__record_loaded_at
  FROM bronze.raw__adventure_works__purchase_order_headers
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY purchase_order__purchase_order_id ORDER BY purchase_order__record_loaded_at) AS purchase_order__record_version,
    CASE
      WHEN purchase_order__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE purchase_order__record_loaded_at
    END AS purchase_order__record_valid_from,
    COALESCE(
      LEAD(purchase_order__record_loaded_at) OVER (PARTITION BY purchase_order__purchase_order_id ORDER BY purchase_order__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS purchase_order__record_valid_to,
    purchase_order__record_valid_to = @max_ts::TIMESTAMP AS purchase_order__is_current_record,
    CASE
      WHEN purchase_order__is_current_record
      THEN purchase_order__record_loaded_at
      ELSE purchase_order__record_valid_to
    END AS purchase_order__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'purchase_order|adventure_works|',
      purchase_order__purchase_order_id,
      '~epoch|valid_from|',
      purchase_order__record_valid_from
    ) AS _pit_hook__purchase_order,
    CONCAT('purchase_order|adventure_works|', purchase_order__purchase_order_id) AS _hook__purchase_order,
    CONCAT('employee|adventure_works|', purchase_order__employee_id) AS _hook__employee,
    CONCAT('ship_method|adventure_works|', purchase_order__ship_method_id) AS _hook__ship_method,
    CONCAT('vendor|adventure_works|', purchase_order__vendor_id) AS _hook__vendor,
    *
  FROM validity
)
SELECT
  _pit_hook__purchase_order::BLOB,
  _hook__purchase_order::BLOB,
  _hook__employee::BLOB,
  _hook__ship_method::BLOB,
  _hook__vendor::BLOB,
  purchase_order__purchase_order_id::VARCHAR,
  purchase_order__employee_id::VARCHAR,
  purchase_order__ship_method_id::VARCHAR,
  purchase_order__vendor_id::VARCHAR,
  purchase_order__freight::VARCHAR,
  purchase_order__modified_date::VARCHAR,
  purchase_order__order_date::VARCHAR,
  purchase_order__revision_number::VARCHAR,
  purchase_order__ship_date::VARCHAR,
  purchase_order__status::VARCHAR,
  purchase_order__sub_total::VARCHAR,
  purchase_order__tax_amt::VARCHAR,
  purchase_order__total_due::VARCHAR,
  purchase_order__record_loaded_at::TIMESTAMP,
  purchase_order__record_version::INT,
  purchase_order__record_valid_from::TIMESTAMP,
  purchase_order__record_valid_to::TIMESTAMP,
  purchase_order__is_current_record::BOOLEAN,
  purchase_order__record_updated_at::TIMESTAMP
FROM hooks