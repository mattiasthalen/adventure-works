MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__order__purchase,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__order__purchase, _hook__order__purchase),
  references (_hook__person__employee, _hook__vendor, _hook__ship_method)
);

WITH staging AS (
  SELECT
    purchase_order_id AS purchase_order_header__purchase_order_id,
    revision_number AS purchase_order_header__revision_number,
    status AS purchase_order_header__status,
    employee_id AS purchase_order_header__employee_id,
    vendor_id AS purchase_order_header__vendor_id,
    ship_method_id AS purchase_order_header__ship_method_id,
    order_date AS purchase_order_header__order_date,
    ship_date AS purchase_order_header__ship_date,
    sub_total AS purchase_order_header__sub_total,
    tax_amt AS purchase_order_header__tax_amt,
    freight AS purchase_order_header__freight,
    total_due AS purchase_order_header__total_due,
    modified_date AS purchase_order_header__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS purchase_order_header__record_loaded_at
  FROM das.raw__adventure_works__purchase_order_headers
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY purchase_order_header__purchase_order_id ORDER BY purchase_order_header__record_loaded_at) AS purchase_order_header__record_version,
    CASE
      WHEN purchase_order_header__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE purchase_order_header__record_loaded_at
    END AS purchase_order_header__record_valid_from,
    COALESCE(
      LEAD(purchase_order_header__record_loaded_at) OVER (PARTITION BY purchase_order_header__purchase_order_id ORDER BY purchase_order_header__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS purchase_order_header__record_valid_to,
    purchase_order_header__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS purchase_order_header__is_current_record,
    CASE
      WHEN purchase_order_header__is_current_record
      THEN purchase_order_header__record_loaded_at
      ELSE purchase_order_header__record_valid_to
    END AS purchase_order_header__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'order__purchase__adventure_works|',
      purchase_order_header__purchase_order_id,
      '~epoch__valid_from|',
      purchase_order_header__record_valid_from
    )::BLOB AS _pit_hook__order__purchase,
    CONCAT('order__purchase__adventure_works|', purchase_order_header__purchase_order_id) AS _hook__order__purchase,
    CONCAT('person__employee__adventure_works|', purchase_order_header__employee_id) AS _hook__person__employee,
    CONCAT('vendor__adventure_works|', purchase_order_header__vendor_id) AS _hook__vendor,
    CONCAT('ship_method__adventure_works|', purchase_order_header__ship_method_id) AS _hook__ship_method,
    *
  FROM validity
)
SELECT
  _pit_hook__order__purchase::BLOB,
  _hook__order__purchase::BLOB,
  _hook__person__employee::BLOB,
  _hook__vendor::BLOB,
  _hook__ship_method::BLOB,
  purchase_order_header__purchase_order_id::BIGINT,
  purchase_order_header__revision_number::BIGINT,
  purchase_order_header__status::BIGINT,
  purchase_order_header__employee_id::BIGINT,
  purchase_order_header__vendor_id::BIGINT,
  purchase_order_header__ship_method_id::BIGINT,
  purchase_order_header__order_date::DATE,
  purchase_order_header__ship_date::DATE,
  purchase_order_header__sub_total::DOUBLE,
  purchase_order_header__tax_amt::DOUBLE,
  purchase_order_header__freight::DOUBLE,
  purchase_order_header__total_due::DOUBLE,
  purchase_order_header__modified_date::DATE,
  purchase_order_header__record_loaded_at::TIMESTAMP,
  purchase_order_header__record_updated_at::TIMESTAMP,
  purchase_order_header__record_version::TEXT,
  purchase_order_header__record_valid_from::TIMESTAMP,
  purchase_order_header__record_valid_to::TIMESTAMP,
  purchase_order_header__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND purchase_order_header__record_updated_at BETWEEN @start_ts AND @end_ts