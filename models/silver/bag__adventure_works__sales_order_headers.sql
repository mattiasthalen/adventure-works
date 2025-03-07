MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__order__sales,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit_hook__order__sales, _hook__order__sales),
  references (_hook__customer, _hook__person__sales, _hook__territory__sales, _hook__address__billing, _hook__address__shipping, _hook__ship_method, _hook__credit_card, _hook__currency)
);

WITH staging AS (
  SELECT
    sales_order_id AS sales_order_header__sales_order_id,
    revision_number AS sales_order_header__revision_number,
    order_date AS sales_order_header__order_date,
    due_date AS sales_order_header__due_date,
    ship_date AS sales_order_header__ship_date,
    status AS sales_order_header__status,
    online_order_flag AS sales_order_header__online_order_flag,
    sales_order_number AS sales_order_header__sales_order_number,
    purchase_order_number AS sales_order_header__purchase_order_number,
    account_number AS sales_order_header__account_number,
    customer_id AS sales_order_header__customer_id,
    sales_person_id AS sales_order_header__sales_person_id,
    territory_id AS sales_order_header__territory_id,
    bill_to_address_id AS sales_order_header__bill_to_address_id,
    ship_to_address_id AS sales_order_header__ship_to_address_id,
    ship_method_id AS sales_order_header__ship_method_id,
    credit_card_id AS sales_order_header__credit_card_id,
    credit_card_approval_code AS sales_order_header__credit_card_approval_code,
    sub_total AS sales_order_header__sub_total,
    tax_amt AS sales_order_header__tax_amt,
    freight AS sales_order_header__freight,
    total_due AS sales_order_header__total_due,
    rowguid AS sales_order_header__rowguid,
    currency_rate_id AS sales_order_header__currency_rate_id,
    modified_date AS sales_order_header__modified_date,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS sales_order_header__record_loaded_at
  FROM bronze.raw__adventure_works__sales_order_headers
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_order_header__sales_order_id ORDER BY sales_order_header__record_loaded_at) AS sales_order_header__record_version,
    CASE
      WHEN sales_order_header__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE sales_order_header__record_loaded_at
    END AS sales_order_header__record_valid_from,
    COALESCE(
      LEAD(sales_order_header__record_loaded_at) OVER (PARTITION BY sales_order_header__sales_order_id ORDER BY sales_order_header__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS sales_order_header__record_valid_to,
    sales_order_header__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS sales_order_header__is_current_record,
    CASE
      WHEN sales_order_header__is_current_record
      THEN sales_order_header__record_loaded_at
      ELSE sales_order_header__record_valid_to
    END AS sales_order_header__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'order__sales__adventure_works|',
      sales_order_header__sales_order_id,
      '~epoch|valid_from|',
      sales_order_header__record_valid_from
    )::BLOB AS _pit_hook__order__sales,
    CONCAT('order__sales__adventure_works|', sales_order_header__sales_order_id) AS _hook__order__sales,
    CONCAT('customer__adventure_works|', sales_order_header__customer_id) AS _hook__customer,
    CONCAT('person__sales__adventure_works|', sales_order_header__sales_person_id) AS _hook__person__sales,
    CONCAT('territory__sales__adventure_works|', sales_order_header__territory_id) AS _hook__territory__sales,
    CONCAT('address__adventure_works|', sales_order_header__bill_to_address_id) AS _hook__address__billing,
    CONCAT('address__adventure_works|', sales_order_header__ship_to_address_id) AS _hook__address__shipping,
    CONCAT('ship_method__adventure_works|', sales_order_header__ship_method_id) AS _hook__ship_method,
    CONCAT('credit_card__adventure_works|', sales_order_header__credit_card_id) AS _hook__credit_card,
    CONCAT('currency__adventure_works|', sales_order_header__currency_rate_id) AS _hook__currency,
    *
  FROM validity
)
SELECT
  _pit_hook__order__sales::BLOB,
  _hook__order__sales::BLOB,
  _hook__customer::BLOB,
  _hook__person__sales::BLOB,
  _hook__territory__sales::BLOB,
  _hook__address__billing::BLOB,
  _hook__address__shipping::BLOB,
  _hook__ship_method::BLOB,
  _hook__credit_card::BLOB,
  _hook__currency::BLOB,
  sales_order_header__sales_order_id::BIGINT,
  sales_order_header__revision_number::BIGINT,
  sales_order_header__order_date::TEXT,
  sales_order_header__due_date::TEXT,
  sales_order_header__ship_date::TEXT,
  sales_order_header__status::BIGINT,
  sales_order_header__online_order_flag::BOOLEAN,
  sales_order_header__sales_order_number::TEXT,
  sales_order_header__purchase_order_number::TEXT,
  sales_order_header__account_number::TEXT,
  sales_order_header__customer_id::BIGINT,
  sales_order_header__sales_person_id::BIGINT,
  sales_order_header__territory_id::BIGINT,
  sales_order_header__bill_to_address_id::BIGINT,
  sales_order_header__ship_to_address_id::BIGINT,
  sales_order_header__ship_method_id::BIGINT,
  sales_order_header__credit_card_id::BIGINT,
  sales_order_header__credit_card_approval_code::TEXT,
  sales_order_header__sub_total::DOUBLE,
  sales_order_header__tax_amt::DOUBLE,
  sales_order_header__freight::DOUBLE,
  sales_order_header__total_due::DOUBLE,
  sales_order_header__rowguid::UUID,
  sales_order_header__currency_rate_id::BIGINT,
  sales_order_header__modified_date::DATE,
  sales_order_header__record_loaded_at::TIMESTAMP,
  sales_order_header__record_updated_at::TIMESTAMP,
  sales_order_header__record_version::TEXT,
  sales_order_header__record_valid_from::TIMESTAMP,
  sales_order_header__record_valid_to::TIMESTAMP,
  sales_order_header__is_current_record::TEXT
FROM hooks
WHERE 1 = 1
AND sales_order_header__record_updated_at BETWEEN @start_ts AND @end_ts