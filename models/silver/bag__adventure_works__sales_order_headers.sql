MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column sales_order_header__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
    sales_order_id AS sales_order_header__sales_order_id,
    bill_to_address_id AS sales_order_header__bill_to_address_id,
    credit_card_id AS sales_order_header__credit_card_id,
    currency_rate_id AS sales_order_header__currency_rate_id,
    customer_id AS sales_order_header__customer_id,
    sales_person_id AS sales_order_header__sales_person_id,
    ship_method_id AS sales_order_header__ship_method_id,
    ship_to_address_id AS sales_order_header__ship_to_address_id,
    territory_id AS sales_order_header__territory_id,
    account_number AS sales_order_header__account_number,
    credit_card_approval_code AS sales_order_header__credit_card_approval_code,
    due_date AS sales_order_header__due_date,
    freight AS sales_order_header__freight,
    modified_date AS sales_order_header__modified_date,
    online_order_flag AS sales_order_header__online_order_flag,
    order_date AS sales_order_header__order_date,
    purchase_order_number AS sales_order_header__purchase_order_number,
    revision_number AS sales_order_header__revision_number,
    rowguid AS sales_order_header__rowguid,
    sales_order_number AS sales_order_header__sales_order_number,
    ship_date AS sales_order_header__ship_date,
    status AS sales_order_header__status,
    sub_total AS sales_order_header__sub_total,
    tax_amt AS sales_order_header__tax_amt,
    total_due AS sales_order_header__total_due,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS sales_order_header__record_loaded_at
  FROM bronze.raw__adventure_works__sales_order_headers
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_order_header__sales_order_id ORDER BY sales_order_header__record_loaded_at) AS sales_order_header__record_version,
    CASE
      WHEN sales_order_header__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE sales_order_header__record_loaded_at
    END AS sales_order_header__record_valid_from,
    COALESCE(
      LEAD(sales_order_header__record_loaded_at) OVER (PARTITION BY sales_order_header__sales_order_id ORDER BY sales_order_header__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS sales_order_header__record_valid_to,
    sales_order_header__record_valid_to = @max_ts::TIMESTAMP AS sales_order_header__is_current_record,
    CASE
      WHEN sales_order_header__is_current_record
      THEN sales_order_header__record_loaded_at
      ELSE sales_order_header__record_valid_to
    END AS sales_order_header__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'sales_order|adventure_works|',
      sales_order_header__sales_order_id,
      '~epoch|valid_from|',
      sales_order_header__record_valid_from
    ) AS _pit_hook__sales_order,
    CONCAT('sales_order|adventure_works|', sales_order_header__sales_order_id) AS _hook__sales_order,
    CONCAT('customer|adventure_works|', sales_order_header__customer_id) AS _hook__customer,
    CONCAT('sales_person|adventure_works|', sales_order_header__sales_person_id) AS _hook__sales_person,
    CONCAT('territory|adventure_works|', sales_order_header__territory_id) AS _hook__territory,
    CONCAT('address|adventure_works|', sales_order_header__bill_to_address_id) AS _hook__address__bill_to,
    CONCAT('address|adventure_works|', sales_order_header__ship_to_address_id) AS _hook__address__ship_to,
    CONCAT('ship_method|adventure_works|', sales_order_header__ship_method_id) AS _hook__ship_method,
    CONCAT('credit_card|adventure_works|', sales_order_header__credit_card_id) AS _hook__credit_card,
    CONCAT('currency_rate|adventure_works|', sales_order_header__currency_rate_id) AS _hook__currency_rate,
    *
  FROM validity
)
SELECT
  _pit_hook__sales_order::BLOB,
  _hook__sales_order::BLOB,
  _hook__address__bill_to::BLOB,
  _hook__credit_card::BLOB,
  _hook__currency_rate::BLOB,
  _hook__customer::BLOB,
  _hook__sales_person::BLOB,
  _hook__ship_method::BLOB,
  _hook__address__ship_to::BLOB,
  _hook__territory::BLOB,
  sales_order_header__sales_order_id::BIGINT,
  sales_order_header__credit_card_id::BIGINT,
  sales_order_header__currency_rate_id::BIGINT,
  sales_order_header__customer_id::BIGINT,
  sales_order_header__sales_person_id::BIGINT,
  sales_order_header__ship_method_id::BIGINT,
  sales_order_header__territory_id::BIGINT,
  sales_order_header__account_number::VARCHAR,
  sales_order_header__credit_card_approval_code::VARCHAR,
  sales_order_header__due_date::VARCHAR,
  sales_order_header__freight::DOUBLE,
  sales_order_header__modified_date::VARCHAR,
  sales_order_header__online_order_flag::BOOLEAN,
  sales_order_header__order_date::VARCHAR,
  sales_order_header__purchase_order_number::VARCHAR,
  sales_order_header__revision_number::BIGINT,
  sales_order_header__rowguid::VARCHAR,
  sales_order_header__sales_order_number::VARCHAR,
  sales_order_header__ship_date::VARCHAR,
  sales_order_header__status::BIGINT,
  sales_order_header__sub_total::DOUBLE,
  sales_order_header__tax_amt::DOUBLE,
  sales_order_header__total_due::DOUBLE,
  sales_order_header__record_loaded_at::TIMESTAMP,
  sales_order_header__record_updated_at::TIMESTAMP,
  sales_order_header__record_valid_from::TIMESTAMP,
  sales_order_header__record_valid_to::TIMESTAMP,
  sales_order_header__record_version::INT,
  sales_order_header__is_current_record::BOOLEAN
FROM hooks
WHERE 1 = 1
AND sales_order_header__record_updated_at BETWEEN @start_ts AND @end_ts