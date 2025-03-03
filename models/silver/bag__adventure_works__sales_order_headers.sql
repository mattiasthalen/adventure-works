MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    sales_order_id AS sales_order__sales_order_id,
    bill_to_address_id AS sales_order__bill_to_address_id,
    credit_card_id AS sales_order__credit_card_id,
    currency_rate_id AS sales_order__currency_rate_id,
    customer_id AS sales_order__customer_id,
    sales_person_id AS sales_order__sales_person_id,
    ship_method_id AS sales_order__ship_method_id,
    ship_to_address_id AS sales_order__ship_to_address_id,
    territory_id AS sales_order__territory_id,
    account_number AS sales_order__account_number,
    credit_card_approval_code AS sales_order__credit_card_approval_code,
    due_date AS sales_order__due_date,
    freight AS sales_order__freight,
    modified_date AS sales_order__modified_date,
    online_order_flag AS sales_order__online_order_flag,
    order_date AS sales_order__order_date,
    purchase_order_number AS sales_order__purchase_order_number,
    revision_number AS sales_order__revision_number,
    rowguid AS sales_order__rowguid,
    sales_order_number AS sales_order__sales_order_number,
    ship_date AS sales_order__ship_date,
    status AS sales_order__status,
    sub_total AS sales_order__sub_total,
    tax_amt AS sales_order__tax_amt,
    total_due AS sales_order__total_due,
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS sales_order__record_loaded_at
  FROM bronze.raw__adventure_works__sales_order_headers
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY sales_order__sales_order_id ORDER BY sales_order__record_loaded_at) AS sales_order__record_version,
    CASE
      WHEN sales_order__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE sales_order__record_loaded_at
    END AS sales_order__record_valid_from,
    COALESCE(
      LEAD(sales_order__record_loaded_at) OVER (PARTITION BY sales_order__sales_order_id ORDER BY sales_order__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS sales_order__record_valid_to,
    sales_order__record_valid_to = @max_ts::TIMESTAMP AS sales_order__is_current_record,
    CASE
      WHEN sales_order__is_current_record
      THEN sales_order__record_loaded_at
      ELSE sales_order__record_valid_to
    END AS sales_order__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    CONCAT(
      'sales_order|adventure_works|',
      sales_order__sales_order_id,
      '~epoch|valid_from|',
      sales_order__record_valid_from
    ) AS _pit_hook__sales_order,
    CONCAT('sales_order|adventure_works|', sales_order__sales_order_id) AS _hook__sales_order,
    CONCAT('bill_to_address|adventure_works|', sales_order__bill_to_address_id) AS _hook__bill_to_address,
    CONCAT('credit_card|adventure_works|', sales_order__credit_card_id) AS _hook__credit_card,
    CONCAT('currency_rate|adventure_works|', sales_order__currency_rate_id) AS _hook__currency_rate,
    CONCAT('customer|adventure_works|', sales_order__customer_id) AS _hook__customer,
    CONCAT('sales_person|adventure_works|', sales_order__sales_person_id) AS _hook__sales_person,
    CONCAT('ship_method|adventure_works|', sales_order__ship_method_id) AS _hook__ship_method,
    CONCAT('ship_to_address|adventure_works|', sales_order__ship_to_address_id) AS _hook__ship_to_address,
    CONCAT('territory|adventure_works|', sales_order__territory_id) AS _hook__territory,
    *
  FROM validity
)
SELECT
  _pit_hook__sales_order::BLOB,
  _hook__sales_order::BLOB,
  _hook__bill_to_address::BLOB,
  _hook__credit_card::BLOB,
  _hook__currency_rate::BLOB,
  _hook__customer::BLOB,
  _hook__sales_person::BLOB,
  _hook__ship_method::BLOB,
  _hook__ship_to_address::BLOB,
  _hook__territory::BLOB,
  sales_order__sales_order_id::VARCHAR,
  sales_order__bill_to_address_id::VARCHAR,
  sales_order__credit_card_id::VARCHAR,
  sales_order__currency_rate_id::VARCHAR,
  sales_order__customer_id::VARCHAR,
  sales_order__sales_person_id::VARCHAR,
  sales_order__ship_method_id::VARCHAR,
  sales_order__ship_to_address_id::VARCHAR,
  sales_order__territory_id::VARCHAR,
  sales_order__account_number::VARCHAR,
  sales_order__credit_card_approval_code::VARCHAR,
  sales_order__due_date::VARCHAR,
  sales_order__freight::VARCHAR,
  sales_order__modified_date::VARCHAR,
  sales_order__online_order_flag::VARCHAR,
  sales_order__order_date::VARCHAR,
  sales_order__purchase_order_number::VARCHAR,
  sales_order__revision_number::VARCHAR,
  sales_order__rowguid::VARCHAR,
  sales_order__sales_order_number::VARCHAR,
  sales_order__ship_date::VARCHAR,
  sales_order__status::VARCHAR,
  sales_order__sub_total::VARCHAR,
  sales_order__tax_amt::VARCHAR,
  sales_order__total_due::VARCHAR,
  sales_order__record_loaded_at::TIMESTAMP,
  sales_order__record_version::INT,
  sales_order__record_valid_from::TIMESTAMP,
  sales_order__record_valid_to::TIMESTAMP,
  sales_order__is_current_record::BOOLEAN,
  sales_order__record_updated_at::TIMESTAMP
FROM hooks