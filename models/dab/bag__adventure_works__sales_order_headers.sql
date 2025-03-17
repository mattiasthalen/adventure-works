MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__order__sales
  ),
  tags hook,
  grain (_pit_hook__order__sales, _hook__order__sales),
  description 'Hook viewpoint of sales_order_headers data: General sales order information.',
  references (_hook__customer, _hook__person__sales, _hook__territory__sales, _hook__address__billing, _hook__address__shipping, _hook__ship_method, _hook__credit_card, _hook__currency),
  column_descriptions (
    sales_order_header__sales_order_id = 'Primary key.',
    sales_order_header__revision_number = 'Incremental number to track changes to the sales order over time.',
    sales_order_header__order_date = 'Dates the sales order was created.',
    sales_order_header__due_date = 'Date the order is due to the customer.',
    sales_order_header__ship_date = 'Date the order was shipped to the customer.',
    sales_order_header__status = 'Order current status. 1 = In process; 2 = Approved; 3 = Backordered; 4 = Rejected; 5 = Shipped; 6 = Cancelled.',
    sales_order_header__online_order_flag = '0 = Order placed by sales person. 1 = Order placed online by customer.',
    sales_order_header__sales_order_number = 'Unique sales order identification number.',
    sales_order_header__purchase_order_number = 'Customer purchase order number reference.',
    sales_order_header__account_number = 'Financial accounting number reference.',
    sales_order_header__customer_id = 'Customer identification number. Foreign key to Customer.BusinessEntityID.',
    sales_order_header__sales_person_id = 'Sales person who created the sales order. Foreign key to SalesPerson.BusinessEntityID.',
    sales_order_header__territory_id = 'Territory in which the sale was made. Foreign key to SalesTerritory.SalesTerritoryID.',
    sales_order_header__bill_to_address_id = 'Customer billing address. Foreign key to Address.AddressID.',
    sales_order_header__ship_to_address_id = 'Customer shipping address. Foreign key to Address.AddressID.',
    sales_order_header__ship_method_id = 'Shipping method. Foreign key to ShipMethod.ShipMethodID.',
    sales_order_header__credit_card_id = 'Credit card identification number. Foreign key to CreditCard.CreditCardID.',
    sales_order_header__credit_card_approval_code = 'Approval code provided by the credit card company.',
    sales_order_header__sub_total = 'Sales subtotal. Computed as SUM(SalesOrderDetail.LineTotal) for the appropriate SalesOrderID.',
    sales_order_header__tax_amt = 'Tax amount.',
    sales_order_header__freight = 'Shipping cost.',
    sales_order_header__total_due = 'Total due from customer. Computed as Subtotal + TaxAmt + Freight.',
    sales_order_header__rowguid = 'ROWGUIDCOL number uniquely identifying the record. Used to support a merge replication sample.',
    sales_order_header__currency_rate_id = 'Currency exchange rate used. Foreign key to CurrencyRate.CurrencyRateID.',
    sales_order_header__record_loaded_at = 'Timestamp when this record was loaded into the system',
    sales_order_header__record_updated_at = 'Timestamp when this record was last updated',
    sales_order_header__record_version = 'Version number for this record',
    sales_order_header__record_valid_from = 'Timestamp from which this record version is valid',
    sales_order_header__record_valid_to = 'Timestamp until which this record version is valid',
    sales_order_header__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__order__sales = 'Reference hook to sales order',
    _hook__customer = 'Reference hook to customer',
    _hook__person__sales = 'Reference hook to sales person',
    _hook__territory__sales = 'Reference hook to sales territory',
    _hook__address__billing = 'Reference hook to billing address',
    _hook__address__shipping = 'Reference hook to shipping address',
    _hook__ship_method = 'Reference hook to ship_method',
    _hook__credit_card = 'Reference hook to credit_card',
    _hook__currency = 'Reference hook to currency',
    _pit_hook__order__sales = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
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
  FROM das.raw__adventure_works__sales_order_headers
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
    CONCAT('order__sales__adventure_works|', sales_order_header__sales_order_id) AS _hook__order__sales,
    CONCAT('customer__adventure_works|', sales_order_header__customer_id) AS _hook__customer,
    CONCAT('person__sales__adventure_works|', sales_order_header__sales_person_id) AS _hook__person__sales,
    CONCAT('territory__sales__adventure_works|', sales_order_header__territory_id) AS _hook__territory__sales,
    CONCAT('address__adventure_works|', sales_order_header__bill_to_address_id) AS _hook__address__billing,
    CONCAT('address__adventure_works|', sales_order_header__ship_to_address_id) AS _hook__address__shipping,
    CONCAT('ship_method__adventure_works|', sales_order_header__ship_method_id) AS _hook__ship_method,
    CONCAT('credit_card__adventure_works|', sales_order_header__credit_card_id) AS _hook__credit_card,
    CONCAT('currency__adventure_works|', sales_order_header__currency_rate_id) AS _hook__currency,
    CONCAT_WS('~',
      _hook__order__sales,
      'epoch__valid_from|'||sales_order_header__record_valid_from
    ) AS _pit_hook__order__sales,
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
  sales_order_header__order_date::DATE,
  sales_order_header__due_date::DATE,
  sales_order_header__ship_date::DATE,
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
  sales_order_header__rowguid::TEXT,
  sales_order_header__currency_rate_id::BIGINT,
  sales_order_header__modified_date::DATE,
  sales_order_header__record_loaded_at::TIMESTAMP,
  sales_order_header__record_updated_at::TIMESTAMP,
  sales_order_header__record_version::TEXT,
  sales_order_header__record_valid_from::TIMESTAMP,
  sales_order_header__record_valid_to::TIMESTAMP,
  sales_order_header__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND sales_order_header__record_updated_at BETWEEN @start_ts AND @end_ts