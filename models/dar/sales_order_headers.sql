MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__order__sales),
  description 'Business viewpoint of sales_order_headers data: General sales order information.',
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
    sales_order_header__modified_date = 'Date when this record was last modified',
    sales_order_header__record_loaded_at = 'Timestamp when this record was loaded into the system',
    sales_order_header__record_updated_at = 'Timestamp when this record was last updated',
    sales_order_header__record_version = 'Version number for this record',
    sales_order_header__record_valid_from = 'Timestamp from which this record version is valid',
    sales_order_header__record_valid_to = 'Timestamp until which this record version is valid',
    sales_order_header__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__order__sales,
    sales_order_header__sales_order_id,
    sales_order_header__revision_number,
    sales_order_header__order_date,
    sales_order_header__due_date,
    sales_order_header__ship_date,
    sales_order_header__status,
    sales_order_header__online_order_flag,
    sales_order_header__sales_order_number,
    sales_order_header__purchase_order_number,
    sales_order_header__account_number,
    sales_order_header__customer_id,
    sales_order_header__sales_person_id,
    sales_order_header__territory_id,
    sales_order_header__bill_to_address_id,
    sales_order_header__ship_to_address_id,
    sales_order_header__ship_method_id,
    sales_order_header__credit_card_id,
    sales_order_header__credit_card_approval_code,
    sales_order_header__sub_total,
    sales_order_header__tax_amt,
    sales_order_header__freight,
    sales_order_header__total_due,
    sales_order_header__rowguid,
    sales_order_header__currency_rate_id,
    sales_order_header__modified_date,
    sales_order_header__record_loaded_at,
    sales_order_header__record_updated_at,
    sales_order_header__record_version,
    sales_order_header__record_valid_from,
    sales_order_header__record_valid_to,
    sales_order_header__is_current_record
  FROM dab.bag__adventure_works__sales_order_headers
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__order__sales,
    NULL AS sales_order_header__sales_order_id,
    NULL AS sales_order_header__revision_number,
    NULL AS sales_order_header__order_date,
    NULL AS sales_order_header__due_date,
    NULL AS sales_order_header__ship_date,
    NULL AS sales_order_header__status,
    FALSE AS sales_order_header__online_order_flag,
    'N/A' AS sales_order_header__sales_order_number,
    'N/A' AS sales_order_header__purchase_order_number,
    'N/A' AS sales_order_header__account_number,
    NULL AS sales_order_header__customer_id,
    NULL AS sales_order_header__sales_person_id,
    NULL AS sales_order_header__territory_id,
    NULL AS sales_order_header__bill_to_address_id,
    NULL AS sales_order_header__ship_to_address_id,
    NULL AS sales_order_header__ship_method_id,
    NULL AS sales_order_header__credit_card_id,
    'N/A' AS sales_order_header__credit_card_approval_code,
    NULL AS sales_order_header__sub_total,
    NULL AS sales_order_header__tax_amt,
    NULL AS sales_order_header__freight,
    NULL AS sales_order_header__total_due,
    'N/A' AS sales_order_header__rowguid,
    NULL AS sales_order_header__currency_rate_id,
    NULL AS sales_order_header__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_order_header__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_order_header__record_updated_at,
    0 AS sales_order_header__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS sales_order_header__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS sales_order_header__record_valid_to,
    TRUE AS sales_order_header__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__order__sales::BLOB,
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
  sales_order_header__is_current_record::BOOLEAN
FROM cte__final
;

@IF(
  @runtime_stage = 'evaluating',
  COPY dar.sales_order_headers TO './export/dar/sales_order_headers.parquet' (FORMAT parquet, COMPRESSION zstd)
);