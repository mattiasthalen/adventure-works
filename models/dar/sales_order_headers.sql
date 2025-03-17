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

SELECT
  *
  EXCLUDE (_hook__order__sales, _hook__customer, _hook__person__sales, _hook__territory__sales, _hook__address__billing, _hook__address__shipping, _hook__ship_method, _hook__credit_card, _hook__currency)
FROM dab.bag__adventure_works__sales_order_headers