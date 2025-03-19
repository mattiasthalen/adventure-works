MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__order__purchase),
  description 'Business viewpoint of purchase_order_headers data: General purchase order information. See PurchaseOrderDetail.',
  column_descriptions (
    purchase_order_header__purchase_order_id = 'Primary key.',
    purchase_order_header__revision_number = 'Incremental number to track changes to the purchase order over time.',
    purchase_order_header__status = 'Order current status. 1 = Pending; 2 = Approved; 3 = Rejected; 4 = Complete.',
    purchase_order_header__employee_id = 'Employee who created the purchase order. Foreign key to Employee.BusinessEntityID.',
    purchase_order_header__vendor_id = 'Vendor with whom the purchase order is placed. Foreign key to Vendor.BusinessEntityID.',
    purchase_order_header__ship_method_id = 'Shipping method. Foreign key to ShipMethod.ShipMethodID.',
    purchase_order_header__order_date = 'Purchase order creation date.',
    purchase_order_header__ship_date = 'Estimated shipment date from the vendor.',
    purchase_order_header__sub_total = 'Purchase order subtotal. Computed as SUM(PurchaseOrderDetail.LineTotal) for the appropriate PurchaseOrderID.',
    purchase_order_header__tax_amt = 'Tax amount.',
    purchase_order_header__freight = 'Shipping cost.',
    purchase_order_header__total_due = 'Total due to vendor. Computed as Subtotal + TaxAmt + Freight.',
    purchase_order_header__modified_date = 'Date when this record was last modified',
    purchase_order_header__record_loaded_at = 'Timestamp when this record was loaded into the system',
    purchase_order_header__record_updated_at = 'Timestamp when this record was last updated',
    purchase_order_header__record_version = 'Version number for this record',
    purchase_order_header__record_valid_from = 'Timestamp from which this record version is valid',
    purchase_order_header__record_valid_to = 'Timestamp until which this record version is valid',
    purchase_order_header__is_current_record = 'Flag indicating if this is the current valid version of the record'
  )
);

WITH cte__source AS (
  SELECT
    _pit_hook__order__purchase,
    purchase_order_header__purchase_order_id,
    purchase_order_header__revision_number,
    purchase_order_header__status,
    purchase_order_header__employee_id,
    purchase_order_header__vendor_id,
    purchase_order_header__ship_method_id,
    purchase_order_header__order_date,
    purchase_order_header__ship_date,
    purchase_order_header__sub_total,
    purchase_order_header__tax_amt,
    purchase_order_header__freight,
    purchase_order_header__total_due,
    purchase_order_header__modified_date,
    purchase_order_header__record_loaded_at,
    purchase_order_header__record_updated_at,
    purchase_order_header__record_version,
    purchase_order_header__record_valid_from,
    purchase_order_header__record_valid_to,
    purchase_order_header__is_current_record
  FROM dab.bag__adventure_works__purchase_order_headers
),

cte__ghost_record AS (
  SELECT
    'ghost_record' AS _pit_hook__order__purchase,
    NULL AS purchase_order_header__purchase_order_id,
    NULL AS purchase_order_header__revision_number,
    NULL AS purchase_order_header__status,
    NULL AS purchase_order_header__employee_id,
    NULL AS purchase_order_header__vendor_id,
    NULL AS purchase_order_header__ship_method_id,
    NULL AS purchase_order_header__order_date,
    NULL AS purchase_order_header__ship_date,
    NULL AS purchase_order_header__sub_total,
    NULL AS purchase_order_header__tax_amt,
    NULL AS purchase_order_header__freight,
    NULL AS purchase_order_header__total_due,
    NULL AS purchase_order_header__modified_date,
    TIMESTAMP '1970-01-01 00:00:00' AS purchase_order_header__record_loaded_at,
    TIMESTAMP '1970-01-01 00:00:00' AS purchase_order_header__record_updated_at,
    0 AS purchase_order_header__record_version,
    TIMESTAMP '1970-01-01 00:00:00' AS purchase_order_header__record_valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS purchase_order_header__record_valid_to,
    TRUE AS purchase_order_header__is_current_record
  FROM (SELECT 1) dummy
),

cte__final AS (
  SELECT * FROM cte__source
  UNION ALL
  SELECT * FROM cte__ghost_record
)

SELECT
  _pit_hook__order__purchase::BLOB,
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
  purchase_order_header__is_current_record::BOOLEAN
FROM cte__final