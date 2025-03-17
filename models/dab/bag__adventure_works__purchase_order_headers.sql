MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__order__purchase
  ),
  tags hook,
  grain (_pit_hook__order__purchase, _hook__order__purchase),
  description 'Hook viewpoint of purchase_order_headers data: General purchase order information. See PurchaseOrderDetail.',
  references (_hook__person__employee, _hook__vendor, _hook__ship_method),
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
    purchase_order_header__record_loaded_at = 'Timestamp when this record was loaded into the system',
    purchase_order_header__record_updated_at = 'Timestamp when this record was last updated',
    purchase_order_header__record_version = 'Version number for this record',
    purchase_order_header__record_valid_from = 'Timestamp from which this record version is valid',
    purchase_order_header__record_valid_to = 'Timestamp until which this record version is valid',
    purchase_order_header__is_current_record = 'Flag indicating if this is the current valid version of the record',
    _hook__order__purchase = 'Reference hook to purchase order',
    _hook__person__employee = 'Reference hook to employee person',
    _hook__vendor = 'Reference hook to vendor',
    _hook__ship_method = 'Reference hook to ship_method',
    _pit_hook__order__purchase = 'Point-in-time hook that combines the primary hook with a validity timestamp'
  )
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
    CONCAT('order__purchase__adventure_works|', purchase_order_header__purchase_order_id) AS _hook__order__purchase,
    CONCAT('person__employee__adventure_works|', purchase_order_header__employee_id) AS _hook__person__employee,
    CONCAT('vendor__adventure_works|', purchase_order_header__vendor_id) AS _hook__vendor,
    CONCAT('ship_method__adventure_works|', purchase_order_header__ship_method_id) AS _hook__ship_method,
    CONCAT_WS('~',
      _hook__order__purchase,
      'epoch__valid_from|'||purchase_order_header__record_valid_from
    ) AS _pit_hook__order__purchase,
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