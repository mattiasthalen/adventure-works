MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags event,
  grain (_pit_hook__bridge),
  references (
    _pit_hook__credit_card,
    _pit_hook__currency,
    _pit_hook__customer,
    _pit_hook__order__sales,
    _pit_hook__order_line__sales,
    _pit_hook__person__sales,
    _pit_hook__product,
    _pit_hook__product_category,
    _pit_hook__product_subcategory,
    _pit_hook__reference__country_region,
    _pit_hook__reference__product_model,
    _pit_hook__reference__special_offer,
    _pit_hook__ship_method,
    _pit_hook__store,
    _pit_hook__territory__sales,
    _hook__epoch__date
  ),
  description 'Event viewpoint of sales_order_details data: Individual products associated with a specific sales order. See SalesOrderHeader.',
  column_descriptions (
    peripheral = 'Name of the sales_order_details peripheral this event relates to',
    _pit_hook__bridge = 'Unique identifier for this event record',
    _hook__epoch__date = 'Hook to the date the event occurred',
    event__sales_order_details_modified = 'Flag indicating a modified event for this sales_order_details',
    event__sales_order_lines_placed = 'Flag indicating a placed event for this sales_order_lines',
    event__sales_order_lines_due = 'Flag indicating a due event for this sales_order_lines',
    event__sales_order_lines_shipped = 'Flag indicating a shipped event for this sales_order_lines',
    event__sales_order_lines_modified = 'Flag indicating a modified event for this sales_order_lines',
    measure__sales_order_line_total_placed = 'Measure of the total sales order line total when the order was placed',
    measure__sales_order_line_total_due = 'Measure of the total sales order line total when the order was due',
    measure__sales_order_line_total_shipped = 'Measure of the total sales order line total when the order was shipped',
    measure__sales_order_line_qty_placed = 'Measure of the quantity of the sales order line when the order was placed',
    measure__sales_order_line_qty_due = 'Measure of the quantity of the sales order line when the order was due',
    measure__sales_order_line_qty_shipped = 'Measure of the quantity of the sales order line when the order was shipped',
    bridge__record_loaded_at = 'Timestamp when this event record was loaded',
    bridge__record_updated_at = 'Timestamp when this event record was last updated',
    bridge__record_valid_from = 'Timestamp from which this event record is valid',
    bridge__record_valid_to = 'Timestamp until which this event record is valid',
    bridge__is_current_record = 'Flag indicating if this is the current valid version of the event record'
  )
);

WITH cte__bridge AS (
  SELECT
    peripheral,
    _pit_hook__bridge,
    _pit_hook__credit_card,
    _pit_hook__currency,
    _pit_hook__customer,
    _pit_hook__order__sales,
    _pit_hook__order_line__sales,
    _pit_hook__person__sales,
    _pit_hook__product,
    _pit_hook__product_category,
    _pit_hook__product_subcategory,
    _pit_hook__reference__country_region,
    _pit_hook__reference__product_model,
    _pit_hook__reference__special_offer,
    _pit_hook__ship_method,
    _pit_hook__store,
    _pit_hook__territory__sales,
    bridge__record_loaded_at,
    bridge__record_updated_at,
    bridge__record_valid_from,
    bridge__record_valid_to,
    bridge__is_current_record
  FROM dar__staging.bridge__sales_order_details
),
cte__events AS (
  SELECT
    *
  FROM cte__bridge
  LEFT JOIN dab.bag__adventure_works__sales_order_headers USING(_pit_hook__order__sales)
  LEFT JOIN dab.bag__adventure_works__sales_order_details USING(_pit_hook__order_line__sales)
),
cte__pivot AS (
  SELECT
    _pit_hook__order_line__sales,
    CONCAT('epoch__date|', event_date) AS _hook__epoch__date,
    MAX(CASE WHEN event = 'sales_order_detail__modified_date' THEN 1 END) AS event__sales_order_details_modified,
    MAX(CASE WHEN event = 'sales_order_header__order_date' THEN 1 END) AS event__sales_order_lines_placed,
    MAX(CASE WHEN event = 'sales_order_header__due_date' THEN 1 END) AS event__sales_order_lines_due,
    MAX(CASE WHEN event = 'sales_order_header__ship_date' THEN 1 END) AS event__sales_order_lines_shipped,
    MAX(CASE WHEN event = 'sales_order_header__modified_date' THEN 1 END) AS event__sales_order_lines_modified,
    MAX(CASE WHEN event = 'sales_order_header__order_date' THEN sales_order_detail__line_total END) AS measure__sales_order_line_total_placed,
    MAX(CASE WHEN event = 'sales_order_header__due_date' THEN sales_order_detail__line_total END) AS measure__sales_order_line_total_due,
    MAX(CASE WHEN event = 'sales_order_header__ship_date' THEN sales_order_detail__line_total END) AS measure__sales_order_line_total_shipped,
    MAX(CASE WHEN event = 'sales_order_header__order_date' THEN sales_order_detail__order_qty END) AS measure__sales_order_line_qty_placed,
    MAX(CASE WHEN event = 'sales_order_header__due_date' THEN sales_order_detail__order_qty END) AS measure__sales_order_line_qty_due,
    MAX(CASE WHEN event = 'sales_order_header__ship_date' THEN sales_order_detail__order_qty END) AS measure__sales_order_line_qty_shipped
  FROM cte__events
  UNPIVOT (
    event_date FOR event IN (
      sales_order_detail__modified_date,
      sales_order_header__order_date,
      sales_order_header__due_date,
      sales_order_header__ship_date,
      sales_order_header__modified_date
    )
  ) AS pivot__events
  GROUP BY ALL
  ORDER BY _hook__epoch__date
),
final AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      _pit_hook__bridge::TEXT,
      _hook__epoch__date::TEXT
    ) AS _pit_hook__bridge
  FROM cte__bridge
  LEFT JOIN cte__pivot USING(_pit_hook__order_line__sales)
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__credit_card::BLOB,
  _pit_hook__currency::BLOB,
  _pit_hook__customer::BLOB,
  _pit_hook__order__sales::BLOB,
  _pit_hook__order_line__sales::BLOB,
  _pit_hook__person__sales::BLOB,
  _pit_hook__product::BLOB,
  _pit_hook__product_category::BLOB,
  _pit_hook__product_subcategory::BLOB,
  _pit_hook__reference__country_region::BLOB,
  _pit_hook__reference__product_model::BLOB,
  _pit_hook__reference__special_offer::BLOB,
  _pit_hook__ship_method::BLOB,
  _pit_hook__store::BLOB,
  _pit_hook__territory__sales::BLOB,
  _hook__epoch__date::BLOB,
  event__sales_order_details_modified::INT,
  event__sales_order_lines_placed::INT,
  event__sales_order_lines_due::INT,
  event__sales_order_lines_shipped::INT,
  event__sales_order_lines_modified::INT,
  measure__sales_order_line_total_placed::DECIMAL,
  measure__sales_order_line_total_due::DECIMAL,
  measure__sales_order_line_total_shipped::DECIMAL,
  measure__sales_order_line_qty_placed::BIGINT,
  measure__sales_order_line_qty_due::BIGINT,
  measure__sales_order_line_qty_shipped::BIGINT,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM final
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts