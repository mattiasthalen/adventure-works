MODEL (
  kind VIEW,
  enabled FALSE
);

SELECT
  *
FROM gold._bridge__as_of_event
LEFT JOIN gold.addresses
  USING (_pit_hook__address)
LEFT JOIN gold.calendar
  USING (_hook__calendar__date)
LEFT JOIN gold.credit_cards
  USING (_pit_hook__credit_card)
LEFT JOIN gold.currency_rates
  USING (_pit_hook__currency_rate)
LEFT JOIN gold.customers
  USING (_pit_hook__customer)
LEFT JOIN gold.product_categories
  USING (_pit_hook__product_category)
LEFT JOIN gold.product_subcategories
  USING (_pit_hook__product_subcategory)
LEFT JOIN gold.products
  USING (_pit_hook__product)
LEFT JOIN gold.sales_order_details
  USING (_pit_hook__sales_order_detail)
LEFT JOIN gold.sales_order_headers
  USING (_pit_hook__sales_order)
LEFT JOIN gold.sales_persons
  USING (_pit_hook__sales_person)
LEFT JOIN gold.sales_territories
  USING (_pit_hook__territory)
LEFT JOIN gold.ship_methods
  USING (_pit_hook__ship_method)
LEFT JOIN gold.special_offers
  USING (_pit_hook__special_offer)
LEFT JOIN gold.state_provinces
  USING (_pit_hook__state_province)