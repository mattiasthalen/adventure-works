MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references (_pit_hook__address, _pit_hook__bill_of_materials, _pit_hook__business_entity, _pit_hook__credit_card, _pit_hook__currency, _pit_hook__currency_rate, _pit_hook__customer, _pit_hook__department, _pit_hook__job_candidate, _pit_hook__order__purchase, _pit_hook__order__sales, _pit_hook__order__work, _pit_hook__order_line__purchase, _pit_hook__order_line__sales, _pit_hook__order_line__work, _pit_hook__person__employee, _pit_hook__person__individual, _pit_hook__person__sales, _pit_hook__product, _pit_hook__product_category, _pit_hook__product_review, _pit_hook__product_subcategory, _pit_hook__reference__address_type, _pit_hook__reference__contact_type, _pit_hook__reference__country_region, _pit_hook__reference__culture, _pit_hook__reference__illustration, _pit_hook__reference__location, _pit_hook__reference__phone_number_type, _pit_hook__reference__product_description, _pit_hook__reference__product_model, _pit_hook__reference__product_photo, _pit_hook__reference__sales_reason, _pit_hook__reference__sales_tax_rate, _pit_hook__reference__scrap_reason, _pit_hook__reference__shift, _pit_hook__reference__special_offer, _pit_hook__reference__state_province, _pit_hook__reference__unit_measure, _pit_hook__ship_method, _pit_hook__shopping_cart_item, _pit_hook__store, _pit_hook__territory__sales, _pit_hook__transaction_history, _pit_hook__transaction_history_archive, _pit_hook__vendor)
);

WITH cte__bridge_union AS (
  SELECT * FROM silver.uss_bridge__persons
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__employees
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__departments
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__vendors
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__credit_cards
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__ship_methods
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__product_categories
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__currencies
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__currency_rates
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__employee_pay_histories
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__shifts
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__address_types
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__contact_types
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__country_regions
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__email_addresses
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__phone_number_types
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__cultures
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__illustrations
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__locations
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__product_cost_histories
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__product_descriptions
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__product_list_price_histories
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__product_models
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__product_photos
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__scrap_reasons
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__unit_measures
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__special_offers
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__sales_person_quota_histories
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__sales_reasons
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__product_subcategories
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__employee_department_histories
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__business_entity_contacts
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__sales_territories
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__person_phones
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__product_model_illustrations
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__bill_of_materials
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__products
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__job_candidates
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__business_entity_addresses
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__sales_persons
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__sales_territory_histories
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__state_provinces
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__work_orders
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__product_inventories
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__product_reviews
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__transaction_histories
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__transaction_history_archives
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__product_vendors
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__shopping_cart_items
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__stores
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__addresses
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__sales_tax_rates
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__work_order_routings
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__purchase_order_headers
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__customers
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__purchase_order_details
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__sales_order_headers
  UNION ALL BY NAME
  SELECT * FROM silver.uss_bridge__sales_order_details
)
SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  _pit_hook__address::BLOB,
  _pit_hook__bill_of_materials::BLOB,
  _pit_hook__business_entity::BLOB,
  _pit_hook__credit_card::BLOB,
  _pit_hook__currency::BLOB,
  _pit_hook__currency_rate::BLOB,
  _pit_hook__customer::BLOB,
  _pit_hook__department::BLOB,
  _pit_hook__job_candidate::BLOB,
  _pit_hook__order__purchase::BLOB,
  _pit_hook__order__sales::BLOB,
  _pit_hook__order__work::BLOB,
  _pit_hook__order_line__purchase::BLOB,
  _pit_hook__order_line__sales::BLOB,
  _pit_hook__order_line__work::BLOB,
  _pit_hook__person__employee::BLOB,
  _pit_hook__person__individual::BLOB,
  _pit_hook__person__sales::BLOB,
  _pit_hook__product::BLOB,
  _pit_hook__product_category::BLOB,
  _pit_hook__product_review::BLOB,
  _pit_hook__product_subcategory::BLOB,
  _pit_hook__reference__address_type::BLOB,
  _pit_hook__reference__contact_type::BLOB,
  _pit_hook__reference__country_region::BLOB,
  _pit_hook__reference__culture::BLOB,
  _pit_hook__reference__illustration::BLOB,
  _pit_hook__reference__location::BLOB,
  _pit_hook__reference__phone_number_type::BLOB,
  _pit_hook__reference__product_description::BLOB,
  _pit_hook__reference__product_model::BLOB,
  _pit_hook__reference__product_photo::BLOB,
  _pit_hook__reference__sales_reason::BLOB,
  _pit_hook__reference__sales_tax_rate::BLOB,
  _pit_hook__reference__scrap_reason::BLOB,
  _pit_hook__reference__shift::BLOB,
  _pit_hook__reference__special_offer::BLOB,
  _pit_hook__reference__state_province::BLOB,
  _pit_hook__reference__unit_measure::BLOB,
  _pit_hook__ship_method::BLOB,
  _pit_hook__shopping_cart_item::BLOB,
  _pit_hook__store::BLOB,
  _pit_hook__territory__sales::BLOB,
  _pit_hook__transaction_history::BLOB,
  _pit_hook__transaction_history_archive::BLOB,
  _pit_hook__vendor::BLOB,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_union
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts