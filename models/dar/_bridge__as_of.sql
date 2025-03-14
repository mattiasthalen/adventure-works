MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__bridge),
  references (_pit_hook__address, _pit_hook__bill_of_materials, _pit_hook__business_entity, _pit_hook__credit_card, _pit_hook__currency, _pit_hook__currency_rate, _pit_hook__customer, _pit_hook__department, _pit_hook__employee_department_history, _pit_hook__employee_pay_history, _pit_hook__job_candidate, _pit_hook__order__purchase, _pit_hook__order__sales, _pit_hook__order__work, _pit_hook__order_line__purchase, _pit_hook__order_line__sales, _pit_hook__person__employee, _pit_hook__person__individual, _pit_hook__person__sales, _pit_hook__product, _pit_hook__product_category, _pit_hook__product_cost_history, _pit_hook__product_list_price_history, _pit_hook__product_model_illustration, _pit_hook__product_review, _pit_hook__product_subcategory, _pit_hook__product_vendor, _pit_hook__reference__address_type, _pit_hook__reference__contact_type, _pit_hook__reference__country_region, _pit_hook__reference__culture, _pit_hook__reference__illustration, _pit_hook__reference__location, _pit_hook__reference__phone_number_type, _pit_hook__reference__product_description, _pit_hook__reference__product_location, _pit_hook__reference__product_model, _pit_hook__reference__product_photo, _pit_hook__reference__sales_reason, _pit_hook__reference__sales_tax_rate, _pit_hook__reference__scrap_reason, _pit_hook__reference__shift, _pit_hook__reference__special_offer, _pit_hook__reference__state_province, _pit_hook__reference__unit_measure, _pit_hook__ship_method, _pit_hook__shopping_cart_item, _pit_hook__store, _pit_hook__territory__sales, _pit_hook__transaction_history, _pit_hook__transaction_history_archive, _pit_hook__vendor, _pit_hook__work_order_routing, _hook__epoch__date)
);

WITH cte__bridge_union AS (
  SELECT * FROM dar__staging.events__address_types
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__addresses
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__bill_of_materials
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__business_entity_addresses
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__business_entity_contacts
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__contact_types
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__country_regions
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__credit_cards
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__cultures
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__currencies
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__currency_rates
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__customers
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__departments
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__email_addresses
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__employee_department_histories
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__employee_pay_histories
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__employees
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__illustrations
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__job_candidates
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__locations
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__person_phones
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__persons
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__phone_number_types
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__product_categories
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__product_cost_histories
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__product_descriptions
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__product_inventories
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__product_list_price_histories
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__product_model_illustrations
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__product_models
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__product_photos
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__product_reviews
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__product_subcategories
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__product_vendors
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__products
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__purchase_order_details
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__purchase_order_headers
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__sales_order_details
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__sales_order_headers
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__sales_person_quota_histories
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__sales_persons
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__sales_reasons
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__sales_tax_rates
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__sales_territories
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__sales_territory_histories
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__scrap_reasons
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__shifts
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__ship_methods
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__shopping_cart_items
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__special_offers
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__state_provinces
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__stores
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__transaction_histories
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__transaction_history_archives
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__unit_measures
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__vendors
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__work_order_routings
  UNION ALL BY NAME
  SELECT * FROM dar__staging.events__work_orders
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
  _pit_hook__employee_department_history::BLOB,
  _pit_hook__employee_pay_history::BLOB,
  _pit_hook__job_candidate::BLOB,
  _pit_hook__order__purchase::BLOB,
  _pit_hook__order__sales::BLOB,
  _pit_hook__order__work::BLOB,
  _pit_hook__order_line__purchase::BLOB,
  _pit_hook__order_line__sales::BLOB,
  _pit_hook__person__employee::BLOB,
  _pit_hook__person__individual::BLOB,
  _pit_hook__person__sales::BLOB,
  _pit_hook__product::BLOB,
  _pit_hook__product_category::BLOB,
  _pit_hook__product_cost_history::BLOB,
  _pit_hook__product_list_price_history::BLOB,
  _pit_hook__product_model_illustration::BLOB,
  _pit_hook__product_review::BLOB,
  _pit_hook__product_subcategory::BLOB,
  _pit_hook__product_vendor::BLOB,
  _pit_hook__reference__address_type::BLOB,
  _pit_hook__reference__contact_type::BLOB,
  _pit_hook__reference__country_region::BLOB,
  _pit_hook__reference__culture::BLOB,
  _pit_hook__reference__illustration::BLOB,
  _pit_hook__reference__location::BLOB,
  _pit_hook__reference__phone_number_type::BLOB,
  _pit_hook__reference__product_description::BLOB,
  _pit_hook__reference__product_location::BLOB,
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
  _pit_hook__work_order_routing::BLOB,
  _hook__epoch__date::BLOB,
  event__address_types_modified::INT,
  event__addresses_modified::INT,
  event__bill_of_materials_ended::INT,
  event__bill_of_materials_modified::INT,
  event__bill_of_materials_started::INT,
  event__business_entity_addresses_modified::INT,
  event__business_entity_contacts_modified::INT,
  event__contact_types_modified::INT,
  event__country_regions_modified::INT,
  event__credit_cards_modified::INT,
  event__cultures_modified::INT,
  event__currencies_modified::INT,
  event__currency_rates_currency_rate::INT,
  event__currency_rates_modified::INT,
  event__customers_modified::INT,
  event__departments_modified::INT,
  event__email_addresses_modified::INT,
  event__employee_department_histories_ended::INT,
  event__employee_department_histories_modified::INT,
  event__employee_department_histories_started::INT,
  event__employee_pay_histories_modified::INT,
  event__employee_pay_histories_rate_change::INT,
  event__employees_birth::INT,
  event__employees_hire::INT,
  event__employees_modified::INT,
  event__illustrations_modified::INT,
  event__job_candidates_modified::INT,
  event__locations_modified::INT,
  event__person_phones_modified::INT,
  event__persons_modified::INT,
  event__phone_number_types_modified::INT,
  event__product_categories_modified::INT,
  event__product_cost_histories_ended::INT,
  event__product_cost_histories_modified::INT,
  event__product_cost_histories_started::INT,
  event__product_descriptions_modified::INT,
  event__product_inventories_modified::INT,
  event__product_list_price_histories_ended::INT,
  event__product_list_price_histories_modified::INT,
  event__product_list_price_histories_started::INT,
  event__product_model_illustrations_modified::INT,
  event__product_models_modified::INT,
  event__product_photos_modified::INT,
  event__product_reviews_modified::INT,
  event__product_reviews_review::INT,
  event__product_subcategories_modified::INT,
  event__product_vendors_last_receipt::INT,
  event__product_vendors_modified::INT,
  event__products_ended::INT,
  event__products_modified::INT,
  event__products_started::INT,
  event__purchase_order_details_due::INT,
  event__purchase_order_details_modified::INT,
  event__purchase_order_headers_modified::INT,
  event__purchase_order_headers_placed::INT,
  event__purchase_order_headers_shipped::INT,
  event__sales_order_details_modified::INT,
  event__sales_order_headers_due::INT,
  event__sales_order_headers_modified::INT,
  event__sales_order_headers_placed::INT,
  event__sales_order_headers_shipped::INT,
  event__sales_person_quota_histories_modified::INT,
  event__sales_person_quota_histories_quota::INT,
  event__sales_persons_modified::INT,
  event__sales_reasons_modified::INT,
  event__sales_tax_rates_modified::INT,
  event__sales_territories_modified::INT,
  event__sales_territory_histories_ended::INT,
  event__sales_territory_histories_modified::INT,
  event__sales_territory_histories_started::INT,
  event__scrap_reasons_modified::INT,
  event__shifts_modified::INT,
  event__ship_methods_modified::INT,
  event__shopping_cart_items_modified::INT,
  event__special_offers_ended::INT,
  event__special_offers_modified::INT,
  event__special_offers_started::INT,
  event__state_provinces_modified::INT,
  event__stores_modified::INT,
  event__transaction_histories_modified::INT,
  event__transaction_histories_transaction::INT,
  event__transaction_history_archives_modified::INT,
  event__transaction_history_archives_transaction::INT,
  event__unit_measures_modified::INT,
  event__vendors_modified::INT,
  event__work_order_routings_actual_ended::INT,
  event__work_order_routings_actual_started::INT,
  event__work_order_routings_modified::INT,
  event__work_order_routings_scheduled_ended::INT,
  event__work_order_routings_scheduled_started::INT,
  event__work_orders_due::INT,
  event__work_orders_ended::INT,
  event__work_orders_modified::INT,
  event__work_orders_started::INT,
  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_union