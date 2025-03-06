# Serverless Lakehouse

This project utilizes dlt, DuckDB, and SQLMesh, to create a serverless lakehouse by:
1. Extracting data from source via dlt.
2. Loading the data to delta files.
3. Reading the bronze using DuckDB.
4. Transforming the data using SQLMesh.
5. Extracting silver & gold from DuckDB with dlt.
6. Loading silver & gold to delta files.

It does this locally into `./lakehouse`, which could be replaced by a S3 bucket.

## Streamlit Dashboard
<img width="1440" alt="image" src="https://github.com/user-attachments/assets/61f70e7b-6f0d-46a3-ba14-2f516dae9575" />

## Architecture
```mermaid
architecture-beta
    service api(cloud)[Adventure Works API]
    service extract(server)[dlt]
    service load(server)[SQLMesh]
    service export_silver(server)[dlt]
    service export_gold(server)[dlt]
    service consumption(cloud)[BI]

    group storage(cloud)[Storage]
        service bronze(disk)[Bronze] in storage
        service silver(disk)[Silver] in storage
        service gold(disk)[Gold] in storage

    group engine(database)[DuckDB]
        service bronze_view(database)[Bronze] in engine
        service l1_transform(server)[SQLMesh] in engine
        service silver_view(database)[Silver] in engine
        service l2_transform(server)[SQLMesh] in engine
        service gold_view(database)[Gold] in engine

    api:R -- L:extract
    extract:R -- L:bronze
    bronze:T -- B:load
    load:T -- B:bronze_view
    bronze_view:R -- L:l1_transform
    l1_transform:R -- L:silver_view
    silver_view:R -- L:l2_transform
    l2_transform:R -- L:gold_view
    silver_view:B -- T:export_silver
    export_silver:B -- T:silver
    gold_view:B -- T:export_gold
    export_gold:B -- T:gold
    gold:R -- L:consumption
```

## Lineage / DAG
```mermaid
flowchart LR
    subgraph db.bronze["db.bronze"]
        direction LR
        raw__adventure_works__address_types(["raw__adventure_works__address_types"])
        raw__adventure_works__addresses(["raw__adventure_works__addresses"])
        raw__adventure_works__bill_of_materials(["raw__adventure_works__bill_of_materials"])
        raw__adventure_works__business_entity_addresses(["raw__adventure_works__business_entity_addresses"])
        raw__adventure_works__business_entity_contacts(["raw__adventure_works__business_entity_contacts"])
        raw__adventure_works__contact_types(["raw__adventure_works__contact_types"])
        raw__adventure_works__country_regions(["raw__adventure_works__country_regions"])
        raw__adventure_works__credit_cards(["raw__adventure_works__credit_cards"])
        raw__adventure_works__cultures(["raw__adventure_works__cultures"])
        raw__adventure_works__currencies(["raw__adventure_works__currencies"])
        raw__adventure_works__currency_rates(["raw__adventure_works__currency_rates"])
        raw__adventure_works__customers(["raw__adventure_works__customers"])
        raw__adventure_works__departments(["raw__adventure_works__departments"])
        raw__adventure_works__email_addresses(["raw__adventure_works__email_addresses"])
        raw__adventure_works__employee_department_histories(["raw__adventure_works__employee_department_histories"])
        raw__adventure_works__employee_pay_histories(["raw__adventure_works__employee_pay_histories"])
        raw__adventure_works__employees(["raw__adventure_works__employees"])
        raw__adventure_works__illustrations(["raw__adventure_works__illustrations"])
        raw__adventure_works__job_candidates(["raw__adventure_works__job_candidates"])
        raw__adventure_works__locations(["raw__adventure_works__locations"])
        raw__adventure_works__person_phones(["raw__adventure_works__person_phones"])
        raw__adventure_works__persons(["raw__adventure_works__persons"])
        raw__adventure_works__phone_number_types(["raw__adventure_works__phone_number_types"])
        raw__adventure_works__product_categories(["raw__adventure_works__product_categories"])
        raw__adventure_works__product_cost_histories(["raw__adventure_works__product_cost_histories"])
        raw__adventure_works__product_descriptions(["raw__adventure_works__product_descriptions"])
        raw__adventure_works__product_inventories(["raw__adventure_works__product_inventories"])
        raw__adventure_works__product_list_price_histories(["raw__adventure_works__product_list_price_histories"])
        raw__adventure_works__product_model_illustrations(["raw__adventure_works__product_model_illustrations"])
        raw__adventure_works__product_models(["raw__adventure_works__product_models"])
        raw__adventure_works__product_photos(["raw__adventure_works__product_photos"])
        raw__adventure_works__product_reviews(["raw__adventure_works__product_reviews"])
        raw__adventure_works__product_subcategories(["raw__adventure_works__product_subcategories"])
        raw__adventure_works__product_vendors(["raw__adventure_works__product_vendors"])
        raw__adventure_works__products(["raw__adventure_works__products"])
        raw__adventure_works__purchase_order_details(["raw__adventure_works__purchase_order_details"])
        raw__adventure_works__purchase_order_headers(["raw__adventure_works__purchase_order_headers"])
        raw__adventure_works__sales_order_details(["raw__adventure_works__sales_order_details"])
        raw__adventure_works__sales_order_headers(["raw__adventure_works__sales_order_headers"])
        raw__adventure_works__sales_person_quota_histories(["raw__adventure_works__sales_person_quota_histories"])
        raw__adventure_works__sales_persons(["raw__adventure_works__sales_persons"])
        raw__adventure_works__sales_reasons(["raw__adventure_works__sales_reasons"])
        raw__adventure_works__sales_tax_rates(["raw__adventure_works__sales_tax_rates"])
        raw__adventure_works__sales_territories(["raw__adventure_works__sales_territories"])
        raw__adventure_works__sales_territory_histories(["raw__adventure_works__sales_territory_histories"])
        raw__adventure_works__scrap_reasons(["raw__adventure_works__scrap_reasons"])
        raw__adventure_works__shifts(["raw__adventure_works__shifts"])
        raw__adventure_works__ship_methods(["raw__adventure_works__ship_methods"])
        raw__adventure_works__shopping_cart_items(["raw__adventure_works__shopping_cart_items"])
        raw__adventure_works__special_offers(["raw__adventure_works__special_offers"])
        raw__adventure_works__state_provinces(["raw__adventure_works__state_provinces"])
        raw__adventure_works__stores(["raw__adventure_works__stores"])
        raw__adventure_works__transaction_histories(["raw__adventure_works__transaction_histories"])
        raw__adventure_works__transaction_history_archives(["raw__adventure_works__transaction_history_archives"])
        raw__adventure_works__unit_measures(["raw__adventure_works__unit_measures"])
        raw__adventure_works__vendors(["raw__adventure_works__vendors"])
        raw__adventure_works__work_order_routings(["raw__adventure_works__work_order_routings"])
        raw__adventure_works__work_orders(["raw__adventure_works__work_orders"])
    end

    subgraph db.silver["db.silver"]
        direction LR
        subgraph bags
            bag__adventure_works__address_types(["bag__adventure_works__address_types"])
            bag__adventure_works__addresses(["bag__adventure_works__addresses"])
            bag__adventure_works__bill_of_materials(["bag__adventure_works__bill_of_materials"])
            bag__adventure_works__business_entity_addresses(["bag__adventure_works__business_entity_addresses"])
            bag__adventure_works__business_entity_contacts(["bag__adventure_works__business_entity_contacts"])
            bag__adventure_works__contact_types(["bag__adventure_works__contact_types"])
            bag__adventure_works__country_regions(["bag__adventure_works__country_regions"])
            bag__adventure_works__credit_cards(["bag__adventure_works__credit_cards"])
            bag__adventure_works__cultures(["bag__adventure_works__cultures"])
            bag__adventure_works__currencies(["bag__adventure_works__currencies"])
            bag__adventure_works__currency_rates(["bag__adventure_works__currency_rates"])
            bag__adventure_works__customers(["bag__adventure_works__customers"])
            bag__adventure_works__departments(["bag__adventure_works__departments"])
            bag__adventure_works__email_addresses(["bag__adventure_works__email_addresses"])
            bag__adventure_works__employee_department_histories(["bag__adventure_works__employee_department_histories"])
            bag__adventure_works__employee_pay_histories(["bag__adventure_works__employee_pay_histories"])
            bag__adventure_works__employees(["bag__adventure_works__employees"])
            bag__adventure_works__illustrations(["bag__adventure_works__illustrations"])
            bag__adventure_works__job_candidates(["bag__adventure_works__job_candidates"])
            bag__adventure_works__locations(["bag__adventure_works__locations"])
            bag__adventure_works__person_phones(["bag__adventure_works__person_phones"])
            bag__adventure_works__persons(["bag__adventure_works__persons"])
            bag__adventure_works__phone_number_types(["bag__adventure_works__phone_number_types"])
            bag__adventure_works__product_categories(["bag__adventure_works__product_categories"])
            bag__adventure_works__product_cost_histories(["bag__adventure_works__product_cost_histories"])
            bag__adventure_works__product_descriptions(["bag__adventure_works__product_descriptions"])
            bag__adventure_works__product_inventories(["bag__adventure_works__product_inventories"])
            bag__adventure_works__product_list_price_histories(["bag__adventure_works__product_list_price_histories"])
            bag__adventure_works__product_model_illustrations(["bag__adventure_works__product_model_illustrations"])
            bag__adventure_works__product_models(["bag__adventure_works__product_models"])
            bag__adventure_works__product_photos(["bag__adventure_works__product_photos"])
            bag__adventure_works__product_reviews(["bag__adventure_works__product_reviews"])
            bag__adventure_works__product_subcategories(["bag__adventure_works__product_subcategories"])
            bag__adventure_works__product_vendors(["bag__adventure_works__product_vendors"])
            bag__adventure_works__products(["bag__adventure_works__products"])
            bag__adventure_works__purchase_order_details(["bag__adventure_works__purchase_order_details"])
            bag__adventure_works__purchase_order_headers(["bag__adventure_works__purchase_order_headers"])
            bag__adventure_works__sales_order_details(["bag__adventure_works__sales_order_details"])
            bag__adventure_works__sales_order_headers(["bag__adventure_works__sales_order_headers"])
            bag__adventure_works__sales_person_quota_histories(["bag__adventure_works__sales_person_quota_histories"])
            bag__adventure_works__sales_persons(["bag__adventure_works__sales_persons"])
            bag__adventure_works__sales_reasons(["bag__adventure_works__sales_reasons"])
            bag__adventure_works__sales_tax_rates(["bag__adventure_works__sales_tax_rates"])
            bag__adventure_works__sales_territories(["bag__adventure_works__sales_territories"])
            bag__adventure_works__sales_territory_histories(["bag__adventure_works__sales_territory_histories"])
            bag__adventure_works__scrap_reasons(["bag__adventure_works__scrap_reasons"])
            bag__adventure_works__shifts(["bag__adventure_works__shifts"])
            bag__adventure_works__ship_methods(["bag__adventure_works__ship_methods"])
            bag__adventure_works__shopping_cart_items(["bag__adventure_works__shopping_cart_items"])
            bag__adventure_works__special_offers(["bag__adventure_works__special_offers"])
            bag__adventure_works__state_provinces(["bag__adventure_works__state_provinces"])
            bag__adventure_works__stores(["bag__adventure_works__stores"])
            bag__adventure_works__transaction_histories(["bag__adventure_works__transaction_histories"])
            bag__adventure_works__transaction_history_archives(["bag__adventure_works__transaction_history_archives"])
            bag__adventure_works__unit_measures(["bag__adventure_works__unit_measures"])
            bag__adventure_works__vendors(["bag__adventure_works__vendors"])
            bag__adventure_works__work_order_routings(["bag__adventure_works__work_order_routings"])
            bag__adventure_works__work_orders(["bag__adventure_works__work_orders"])
        end
        uss_bridge(["uss_bridge"])
        uss_bridge__address_types(["uss_bridge__address_types"])
        uss_bridge__addresses(["uss_bridge__addresses"])
        uss_bridge__bill_of_materials(["uss_bridge__bill_of_materials"])
        uss_bridge__business_entity_addresses(["uss_bridge__business_entity_addresses"])
        uss_bridge__business_entity_contacts(["uss_bridge__business_entity_contacts"])
        uss_bridge__contact_types(["uss_bridge__contact_types"])
        uss_bridge__country_regions(["uss_bridge__country_regions"])
        uss_bridge__credit_cards(["uss_bridge__credit_cards"])
        uss_bridge__cultures(["uss_bridge__cultures"])
        uss_bridge__currencies(["uss_bridge__currencies"])
        uss_bridge__currency_rates(["uss_bridge__currency_rates"])
        uss_bridge__customers(["uss_bridge__customers"])
        uss_bridge__departments(["uss_bridge__departments"])
        uss_bridge__email_addresses(["uss_bridge__email_addresses"])
        uss_bridge__employee_department_histories(["uss_bridge__employee_department_histories"])
        uss_bridge__employee_pay_histories(["uss_bridge__employee_pay_histories"])
        uss_bridge__employees(["uss_bridge__employees"])
        uss_bridge__illustrations(["uss_bridge__illustrations"])
        uss_bridge__job_candidates(["uss_bridge__job_candidates"])
        uss_bridge__locations(["uss_bridge__locations"])
        uss_bridge__person_phones(["uss_bridge__person_phones"])
        uss_bridge__persons(["uss_bridge__persons"])
        uss_bridge__phone_number_types(["uss_bridge__phone_number_types"])
        uss_bridge__product_categories(["uss_bridge__product_categories"])
        uss_bridge__product_cost_histories(["uss_bridge__product_cost_histories"])
        uss_bridge__product_descriptions(["uss_bridge__product_descriptions"])
        uss_bridge__product_inventories(["uss_bridge__product_inventories"])
        uss_bridge__product_list_price_histories(["uss_bridge__product_list_price_histories"])
        uss_bridge__product_model_illustrations(["uss_bridge__product_model_illustrations"])
        uss_bridge__product_models(["uss_bridge__product_models"])
        uss_bridge__product_photos(["uss_bridge__product_photos"])
        uss_bridge__product_reviews(["uss_bridge__product_reviews"])
        uss_bridge__product_subcategories(["uss_bridge__product_subcategories"])
        uss_bridge__product_vendors(["uss_bridge__product_vendors"])
        uss_bridge__products(["uss_bridge__products"])
        uss_bridge__purchase_order_details(["uss_bridge__purchase_order_details"])
        uss_bridge__purchase_order_headers(["uss_bridge__purchase_order_headers"])
        uss_bridge__sales_order_details(["uss_bridge__sales_order_details"])
        uss_bridge__sales_order_headers(["uss_bridge__sales_order_headers"])
        uss_bridge__sales_person_quota_histories(["uss_bridge__sales_person_quota_histories"])
        uss_bridge__sales_persons(["uss_bridge__sales_persons"])
        uss_bridge__sales_reasons(["uss_bridge__sales_reasons"])
        uss_bridge__sales_tax_rates(["uss_bridge__sales_tax_rates"])
        uss_bridge__sales_territories(["uss_bridge__sales_territories"])
        uss_bridge__sales_territory_histories(["uss_bridge__sales_territory_histories"])
        uss_bridge__scrap_reasons(["uss_bridge__scrap_reasons"])
        uss_bridge__shifts(["uss_bridge__shifts"])
        uss_bridge__ship_methods(["uss_bridge__ship_methods"])
        uss_bridge__shopping_cart_items(["uss_bridge__shopping_cart_items"])
        uss_bridge__special_offers(["uss_bridge__special_offers"])
        uss_bridge__state_provinces(["uss_bridge__state_provinces"])
        uss_bridge__stores(["uss_bridge__stores"])
        uss_bridge__transaction_histories(["uss_bridge__transaction_histories"])
        uss_bridge__transaction_history_archives(["uss_bridge__transaction_history_archives"])
        uss_bridge__unit_measures(["uss_bridge__unit_measures"])
        uss_bridge__vendors(["uss_bridge__vendors"])
        uss_bridge__work_order_routings(["uss_bridge__work_order_routings"])
        uss_bridge__work_orders(["uss_bridge__work_orders"])
    end

    subgraph db.gold["db.gold"]
        direction LR
        _bridge__as_of(["_bridge__as_of"])
        _one_big_table__as_of(["_one_big_table__as_of"])
        address_types(["address_types"])
        addresses(["addresses"])
        bill_of_materials(["bill_of_materials"])
        business_entity_addresses(["business_entity_addresses"])
        business_entity_contacts(["business_entity_contacts"])
        contact_types(["contact_types"])
        country_regions(["country_regions"])
        credit_cards(["credit_cards"])
        cultures(["cultures"])
        currencies(["currencies"])
        currency_rates(["currency_rates"])
        customers(["customers"])
        departments(["departments"])
        email_addresses(["email_addresses"])
        employee_department_histories(["employee_department_histories"])
        employee_pay_histories(["employee_pay_histories"])
        employees(["employees"])
        illustrations(["illustrations"])
        job_candidates(["job_candidates"])
        locations(["locations"])
        person_phones(["person_phones"])
        persons(["persons"])
        phone_number_types(["phone_number_types"])
        product_categories(["product_categories"])
        product_cost_histories(["product_cost_histories"])
        product_descriptions(["product_descriptions"])
        product_inventories(["product_inventories"])
        product_list_price_histories(["product_list_price_histories"])
        product_model_illustrations(["product_model_illustrations"])
        product_models(["product_models"])
        product_photos(["product_photos"])
        product_reviews(["product_reviews"])
        product_subcategories(["product_subcategories"])
        product_vendors(["product_vendors"])
        products(["products"])
        purchase_order_details(["purchase_order_details"])
        purchase_order_headers(["purchase_order_headers"])
        sales_order_details(["sales_order_details"])
        sales_order_headers(["sales_order_headers"])
        sales_person_quota_histories(["sales_person_quota_histories"])
        sales_persons(["sales_persons"])
        sales_reasons(["sales_reasons"])
        sales_tax_rates(["sales_tax_rates"])
        sales_territories(["sales_territories"])
        sales_territory_histories(["sales_territory_histories"])
        scrap_reasons(["scrap_reasons"])
        shifts(["shifts"])
        ship_methods(["ship_methods"])
        shopping_cart_items(["shopping_cart_items"])
        special_offers(["special_offers"])
        state_provinces(["state_provinces"])
        stores(["stores"])
        transaction_histories(["transaction_histories"])
        transaction_history_archives(["transaction_history_archives"])
        unit_measures(["unit_measures"])
        vendors(["vendors"])
        work_order_routings(["work_order_routings"])
        work_orders(["work_orders"])
    end

    %% db.bronze -> db.silver
    raw__adventure_works__address_types --> bag__adventure_works__address_types
    raw__adventure_works__addresses --> bag__adventure_works__addresses
    raw__adventure_works__bill_of_materials --> bag__adventure_works__bill_of_materials
    raw__adventure_works__business_entity_addresses --> bag__adventure_works__business_entity_addresses
    raw__adventure_works__business_entity_contacts --> bag__adventure_works__business_entity_contacts
    raw__adventure_works__contact_types --> bag__adventure_works__contact_types
    raw__adventure_works__country_regions --> bag__adventure_works__country_regions
    raw__adventure_works__credit_cards --> bag__adventure_works__credit_cards
    raw__adventure_works__cultures --> bag__adventure_works__cultures
    raw__adventure_works__currencies --> bag__adventure_works__currencies
    raw__adventure_works__currency_rates --> bag__adventure_works__currency_rates
    raw__adventure_works__customers --> bag__adventure_works__customers
    raw__adventure_works__departments --> bag__adventure_works__departments
    raw__adventure_works__email_addresses --> bag__adventure_works__email_addresses
    raw__adventure_works__employee_department_histories --> bag__adventure_works__employee_department_histories
    raw__adventure_works__employee_pay_histories --> bag__adventure_works__employee_pay_histories
    raw__adventure_works__employees --> bag__adventure_works__employees
    raw__adventure_works__illustrations --> bag__adventure_works__illustrations
    raw__adventure_works__job_candidates --> bag__adventure_works__job_candidates
    raw__adventure_works__locations --> bag__adventure_works__locations
    raw__adventure_works__person_phones --> bag__adventure_works__person_phones
    raw__adventure_works__persons --> bag__adventure_works__persons
    raw__adventure_works__phone_number_types --> bag__adventure_works__phone_number_types
    raw__adventure_works__product_categories --> bag__adventure_works__product_categories
    raw__adventure_works__product_cost_histories --> bag__adventure_works__product_cost_histories
    raw__adventure_works__product_descriptions --> bag__adventure_works__product_descriptions
    raw__adventure_works__product_inventories --> bag__adventure_works__product_inventories
    raw__adventure_works__product_list_price_histories --> bag__adventure_works__product_list_price_histories
    raw__adventure_works__product_model_illustrations --> bag__adventure_works__product_model_illustrations
    raw__adventure_works__product_models --> bag__adventure_works__product_models
    raw__adventure_works__product_photos --> bag__adventure_works__product_photos
    raw__adventure_works__product_reviews --> bag__adventure_works__product_reviews
    raw__adventure_works__product_subcategories --> bag__adventure_works__product_subcategories
    raw__adventure_works__product_vendors --> bag__adventure_works__product_vendors
    raw__adventure_works__products --> bag__adventure_works__products
    raw__adventure_works__purchase_order_details --> bag__adventure_works__purchase_order_details
    raw__adventure_works__purchase_order_headers --> bag__adventure_works__purchase_order_headers
    raw__adventure_works__sales_order_details --> bag__adventure_works__sales_order_details
    raw__adventure_works__sales_order_headers --> bag__adventure_works__sales_order_headers
    raw__adventure_works__sales_person_quota_histories --> bag__adventure_works__sales_person_quota_histories
    raw__adventure_works__sales_persons --> bag__adventure_works__sales_persons
    raw__adventure_works__sales_reasons --> bag__adventure_works__sales_reasons
    raw__adventure_works__sales_tax_rates --> bag__adventure_works__sales_tax_rates
    raw__adventure_works__sales_territories --> bag__adventure_works__sales_territories
    raw__adventure_works__sales_territory_histories --> bag__adventure_works__sales_territory_histories
    raw__adventure_works__scrap_reasons --> bag__adventure_works__scrap_reasons
    raw__adventure_works__shifts --> bag__adventure_works__shifts
    raw__adventure_works__ship_methods --> bag__adventure_works__ship_methods
    raw__adventure_works__shopping_cart_items --> bag__adventure_works__shopping_cart_items
    raw__adventure_works__special_offers --> bag__adventure_works__special_offers
    raw__adventure_works__state_provinces --> bag__adventure_works__state_provinces
    raw__adventure_works__stores --> bag__adventure_works__stores
    raw__adventure_works__transaction_histories --> bag__adventure_works__transaction_histories
    raw__adventure_works__transaction_history_archives --> bag__adventure_works__transaction_history_archives
    raw__adventure_works__unit_measures --> bag__adventure_works__unit_measures
    raw__adventure_works__vendors --> bag__adventure_works__vendors
    raw__adventure_works__work_order_routings --> bag__adventure_works__work_order_routings
    raw__adventure_works__work_orders --> bag__adventure_works__work_orders

    %% db.silver -> db.silver
    bag__adventure_works__address_types --> uss_bridge__address_types
    bag__adventure_works__addresses --> uss_bridge__addresses
    bag__adventure_works__bill_of_materials --> uss_bridge__bill_of_materials
    bag__adventure_works__business_entity_addresses --> uss_bridge__business_entity_addresses
    bag__adventure_works__business_entity_contacts --> uss_bridge__business_entity_contacts
    bag__adventure_works__contact_types --> uss_bridge__contact_types
    bag__adventure_works__country_regions --> uss_bridge__country_regions
    bag__adventure_works__credit_cards --> uss_bridge__credit_cards
    bag__adventure_works__cultures --> uss_bridge__cultures
    bag__adventure_works__currencies --> uss_bridge__currencies
    bag__adventure_works__currency_rates --> uss_bridge__currency_rates
    bag__adventure_works__customers --> uss_bridge__customers
    bag__adventure_works__departments --> uss_bridge__departments
    bag__adventure_works__email_addresses --> uss_bridge__email_addresses
    bag__adventure_works__employee_department_histories --> uss_bridge__employee_department_histories
    bag__adventure_works__employee_pay_histories --> uss_bridge__employee_pay_histories
    bag__adventure_works__employees --> uss_bridge__employees
    bag__adventure_works__illustrations --> uss_bridge__illustrations
    bag__adventure_works__job_candidates --> uss_bridge__job_candidates
    bag__adventure_works__locations --> uss_bridge__locations
    bag__adventure_works__person_phones --> uss_bridge__person_phones
    bag__adventure_works__persons --> uss_bridge__persons
    bag__adventure_works__phone_number_types --> uss_bridge__phone_number_types
    bag__adventure_works__product_categories --> uss_bridge__product_categories
    bag__adventure_works__product_cost_histories --> uss_bridge__product_cost_histories
    bag__adventure_works__product_descriptions --> uss_bridge__product_descriptions
    bag__adventure_works__product_inventories --> uss_bridge__product_inventories
    bag__adventure_works__product_list_price_histories --> uss_bridge__product_list_price_histories
    bag__adventure_works__product_model_illustrations --> uss_bridge__product_model_illustrations
    bag__adventure_works__product_models --> uss_bridge__product_models
    bag__adventure_works__product_photos --> uss_bridge__product_photos
    bag__adventure_works__product_reviews --> uss_bridge__product_reviews
    bag__adventure_works__product_subcategories --> uss_bridge__product_subcategories
    bag__adventure_works__product_vendors --> uss_bridge__product_vendors
    bag__adventure_works__products --> uss_bridge__products
    bag__adventure_works__purchase_order_details --> uss_bridge__purchase_order_details
    bag__adventure_works__purchase_order_headers --> uss_bridge__purchase_order_headers
    bag__adventure_works__sales_order_details --> uss_bridge__sales_order_details
    bag__adventure_works__sales_order_headers --> uss_bridge__sales_order_headers
    bag__adventure_works__sales_person_quota_histories --> uss_bridge__sales_person_quota_histories
    bag__adventure_works__sales_persons --> uss_bridge__sales_persons
    bag__adventure_works__sales_reasons --> uss_bridge__sales_reasons
    bag__adventure_works__sales_tax_rates --> uss_bridge__sales_tax_rates
    bag__adventure_works__sales_territories --> uss_bridge__sales_territories
    bag__adventure_works__sales_territory_histories --> uss_bridge__sales_territory_histories
    bag__adventure_works__scrap_reasons --> uss_bridge__scrap_reasons
    bag__adventure_works__shifts --> uss_bridge__shifts
    bag__adventure_works__ship_methods --> uss_bridge__ship_methods
    bag__adventure_works__shopping_cart_items --> uss_bridge__shopping_cart_items
    bag__adventure_works__special_offers --> uss_bridge__special_offers
    bag__adventure_works__state_provinces --> uss_bridge__state_provinces
    bag__adventure_works__stores --> uss_bridge__stores
    bag__adventure_works__transaction_histories --> uss_bridge__transaction_histories
    bag__adventure_works__transaction_history_archives --> uss_bridge__transaction_history_archives
    bag__adventure_works__unit_measures --> uss_bridge__unit_measures
    bag__adventure_works__vendors --> uss_bridge__vendors
    bag__adventure_works__work_order_routings --> uss_bridge__work_order_routings
    bag__adventure_works__work_orders --> uss_bridge__work_orders
    uss_bridge__address_types --> uss_bridge
    uss_bridge__address_types --> uss_bridge__business_entity_addresses
    uss_bridge__addresses --> uss_bridge
    uss_bridge__bill_of_materials --> uss_bridge
    uss_bridge__business_entity_addresses --> uss_bridge
    uss_bridge__business_entity_contacts --> uss_bridge
    uss_bridge__business_entity_contacts --> uss_bridge__business_entity_addresses
    uss_bridge__contact_types --> uss_bridge
    uss_bridge__contact_types --> uss_bridge__business_entity_contacts
    uss_bridge__country_regions --> uss_bridge
    uss_bridge__country_regions --> uss_bridge__sales_territories
    uss_bridge__country_regions --> uss_bridge__state_provinces
    uss_bridge__credit_cards --> uss_bridge
    uss_bridge__credit_cards --> uss_bridge__sales_order_headers
    uss_bridge__cultures --> uss_bridge
    uss_bridge__currencies --> uss_bridge
    uss_bridge__currencies --> uss_bridge__sales_order_headers
    uss_bridge__currency_rates --> uss_bridge
    uss_bridge__customers --> uss_bridge
    uss_bridge__customers --> uss_bridge__sales_order_headers
    uss_bridge__departments --> uss_bridge
    uss_bridge__departments --> uss_bridge__employee_department_histories
    uss_bridge__email_addresses --> uss_bridge
    uss_bridge__employee_department_histories --> uss_bridge
    uss_bridge__employee_department_histories --> uss_bridge__job_candidates
    uss_bridge__employee_department_histories --> uss_bridge__purchase_order_headers
    uss_bridge__employee_pay_histories --> uss_bridge
    uss_bridge__employee_pay_histories --> uss_bridge__job_candidates
    uss_bridge__employee_pay_histories --> uss_bridge__purchase_order_headers
    uss_bridge__employees --> uss_bridge
    uss_bridge__employees --> uss_bridge__job_candidates
    uss_bridge__employees --> uss_bridge__purchase_order_headers
    uss_bridge__illustrations --> uss_bridge
    uss_bridge__job_candidates --> uss_bridge
    uss_bridge__locations --> uss_bridge
    uss_bridge__locations --> uss_bridge__work_order_routings
    uss_bridge__person_phones --> uss_bridge
    uss_bridge__persons --> uss_bridge
    uss_bridge__phone_number_types --> uss_bridge
    uss_bridge__phone_number_types --> uss_bridge__person_phones
    uss_bridge__product_categories --> uss_bridge
    uss_bridge__product_categories --> uss_bridge__product_subcategories
    uss_bridge__product_cost_histories --> uss_bridge
    uss_bridge__product_cost_histories --> uss_bridge__product_inventories
    uss_bridge__product_cost_histories --> uss_bridge__product_reviews
    uss_bridge__product_cost_histories --> uss_bridge__product_vendors
    uss_bridge__product_cost_histories --> uss_bridge__purchase_order_details
    uss_bridge__product_cost_histories --> uss_bridge__sales_order_details
    uss_bridge__product_cost_histories --> uss_bridge__shopping_cart_items
    uss_bridge__product_cost_histories --> uss_bridge__transaction_histories
    uss_bridge__product_cost_histories --> uss_bridge__transaction_history_archives
    uss_bridge__product_cost_histories --> uss_bridge__work_order_routings
    uss_bridge__product_cost_histories --> uss_bridge__work_orders
    uss_bridge__product_descriptions --> uss_bridge
    uss_bridge__product_inventories --> uss_bridge
    uss_bridge__product_inventories --> uss_bridge__work_order_routings
    uss_bridge__product_list_price_histories --> uss_bridge
    uss_bridge__product_list_price_histories --> uss_bridge__product_inventories
    uss_bridge__product_list_price_histories --> uss_bridge__product_reviews
    uss_bridge__product_list_price_histories --> uss_bridge__product_vendors
    uss_bridge__product_list_price_histories --> uss_bridge__purchase_order_details
    uss_bridge__product_list_price_histories --> uss_bridge__sales_order_details
    uss_bridge__product_list_price_histories --> uss_bridge__shopping_cart_items
    uss_bridge__product_list_price_histories --> uss_bridge__transaction_histories
    uss_bridge__product_list_price_histories --> uss_bridge__transaction_history_archives
    uss_bridge__product_list_price_histories --> uss_bridge__work_order_routings
    uss_bridge__product_list_price_histories --> uss_bridge__work_orders
    uss_bridge__product_model_illustrations --> uss_bridge
    uss_bridge__product_models --> uss_bridge
    uss_bridge__product_models --> uss_bridge__product_model_illustrations
    uss_bridge__product_models --> uss_bridge__products
    uss_bridge__product_photos --> uss_bridge
    uss_bridge__product_reviews --> uss_bridge
    uss_bridge__product_subcategories --> uss_bridge
    uss_bridge__product_subcategories --> uss_bridge__products
    uss_bridge__product_vendors --> uss_bridge
    uss_bridge__product_vendors --> uss_bridge__purchase_order_headers
    uss_bridge__products --> uss_bridge
    uss_bridge__products --> uss_bridge__product_inventories
    uss_bridge__products --> uss_bridge__product_reviews
    uss_bridge__products --> uss_bridge__product_vendors
    uss_bridge__products --> uss_bridge__purchase_order_details
    uss_bridge__products --> uss_bridge__sales_order_details
    uss_bridge__products --> uss_bridge__shopping_cart_items
    uss_bridge__products --> uss_bridge__transaction_histories
    uss_bridge__products --> uss_bridge__transaction_history_archives
    uss_bridge__products --> uss_bridge__work_order_routings
    uss_bridge__products --> uss_bridge__work_orders
    uss_bridge__purchase_order_details --> uss_bridge
    uss_bridge__purchase_order_headers --> uss_bridge
    uss_bridge__purchase_order_headers --> uss_bridge__purchase_order_details
    uss_bridge__sales_order_details --> uss_bridge
    uss_bridge__sales_order_headers --> uss_bridge
    uss_bridge__sales_order_headers --> uss_bridge__sales_order_details
    uss_bridge__sales_person_quota_histories --> uss_bridge
    uss_bridge__sales_person_quota_histories --> uss_bridge__sales_order_headers
    uss_bridge__sales_person_quota_histories --> uss_bridge__stores
    uss_bridge__sales_persons --> uss_bridge
    uss_bridge__sales_persons --> uss_bridge__sales_order_headers
    uss_bridge__sales_persons --> uss_bridge__stores
    uss_bridge__sales_reasons --> uss_bridge
    uss_bridge__sales_tax_rates --> uss_bridge
    uss_bridge__sales_territories --> uss_bridge
    uss_bridge__sales_territories --> uss_bridge__customers
    uss_bridge__sales_territories --> uss_bridge__sales_order_headers
    uss_bridge__sales_territories --> uss_bridge__sales_persons
    uss_bridge__sales_territories --> uss_bridge__sales_territory_histories
    uss_bridge__sales_territories --> uss_bridge__state_provinces
    uss_bridge__sales_territory_histories --> uss_bridge
    uss_bridge__sales_territory_histories --> uss_bridge__sales_order_headers
    uss_bridge__sales_territory_histories --> uss_bridge__stores
    uss_bridge__scrap_reasons --> uss_bridge
    uss_bridge__scrap_reasons --> uss_bridge__work_orders
    uss_bridge__shifts --> uss_bridge
    uss_bridge__shifts --> uss_bridge__employee_department_histories
    uss_bridge__ship_methods --> uss_bridge
    uss_bridge__ship_methods --> uss_bridge__purchase_order_headers
    uss_bridge__ship_methods --> uss_bridge__sales_order_headers
    uss_bridge__shopping_cart_items --> uss_bridge
    uss_bridge__special_offers --> uss_bridge
    uss_bridge__special_offers --> uss_bridge__sales_order_details
    uss_bridge__state_provinces --> uss_bridge
    uss_bridge__state_provinces --> uss_bridge__addresses
    uss_bridge__state_provinces --> uss_bridge__sales_tax_rates
    uss_bridge__stores --> uss_bridge
    uss_bridge__stores --> uss_bridge__customers
    uss_bridge__transaction_histories --> uss_bridge
    uss_bridge__transaction_history_archives --> uss_bridge
    uss_bridge__unit_measures --> uss_bridge
    uss_bridge__unit_measures --> uss_bridge__bill_of_materials
    uss_bridge__unit_measures --> uss_bridge__product_vendors
    uss_bridge__vendors --> uss_bridge
    uss_bridge__vendors --> uss_bridge__purchase_order_headers
    uss_bridge__work_order_routings --> uss_bridge
    uss_bridge__work_orders --> uss_bridge
    uss_bridge__work_orders --> uss_bridge__work_order_routings

    %% db.silver -> db.gold
    bag__adventure_works__address_types --> address_types
    bag__adventure_works__addresses --> addresses
    bag__adventure_works__bill_of_materials --> bill_of_materials
    bag__adventure_works__business_entity_addresses --> business_entity_addresses
    bag__adventure_works__business_entity_contacts --> business_entity_contacts
    bag__adventure_works__contact_types --> contact_types
    bag__adventure_works__country_regions --> country_regions
    bag__adventure_works__credit_cards --> credit_cards
    bag__adventure_works__cultures --> cultures
    bag__adventure_works__currencies --> currencies
    bag__adventure_works__currency_rates --> currency_rates
    bag__adventure_works__customers --> customers
    bag__adventure_works__departments --> departments
    bag__adventure_works__email_addresses --> email_addresses
    bag__adventure_works__employee_department_histories --> employee_department_histories
    bag__adventure_works__employee_pay_histories --> employee_pay_histories
    bag__adventure_works__employees --> employees
    bag__adventure_works__illustrations --> illustrations
    bag__adventure_works__job_candidates --> job_candidates
    bag__adventure_works__locations --> locations
    bag__adventure_works__person_phones --> person_phones
    bag__adventure_works__persons --> persons
    bag__adventure_works__phone_number_types --> phone_number_types
    bag__adventure_works__product_categories --> product_categories
    bag__adventure_works__product_cost_histories --> product_cost_histories
    bag__adventure_works__product_descriptions --> product_descriptions
    bag__adventure_works__product_inventories --> product_inventories
    bag__adventure_works__product_list_price_histories --> product_list_price_histories
    bag__adventure_works__product_model_illustrations --> product_model_illustrations
    bag__adventure_works__product_models --> product_models
    bag__adventure_works__product_photos --> product_photos
    bag__adventure_works__product_reviews --> product_reviews
    bag__adventure_works__product_subcategories --> product_subcategories
    bag__adventure_works__product_vendors --> product_vendors
    bag__adventure_works__products --> products
    bag__adventure_works__purchase_order_details --> purchase_order_details
    bag__adventure_works__purchase_order_headers --> purchase_order_headers
    bag__adventure_works__sales_order_details --> sales_order_details
    bag__adventure_works__sales_order_headers --> sales_order_headers
    bag__adventure_works__sales_person_quota_histories --> sales_person_quota_histories
    bag__adventure_works__sales_persons --> sales_persons
    bag__adventure_works__sales_reasons --> sales_reasons
    bag__adventure_works__sales_tax_rates --> sales_tax_rates
    bag__adventure_works__sales_territories --> sales_territories
    bag__adventure_works__sales_territory_histories --> sales_territory_histories
    bag__adventure_works__scrap_reasons --> scrap_reasons
    bag__adventure_works__shifts --> shifts
    bag__adventure_works__ship_methods --> ship_methods
    bag__adventure_works__shopping_cart_items --> shopping_cart_items
    bag__adventure_works__special_offers --> special_offers
    bag__adventure_works__state_provinces --> state_provinces
    bag__adventure_works__stores --> stores
    bag__adventure_works__transaction_histories --> transaction_histories
    bag__adventure_works__transaction_history_archives --> transaction_history_archives
    bag__adventure_works__unit_measures --> unit_measures
    bag__adventure_works__vendors --> vendors
    bag__adventure_works__work_order_routings --> work_order_routings
    bag__adventure_works__work_orders --> work_orders
    uss_bridge --> _bridge__as_of

    %% db.gold -> db.gold
    _bridge__as_of --> _one_big_table__as_of
    address_types --> _one_big_table__as_of
    addresses --> _one_big_table__as_of
    bill_of_materials --> _one_big_table__as_of
    business_entity_addresses --> _one_big_table__as_of
    business_entity_contacts --> _one_big_table__as_of
    contact_types --> _one_big_table__as_of
    country_regions --> _one_big_table__as_of
    credit_cards --> _one_big_table__as_of
    cultures --> _one_big_table__as_of
    currencies --> _one_big_table__as_of
    currency_rates --> _one_big_table__as_of
    customers --> _one_big_table__as_of
    departments --> _one_big_table__as_of
    email_addresses --> _one_big_table__as_of
    employee_department_histories --> _one_big_table__as_of
    employee_pay_histories --> _one_big_table__as_of
    employees --> _one_big_table__as_of
    illustrations --> _one_big_table__as_of
    job_candidates --> _one_big_table__as_of
    locations --> _one_big_table__as_of
    person_phones --> _one_big_table__as_of
    persons --> _one_big_table__as_of
    phone_number_types --> _one_big_table__as_of
    product_categories --> _one_big_table__as_of
    product_cost_histories --> _one_big_table__as_of
    product_descriptions --> _one_big_table__as_of
    product_inventories --> _one_big_table__as_of
    product_list_price_histories --> _one_big_table__as_of
    product_model_illustrations --> _one_big_table__as_of
    product_models --> _one_big_table__as_of
    product_photos --> _one_big_table__as_of
    product_reviews --> _one_big_table__as_of
    product_subcategories --> _one_big_table__as_of
    product_vendors --> _one_big_table__as_of
    products --> _one_big_table__as_of
    purchase_order_details --> _one_big_table__as_of
    purchase_order_headers --> _one_big_table__as_of
    sales_order_details --> _one_big_table__as_of
    sales_order_headers --> _one_big_table__as_of
    sales_person_quota_histories --> _one_big_table__as_of
    sales_persons --> _one_big_table__as_of
    sales_reasons --> _one_big_table__as_of
    sales_tax_rates --> _one_big_table__as_of
    sales_territories --> _one_big_table__as_of
    sales_territory_histories --> _one_big_table__as_of
    scrap_reasons --> _one_big_table__as_of
    shifts --> _one_big_table__as_of
    ship_methods --> _one_big_table__as_of
    shopping_cart_items --> _one_big_table__as_of
    special_offers --> _one_big_table__as_of
    state_provinces --> _one_big_table__as_of
    stores --> _one_big_table__as_of
    transaction_histories --> _one_big_table__as_of
    transaction_history_archives --> _one_big_table__as_of
    unit_measures --> _one_big_table__as_of
    vendors --> _one_big_table__as_of
    work_order_routings --> _one_big_table__as_of
    work_orders --> _one_big_table__as_of
    
    linkStyle default stroke:#666,stroke-width:2px

    %% Bronze shades
    classDef bronze_classic fill:#CD7F32,color:black
    classDef bronze_dark fill:#B87333,color:black
    classDef bronze_light fill:#E09756,color:black
    classDef bronze_antique fill:#966B47,color:black
    
    %% Silver shades
    classDef silver_classic fill:#C0C0C0,color:black
    classDef silver_dark fill:#A8A8A8,color:black
    classDef silver_light fill:#D8D8D8,color:black
    classDef silver_antique fill:#B4B4B4,color:black
    
    %% Gold shades
    classDef gold_classic fill:#FFD700,color:black
    classDef gold_dark fill:#DAA520,color:black
    classDef gold_light fill:#FFE55C,color:black
    classDef gold_antique fill:#CFB53B,color:black

    class db.bronze bronze_classic
    class db.silver silver_classic
    class bags silver_dark
    class db.gold gold_classic
```

## ERDs - Oriented Data Models
### Bronze
```mermaid
flowchart LR
    raw__adventure_works__sales_order_details --> raw__adventure_works__products
    raw__adventure_works__sales_order_details --> raw__adventure_works__sales_order_headers
    raw__adventure_works__sales_order_details --> raw__adventure_works__special_offers
    
    raw__adventure_works__products --> raw__adventure_works__product_subcategories
    raw__adventure_works__product_subcategories --> raw__adventure_works__product_categories

    raw__adventure_works__sales_order_headers --> raw__adventure_works__addresses
    raw__adventure_works__sales_order_headers --> raw__adventure_works__credit_cards
    raw__adventure_works__sales_order_headers --> raw__adventure_works__currency_rates
    raw__adventure_works__sales_order_headers --> raw__adventure_works__customers
    raw__adventure_works__sales_order_headers --> raw__adventure_works__persons
    raw__adventure_works__sales_order_headers --> raw__adventure_works__ship_methods
    raw__adventure_works__customers --> raw__adventure_works__sales_territories
    raw__adventure_works__sales_order_headers --> raw__adventure_works__sales_territories
    
    raw__adventure_works__customers --> raw__adventure_works__persons
    raw__adventure_works__customers --> raw__adventure_works__stores
    
    raw__adventure_works__sales_territories --> raw__adventure_works__state_provinces
    
    raw__adventure_works__stores --> raw__adventure_works__persons
```

### Silver
Under construction

### Gold
```mermaid
flowchart LR
    _bridge --> customers
    _bridge --> products
    _bridge --> sales_order_details
    _bridge --> sales_order_headers
    _bridge --> sales_persons
    _bridge --> sales_territories
    _bridge --> stores
```
