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
*Simplified since there are 200+ models in total.*
```mermaid
flowchart LR

    subgraph bronze["Bronze"]
        raw(["Raw Views"])
    end

    subgraph silver["Silver"]
        hook(["HOOK Bags"])
        measures(["Event Measures"])
        bridge_staging(["Bridge Staging"])
    end

    subgraph gold["Gold"]
        bridge(["Puppini Bridges"])
        peripheral(["Peripheral Tables"])
    end

    raw --> hook --> measures --> bridge_staging --> bridge
    hook --> bridge_staging
    hook --> peripheral

    style raw fill:#CD7F32,color:black

    style hook fill:#C0C0C0,color:black
    style measures fill:#C0C0C0,color:black
    style bridge_staging fill:#C0C0C0,color:black

    style bridge fill:#FFD700,color:black
    style peripheral fill:#FFD700,color:black
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
