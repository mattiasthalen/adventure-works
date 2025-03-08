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
    classDef bronze fill:#CD7F32,color:black
    classDef silver fill:#C0C0C0,color:black
    classDef gold fill:#FFD700,color:black

    subgraph das["db.das"]
        raw(["Raw Tables [58]"]):::bronze
    end

    subgraph dab["db.dab"]
        hook(["HOOK Bags [58]"]):::silver
    end

    subgraph dar_stg["db.dar__staging"]
        bridges(["Puppini Bridges [58]"]):::silver
        event_bridges(["Event Bridges [58]"]):::silver
    end

    subgraph dar["db.dar"]
        bridge_union(["Puppini Bridge Union [1]"]):::gold
        peripheral(["Peripheral Tables [58]"]):::gold
    end

    raw --> hook --> bridges --> event_bridges --> bridge_union
    hook --> event_bridges
    hook --> peripheral

    legend_das["DAS = Data According To System"] -->
    legend_dab["DAB = Data According To Business"] ---->
    legend_dar["DAR = Data According To Requirements"]
```

## ERDs - Oriented Data Models
Under construction