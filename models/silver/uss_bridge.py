import pandas as pd
import polars as pl
import typing as t

from datetime import datetime
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName

@model(
    name='silver.uss_bridge',
    enabled=False,
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,
        time_column='bridge__record_updated_at'
    ),
    columns={
        "stage": "string",
        "_pit_hook__address": "binary",
        "_pit_hook__credit_card": "binary",
        "_pit_hook__currency_rate": "binary",
        "_pit_hook__customer": "binary",
        "_pit_hook__product": "binary",
        "_pit_hook__product_category": "binary",
        "_pit_hook__product_subcategory": "binary",
        "_pit_hook__sales_order": "binary",
        "_pit_hook__sales_order_detail": "binary",
        "_pit_hook__sales_person": "binary",
        "_pit_hook__ship_method": "binary",
        "_pit_hook__special_offer": "binary",
        "_pit_hook__state_province": "binary",
        "_pit_hook__territory": "binary",
        "_pit_hook__inventory": "binary",
        "_hook__calendar__date": "binary",
        "measure__inventory__quantity_purchased": "int",
        "measure__inventory__quantity_made": "int",
        "measure__inventory__quantity_sold": "int",
        "measure__inventory__net_transacted_quantity": "int",
        "measure__inventory__gross_on_hand_quantity": "int",
        "measure__inventory__net_on_hand_quantity": "int",
        "measure__is_returning_customer": "int",
        "measure__sales_order_detail__placed": "int",
        "measure__sales_order_detail__has_special_offer": "int",
        "measure__sales_order_detail__discount_price": "float",
        "measure__sales_order_detail__price": "float",
        "measure__sales_order_detail__discount": "float",
        "measure__sales_order_placed": "int",
        "measure__sales_order_due_lead_time": "float",
        "measure__sales_order_shipping_lead_time": "float",
        "measure__sales_order_due": "int",
        "measure__sales_order_shipped_on_time": "int",
        "measure__sales_order_shipped": "int",
        "bridge__record_loaded_at": "timestamp",
        "bridge__record_updated_at": "timestamp",
        "bridge__record_valid_from": "timestamp",
        "bridge__record_valid_to": "timestamp",
        "bridge__is_current_record": "boolean",
    }
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> t.Generator[pd.DataFrame, None, None]:
    
    # Load data
    bridges = [
        context.resolve_table("silver.uss_bridge__currency_rates"),
        context.resolve_table("silver.uss_bridge__sales_order_headers"),
        context.resolve_table("silver.uss_bridge__product_subcategories"),
        context.resolve_table("silver.uss_bridge__sales_persons"),
        context.resolve_table("silver.uss_bridge__products"),
        context.resolve_table("silver.uss_bridge__state_provinces"),
        context.resolve_table("silver.uss_bridge__sales_territories"),
        context.resolve_table("silver.uss_bridge__sales_order_details"),
        context.resolve_table("silver.uss_bridge__ship_methods"),
        context.resolve_table("silver.uss_bridge__special_offers"),
        context.resolve_table("silver.uss_bridge__product_categories"),
        context.resolve_table("silver.uss_bridge__customers"),
        context.resolve_table("silver.uss_bridge__addresses"),
        context.resolve_table("silver.uss_bridge__credit_cards"),
        context.resolve_table("silver.uss_bridge__inventories"),
    ]
    
    df = pl.DataFrame()
    
    for bridge in bridges:
        bridge_df = pl.from_pandas(context.fetchdf(f"SELECT * FROM {bridge} WHERE bridge__record_updated_at BETWEEN '{start}' AND '{end}'"))
        df = pl.vstack(df, bridge_df)
    
    yield df.to_pandas()