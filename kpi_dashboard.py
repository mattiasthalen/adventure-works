import polars as pl
import streamlit as st

gold_path = "./lakehouse/gold"

bridge_df = pl.read_delta(f"{gold_path}/_bridge__as_is")
customers_df = pl.read_delta(f"{gold_path}/customers")
sales_order_headers_df = pl.read_delta(f"{gold_path}/sales_order_headers")

joined_df = bridge_df.join(
    customers_df,
    on="_pit_hook__customer",
    how="left"
).join(
    sales_order_headers_df,
    on="_pit_hook__sales_order",
    how="left"
)

orders_placed = (
    joined_df.select(
        pl.col("sales_order__order_date").alias("date"),
        "sales_order__sales_order_number"
    )
    .filter(pl.col("date").is_not_null())
    .group_by("date")
    .agg(pl.col("sales_order__sales_order_number").n_unique().alias("orders_placed"))
)

orders_due = (
    joined_df.select(
        pl.col("sales_order__due_date").alias("date"),
        "sales_order__sales_order_number"
    )
    .filter(pl.col("date").is_not_null())
    .group_by("date")
    .agg(pl.col("sales_order__sales_order_number").n_unique().alias("orders_due"))
)

orders_shipped = (
    joined_df.select(
        pl.col("sales_order__ship_date").alias("date"),
        "sales_order__sales_order_number"
    )
    .filter(pl.col("date").is_not_null())
    .group_by("date")
    .agg(pl.col("sales_order__sales_order_number").n_unique().alias("orders_shipped"))
)

orders_shipped_on_time = (
    joined_df.filter(
        pl.col("sales_order__ship_date") <= pl.col("sales_order__due_date")
    )
    .select(
        pl.col("sales_order__due_date").alias("date"),
        "sales_order__sales_order_number"
    )
    .filter(
        pl.col("date").is_not_null()
    )
    .group_by("date")
    .agg(
        pl.col("sales_order__sales_order_number").n_unique().alias("orders_shipped_on_time")
    )
)

orders_due_lead_time = (
    joined_df.select(
        pl.col("sales_order__order_date").alias("date"),
        (pl.col("sales_order__due_date").cast(pl.Date) - pl.col("sales_order__order_date").cast(pl.Date)).alias("sales_order_due_lead_time")
    )
    .filter(pl.col("date").is_not_null())
    .group_by("date")
    .agg(pl.col("sales_order_due_lead_time").mean().alias("mean_sales_order_due_lead_time"))
)

# Combine all measures into one table using the complete date range
measures_df = (
    orders_placed
    .join(orders_due, on="date", how="left")
    .join(orders_shipped, on="date", how="left")
    .join(orders_shipped_on_time, on="date", how="left")
    .join(orders_due_lead_time, on="date", how="left")
    .fill_null(0)
    .sort("date")
)

shipping_capacity = measures_df.select(
    pl.col("orders_shipped").quantile(0.9)
).item()

shipping_lead_time_capacity = joined_df.select(
    (pl.col("sales_order__ship_date").cast(pl.Date) - pl.col("sales_order__order_date").cast(pl.Date)).quantile(0.9)
).item()

measures_df = measures_df.with_columns(
    pl.lit(shipping_capacity).alias("shipping_capacity"),
    pl.lit(shipping_lead_time_capacity).alias("shipping_lead_time_capacity"),
)

metrics_df = measures_df.with_columns(
    (pl.col("mean_sales_order_due_lead_time") / pl.col("shipping_lead_time_capacity")).alias("shipping_lead_time_load"),
    (pl.col("orders_due") / pl.col("shipping_capacity")).alias("shipping_load"),
    (pl.col("orders_shipped_on_time") / pl.col("orders_due")).alias("on_time_shipping")
)

import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime, timedelta
import polars as pl
import streamlit as st

# Dark theme color palette
COLORS = {
    'good': '#90EE90',      # Light green
    'bad': '#FF7F7F',       # Light red
    'neutral': '#89CFF0',   # Light blue
    'gray': '#808080',      # Medium gray
    'background': '#0E1117', # Streamlit's dark background
    'grid': '#333333',      # Dark gray for grid
    'text': '#FFFFFF'       # White text
}

# Filter for last 12 months
last_date = metrics_df["date"].max()
first_date = last_date - timedelta(days=90)
filtered_df = metrics_df.filter(pl.col("date") >= first_date)

def calculate_control_limits(data):
    """Calculate X chart limits using moving range method"""
    values = data.drop_nulls().to_numpy()
    moving_ranges = np.abs(np.diff(values))
    
    d2 = 1.128  # for n=2 (moving range)
    E2 = 2.66   # for n=1 (individual values)
    
    mean = np.mean(values)
    mR_bar = np.mean(moving_ranges)
    
    ucl = mean + (E2 * mR_bar)
    lcl = mean - (E2 * mR_bar)
    
    return mean, ucl, lcl

def create_xmr_chart(df, col_name, title, is_percentage=False, lower_is_bad=True):
    """Create an X chart with styled points and context-aware control limits"""
    mean, ucl, lcl = calculate_control_limits(df.select(col_name).to_series())
    
    df_pandas = df.fill_null(strategy="zero").to_pandas()
    
    values = df_pandas[col_name].values
    above_ucl = values > ucl
    below_lcl = values < lcl
    in_control = ~(above_ucl | below_lcl)
    
    fig = go.Figure()
    
    # In-control points (small and faint)
    fig.add_trace(go.Scatter(
        x=df_pandas[in_control]["date"],
        y=df_pandas[in_control][col_name],
        mode='markers',
        marker=dict(
            size=5,
            color=COLORS['gray'],
            opacity=0.3
        ),
        name='In Control'
    ))
    
    # Points above UCL
    if len(df_pandas[above_ucl]) > 0:
        fig.add_trace(go.Scatter(
            x=df_pandas[above_ucl]["date"],
            y=df_pandas[above_ucl][col_name],
            mode='markers',
            marker=dict(
                size=8,
                color=COLORS['good'] if lower_is_bad else COLORS['bad'],
                symbol='circle-open',
                line=dict(width=1.5)
            ),
            name='Above UCL' + (' (Good)' if lower_is_bad else ' (Bad)')
        ))
    
    # Points below LCL
    if len(df_pandas[below_lcl]) > 0:
        fig.add_trace(go.Scatter(
            x=df_pandas[below_lcl]["date"],
            y=df_pandas[below_lcl][col_name],
            mode='markers',
            marker=dict(
                size=8,
                color=COLORS['bad'] if lower_is_bad else COLORS['good'],
                symbol='circle-open',
                line=dict(width=1.5)
            ),
            name='Below LCL' + (' (Bad)' if lower_is_bad else ' (Good)')
        ))
    
    # Format values
    if is_percentage:
        mean_text = f"Mean: {mean:.4%}"
        ucl_text = f"UCL: {ucl:.4%}"
        lcl_text = f"LCL: {lcl:.4%}"
    else:
        mean_text = f"Mean: {mean:.4f}"
        ucl_text = f"UCL: {ucl:.4f}"
        lcl_text = f"LCL: {lcl:.4f}"
    
    # Add mean line
    fig.add_hline(
        y=mean,
        line_dash="solid",
        line_color=COLORS['neutral'],
        line_width=1,
        annotation_text=mean_text,
        annotation_font_color=COLORS['text']
    )
    
    # Add control limits
    if lower_is_bad:
        lcl_color = COLORS['bad']
        ucl_color = COLORS['good']
    else:
        lcl_color = COLORS['good']
        ucl_color = COLORS['bad']
    
    fig.add_hline(
        y=ucl,
        line_dash="dash",
        line_color=ucl_color,
        line_width=1,
        annotation_text=ucl_text,
        annotation_font_color=COLORS['text']
    )
    
    fig.add_hline(
        y=lcl,
        line_dash="dash",
        line_color=lcl_color,
        line_width=1,
        annotation_text=lcl_text,
        annotation_font_color=COLORS['text']
    )
    
    # Update layout for dark theme
    fig.update_layout(
        title={
            'text': title,
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 16, 'color': COLORS['text']}
        },
        # xaxis_title="Date",
        # yaxis_title=col_name,
        yaxis_tickformat='.1%' if is_percentage else None,
        showlegend=True,
        legend={
            'orientation': 'h',
            'yanchor': 'bottom',
            'y': 1.02,
            'xanchor': 'center',
            'x': 0.5,
            'font': {'color': COLORS['text']}
        },
        plot_bgcolor=COLORS['background'],
        paper_bgcolor=COLORS['background'],
        font={'color': COLORS['text']},
        xaxis=dict(
            showgrid=True,
            gridcolor=COLORS['grid'],
            gridwidth=1,
            color=COLORS['text']
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor=COLORS['grid'],
            gridwidth=1,
            color=COLORS['text']
        ),
        margin=dict(t=80)
    )
    
    return fig

# Create the dashboard
with st.container():
    fig1 = create_xmr_chart(
        filtered_df,
        "shipping_lead_time_load",
        "Shipping Lead Time Load - X Chart",
        lower_is_bad=True  # Low lead time load is bad
    )
    st.plotly_chart(fig1, use_container_width=True)

    fig2 = create_xmr_chart(
        filtered_df,
        "shipping_load",
        "Shipping Load - X Chart",
        lower_is_bad=False  # High shipping load is bad
    )
    st.plotly_chart(fig2, use_container_width=True)