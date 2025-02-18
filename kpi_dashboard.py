import plotly.graph_objects as go
import numpy as np
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

# Combine all measures into one table using the complete date range
measures_df = (
    orders_placed
    .join(orders_due, on="date", how="left")
    .join(orders_shipped, on="date", how="left")
    #.fill_null(0)
    .sort("date")
    .with_columns(
        pl.col("date")
    )
)

metrics_df = measures_df

def calculate_control_limits(df: pl.DataFrame, measure_col: str, calc_window: int = 20, long_run: int = 8, short_run: int = 4):
    total_rows = df.height
    
    df = df.with_columns(
        pl.col(measure_col).cast(pl.Float64).alias(measure_col)
    ).drop_nulls()
    
    # Calculate initial limits
    initial_values = df[measure_col][:calc_window]    
    central_line = initial_values.mean()
    
    moving_ranges = np.abs(np.diff(initial_values))
    moving_range_avg = np.nanmean(moving_ranges) if len(moving_ranges) > 0 else 0
    moving_range_scaled = moving_range_avg * 2.66
    upper_control_limit = central_line + moving_range_scaled
    lower_control_limit = max(0, central_line - moving_range_scaled)  # Ensure lower limit is non-negative
    
    # Initialize new columns
    df = df.with_columns([
        pl.lit(None).alias("central_line"),
        pl.lit(None).alias("upper_control_limit"),
        pl.lit(None).alias("lower_control_limit"),
        pl.lit(None).alias("short_run"),
        pl.lit(None).alias("long_run"),
        pl.lit(None).alias("upper_outlier"),
        pl.lit(None).alias("lower_outlier"),
    ])
    
    freeze_until = calc_window
    
    for row_num in range(total_rows):
        value = df[measure_col][row_num]
        
        # Set control limits for first row
        if row_num == 0:
            df = df.with_columns([
                pl.when(pl.arange(0, total_rows) == row_num).then(central_line).otherwise(df["central_line"]).alias("central_line"),
                pl.when(pl.arange(0, total_rows) == row_num).then(upper_control_limit).otherwise(df["upper_control_limit"]).alias("upper_control_limit"),
                pl.when(pl.arange(0, total_rows) == row_num).then(lower_control_limit).otherwise(df["lower_control_limit"]).alias("lower_control_limit"),
            ])
        
        # Detect long runs
        if row_num > freeze_until and row_num < total_rows - long_run:
            subset = df[measure_col][row_num:row_num+long_run]
            all_above = all(subset > central_line)
            all_below = all(subset < central_line)
            
            if all_above or all_below:
                #  df = df.with_columns([
                    # pl.when(pl.arange(0, total_rows) == row_num - 1).then(central_line).otherwise(df["central_line"]).alias("central_line"),
                    # pl.when(pl.arange(0, total_rows) == row_num - 1).then(upper_control_limit).otherwise(df["upper_control_limit"]).alias("upper_control_limit"),
                    # pl.when(pl.arange(0, total_rows) == row_num - 1).then(lower_control_limit).otherwise(df["lower_control_limit"]).alias("lower_control_limit"),
                #  ])
                
                df = df.with_columns([
                    pl.when((pl.arange(0, total_rows) >= row_num) & (pl.arange(0, total_rows) < row_num + long_run)).then(df[measure_col]).otherwise(df["long_run"]).alias("long_run"),
                ])
                
                new_window = min(calc_window, total_rows - row_num)
                new_values = df[measure_col][row_num:row_num + new_window]
                central_line = new_values.mean()
                moving_ranges = np.abs(np.diff(new_values))
                moving_range_avg = np.nanmean(moving_ranges) if len(moving_ranges) > 0 else 0
                moving_range_scaled = moving_range_avg * 2.66
                upper_control_limit = central_line + moving_range_scaled
                lower_control_limit = max(0, central_line - moving_range_scaled)
                freeze_until = row_num + calc_window
                
                df = df.with_columns([
                    pl.when(pl.arange(0, total_rows) == row_num).then(central_line).otherwise(df["central_line"]).alias("central_line"),
                    pl.when(pl.arange(0, total_rows) == row_num).then(upper_control_limit).otherwise(df["upper_control_limit"]).alias("upper_control_limit"),
                    pl.when(pl.arange(0, total_rows) == row_num).then(lower_control_limit).otherwise(df["lower_control_limit"]).alias("lower_control_limit"),
                ])
    
    # Final limits
    df = df.with_columns([
        pl.when(pl.arange(0, total_rows) == total_rows - 1).then(central_line).otherwise(df["central_line"]).alias("central_line"),
        pl.when(pl.arange(0, total_rows) == total_rows - 1).then(upper_control_limit).otherwise(df["upper_control_limit"]).alias("upper_control_limit"),
        pl.when(pl.arange(0, total_rows) == total_rows - 1).then(lower_control_limit).otherwise(df["lower_control_limit"]).alias("lower_control_limit"),
    ])
    
    # Fill lines
    df = df.with_columns(
        pl.col("central_line").forward_fill().alias("central_line"),
        pl.col("upper_control_limit").forward_fill().alias("upper_control_limit"),
        pl.col("lower_control_limit").forward_fill().alias("lower_control_limit")
    )
    
    # Detect outliers
    df = df.with_columns(
        pl.when(pl.col(measure_col) > pl.col("upper_control_limit")).then(pl.col(measure_col)).otherwise(None).alias("upper_outlier"),
        pl.when(pl.col(measure_col) < pl.col("lower_control_limit")).then(pl.col(measure_col)).otherwise(None).alias("lower_outlier")
    )

    return df

def plot_control_chart(metrics_df: pl.DataFrame, metric: str):
    xmr_df = metrics_df.select("date", metric)
    xmr_df = calculate_control_limits(xmr_df, metric)
    
    metric_name = metric.replace("_", " ").title()
    
    fig = go.Figure()
        
    # Add metric scatter plot
    fig.add_trace(go.Scatter(
        x=xmr_df["date"].to_list(), y=xmr_df[metric].to_list(),
        mode='markers', name=metric_name, marker=dict(color='lightgray', size=3)
    ))
    
    # Add control lines
    fig.add_trace(go.Scatter(
        x=xmr_df["date"].to_list(), y=xmr_df["central_line"].to_list(),
        mode='lines', name='Central Line', line=dict(color='white', width=2)
    ))
    fig.add_trace(go.Scatter(
        x=xmr_df["date"].to_list(), y=xmr_df["upper_control_limit"].to_list(),
        mode='lines', name='Upper Control Limit', line=dict(color='salmon', width=2)
    ))
    fig.add_trace(go.Scatter(
        x=xmr_df["date"].to_list(), y=xmr_df["lower_control_limit"].to_list(),
        mode='lines', name='Lower Control Limit', line=dict(color='mediumseagreen', width=2)
    ))
    
    # Add outliers
    fig.add_trace(go.Scatter(
        x=xmr_df["date"].to_list(), y=xmr_df["upper_outlier"].to_list(),
        mode='markers', name='Upper Outlier', marker=dict(color='salmon', size=8)
    ))
    fig.add_trace(go.Scatter(
        x=xmr_df["date"].to_list(), y=xmr_df["lower_outlier"].to_list(),
        mode='markers', name='Lower Outlier', marker=dict(color='mediumseagreen', size=8)
    ))
    
    # Customize layout
    fig.update_layout(
        title=dict(text=metric_name, x=0.05, font=dict(size=18)),
        plot_bgcolor='rgb(40,40,40)',
        paper_bgcolor='rgb(40,40,40)',
        font=dict(color='white'),
        xaxis=dict(gridcolor='gray'),
        yaxis=dict(gridcolor='gray'),
        margin=dict(l=40, r=40, t=40, b=40),
        hovermode="x unified",
        template="plotly_dark",
        showlegend=False,
        xaxis_zeroline=False, yaxis_zeroline=False,
        shapes=[
            dict(
                type="rect",
                xref="paper", yref="paper",
                x0=0, y0=0, x1=1, y1=1,
                fillcolor="rgba(50,50,50,1)",
                layer="below", line=dict(width=0),
                opacity=1,
            )
        ]
    )
    
    st.plotly_chart(fig, use_container_width=True)


n_periods = 90

with st.container():
    col1, col2 = st.columns(2)
    with col1:
        plot_control_chart(metrics_df.tail(n_periods), "orders_placed")
    
    with col2:
        plot_control_chart(metrics_df.tail(n_periods), "orders_due")
    
    plot_control_chart(metrics_df.tail(n_periods), "orders_shipped")
