import plotly.graph_objects as go
import numpy as np
import polars as pl
import streamlit as st
from streamlit_extras.grid import grid

def get_latest_metadata(iceberg_path):
    import os
    import glob

    metadata_dir = os.path.join(iceberg_path, "metadata")
    json_files = glob.glob(os.path.join(metadata_dir, "*.json"))
    json_files.sort(reverse=True)

    if json_files:
        return json_files[0]
    else:
        raise FileNotFoundError(f"No JSON metadata files found in {metadata_dir}")

peripherals = [
    #"addresses",
    #"address_types",
    #"bill_of_materials",
    #"business_entity_addresses",
    #"business_entity_contacts",
    "calendar",
    #"contact_types",
    #"country_regions",
    #"credit_cards",
    #"cultures",
    #"currencies",
    #"currency_rates",
    #"customers",
    #"departments",
    #"email_addresses",
    #"employees",
    #"employee_department_histories",
    #"employee_pay_histories",
    #"illustrations",
    #"job_candidates",
    #"locations",
    #"persons",
    #"person_phones",
    #"phone_number_types",
    #"products",
    #"product_categories",
    #"product_cost_histories",
    #"product_descriptions",
    #"product_inventories",
    #"product_list_price_histories",
    #"product_models",
    #"product_model_illustrations",
    #"product_photos",
    #"product_reviews",
    #"product_subcategories",
    #"product_vendors",
    #"purchase_order_details",
    #"purchase_order_headers",
    #"sales_order_details",
    #"sales_order_headers",
    #"sales_persons",
    #"sales_person_quota_histories",
    #"sales_reasons",
    #"sales_tax_rates",
    #"sales_territories",
    #"sales_territory_histories",
    #"scrap_reasons",
    #"shifts",
    #"ship_methods",
    #"shopping_cart_items",
    #"special_offers",
    #"state_provinces",
    #"stores",
    #"transaction_histories",
    #"transaction_history_archives",
    #"unit_measures",
    #"vendors",
    #"work_orders",
    #"work_order_routings",
]
n_weeks = 52

dar_path = "./lakehouse/dar"

# Load the bridge
bridge_df = pl.scan_iceberg(
    get_latest_metadata(f"{dar_path}/_bridge__as_of")
).collect().filter(
    pl.col("bridge__is_current_record") == True,
    #pl.col("peripheral").is_in(peripherals)
)

# Define the OBT
obt_df = bridge_df

# Add the peripheral tables
for peripheral in peripherals:
    print(f"Loading {peripheral}...")
    peripheral_df = pl.scan_iceberg(
        get_latest_metadata(f"{dar_path}/{peripheral}")
    ).collect().select(
        pl.exclude([col for col in obt_df.columns if col.startswith("_dlt_") or col.endswith("__modified_date")])
    )
    
    common_columns = [set(obt_df.columns).intersection(set(peripheral_df.columns))][0]
    
    obt_df = obt_df.join(
        peripheral_df,
        on=list(common_columns),
        how="inner",
        #validate="m:1"
    )

# Filter out date range
obt_df = obt_df.filter(
    pl.col("date").cast(pl.Date) < pl.date(2014, 1, 1)
).filter(
    pl.col("date").cast(pl.Date) >= pl.col("date").cast(pl.Date).max() - pl.duration(days=n_weeks*7)
)

min_date = obt_df.select(pl.col("date").min()).item()
max_date = obt_df.select(pl.col("date").max()).item()

# Create a metric cube
def create_metric_summary(df, group_by_col=None, sort_by=None): 
    sort_by = sort_by or group_by_col
    
    event_cols = [
        col for col in df.columns
       if col.startswith(("event__", "measure__"))
        and not col.endswith("_modified")
    ]
    aggregations = []
    
    for col in event_cols:
        prefixes = {
            "event__": "metric__",
            "measure__": "metric__"
        }

        metric_name = col
        for old, new in prefixes.items():
            metric_name = metric_name.replace(old, new)
        
        aggregation = pl.col(col).sum().alias(metric_name)
        aggregations.append(aggregation)
    
    metric_df = df.select(aggregations)
    
    if group_by_col:
        metric_df = df.group_by(group_by_col).agg(aggregations)

    metric_df = metric_df.with_columns(
        (
            pl.col("metric__sales_order_headers_from_new_customers")
            / pl.col("metric__sales_order_headers_placed")
            * 100
        ).alias("metric__sales_order_headers_from_new_customers__percentage"),
        (
            pl.col("metric__sales_order_headers_from_returning_customers")
            / pl.col("metric__sales_order_headers_placed")
            * 100
        ).alias("metric__sales_order_headers_from_returning_customers__percentage")
    )
        
    metric_df = metric_df.sort(
        sort_by,
        descending=True
    )
    
    return metric_df

metric__date_df = create_metric_summary(
    df=obt_df,
    group_by_col="date",
    sort_by="date"
)

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

# Draw the dashboard
st.set_page_config(
    page_title="Adventure Works",
    initial_sidebar_state="expanded",
    layout="wide",
)

st.title("Adventure Works")
st.text(f"{min_date} to {max_date}")

neutral_color = lambda x: f"rgba(90, 90, 160, {x})"
good_color = lambda x: f"rgba(100, 170, 90, {x})"
bad_color = lambda x: f"rgba(190, 120, 80, {x})"

with st.container():
    kpi_grid = grid(
        4,
        vertical_align="center",
        gap="small"
    )
    
    # Sort metric columns by their most recent values (descending)
    metric_columns = sorted(
        [col for col in metric__date_df.columns if col != "date"],
        key=lambda col: metric__date_df[col][0],
        reverse=True
    )
    
    for col in metric_columns:
        
        metric = col
        metric_title = metric.replace("metric__", "").replace("_", " ").title()
        
        control_data_df = calculate_control_limits(metric__date_df, metric)
        
        current_value = control_data_df[metric][0]
        
        current_central_line = control_data_df["central_line"][0]
        current_lower_control_limit = control_data_df["lower_control_limit"][0]
        current_upper_control_limit = control_data_df["upper_control_limit"][0]
        
        with kpi_grid.container(border=True):
            st.markdown(f"**{metric_title}**")
            
            suffix = ""
            if current_value > current_upper_control_limit:
                suffix = "🔥"
                
            elif current_value < current_lower_control_limit:
                suffix = "❄️"
                
            current_value_str = f"## {current_value:,.2f}{suffix}".replace(",", " ")
            st.markdown(f"{current_value_str}")
            
            current_central_line_str = f"{current_central_line:,.2f}".replace(",", " ")
            current_lower_control_limit_str = f"{current_lower_control_limit:,.2f}".replace(",", " ")
            current_upper_control_limit_str = f"{current_upper_control_limit:,.2f}".replace(",", " ")
            st.markdown(f"CL: {current_central_line_str} | LCL: {current_lower_control_limit_str} | UCL: {current_upper_control_limit_str}")
            
            with st.expander(f"Process Control Chart", expanded=False):
                process_control_df = control_data_df.select(
                    pl.col("date"),
                    pl.col(metric),
                    pl.col("central_line"),
                    pl.col("upper_control_limit"),
                    pl.col("lower_control_limit"),
                    pl.col("short_run"),
                    pl.col("long_run"),
                    pl.col("upper_outlier"),
                    pl.col("lower_outlier")
                )
        
                fig = go.Figure()
        
                # Plot metric_name as tiny points without lines
                fig.add_trace(go.Scatter(
                    x=process_control_df["date"],
                    y=process_control_df[metric],
                    mode="markers",
                    marker=dict(size=5, color=neutral_color(0.3)),
                    name="In control"
                ))
        
                # Plot central line, UCL, and LCL as thick whole lines
                for col, color, name in [
                    ("central_line", neutral_color(1.0), "Central Line"),
                    ("upper_control_limit", bad_color(1.0), "Upper Control Limit"),
                    ("lower_control_limit", good_color(1.0), "Lower Control Limit")
                ]:
                    fig.add_trace(go.Scatter(
                        x=process_control_df["date"],
                        y=process_control_df[col],
                        mode="lines",
                        line=dict(color=color),
                        name=name
                    ))
        
                # Plot upper and lower outliers as bold points
                for col, color, name in [
                    ("upper_outlier", bad_color(1.0), "Upper Outlier"),
                    ("lower_outlier", good_color(1.0), "Lower Outlier")
                ]:
                    fig.add_trace(go.Scatter(
                        x=process_control_df["date"],
                        y=process_control_df[col],
                        mode="markers",
                        marker=dict(size=6, color=color, symbol="circle"),
                        name=name
                    ))
        
                # Layout settings
                fig.update_layout(
                    yaxis=dict(
                        #title=metric_title,
                        rangemode="tozero"
                    ),
                    margin=dict(l=50, r=50, t=50, b=50),
                    showlegend=False,
                    paper_bgcolor="rgba(0, 0, 0, 0)",
                    plot_bgcolor="rgba(0, 0, 0, 0)",
                    height=300
                )
        
                # Display in Streamlit
                st.plotly_chart(fig, key=f"process_control_chart__{metric}")