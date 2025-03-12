from pandas.core.ops import common
import plotly.graph_objects as go
import numpy as np
import polars as pl
import streamlit as st
from turtledemo.forest import start

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

peripherals = ['calendar', 'sales_order_headers', 'sales_territories']
n_weeks = 52

dar_path = "./lakehouse/dar"

# Load the bridge
bridge_df = pl.scan_iceberg(
    get_latest_metadata(f"{dar_path}/_bridge__as_of")
).collect().filter(
    pl.col("bridge__is_current_record") == True,
    pl.col("peripheral").is_in(peripherals)
)

# Define the OBT
obt_df = bridge_df.select(
    [
        col for col in bridge_df.columns 
        if not bridge_df.get_column(col).is_null().all()
    ]
)

# Add the peripheral tables
for peripheral in peripherals:
    peripheral_df = pl.scan_iceberg(
        get_latest_metadata(f"{dar_path}/{peripheral}")
    ).collect().select(
        pl.exclude([col for col in obt_df.columns if col.startswith("_dlt_")])
    )
    
    common_columns = [set(obt_df.columns).intersection(set(peripheral_df.columns))][0]
    
    obt_df = obt_df.join(
        peripheral_df,
        on=list(common_columns),
        how="left",
        validate="m:1"
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
    
    aggregation = [
        pl.col("event__sales_order_headers_placed").sum().alias("metric__sales_order_headers_placed"),
        pl.col("event__sales_order_headers_due").sum().alias("metric__sales_order_headers_due"),
        pl.col("event__sales_order_headers_shipped").sum().alias("metric__sales_order_headers_shipped"),
        pl.col("event__sales_order_headers_modified").sum().alias("metric__sales_order_headers_modified"),
    ]
    
    metric_df = df.select(aggregation)
    
    if group_by_col:
        metric_df = df.group_by(group_by_col).agg(aggregation)
        
    metric_df = metric_df.sort(
        sort_by,
        descending=True
    )
    
    return metric_df

metric__global_df = create_metric_summary(obt_df)
metric__date_df = create_metric_summary(obt_df, "date", "date")
metric__year_week_day_df = create_metric_summary(obt_df, ["year_week", "weekday__name", "date"], "date")
metric__territory_df = create_metric_summary(obt_df, "sales_territory__name")

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

# Set page config to use wide layout
st.set_page_config(layout="wide")

# Render visualizations
metrics = [
    "metric__sales_order_headers_placed",
    "metric__sales_order_headers_due",
    "metric__sales_order_headers_shipped",
    "metric__sales_order_headers_modified",
]
dashboard_columns = len(metrics)

st.title(f"Adventure Works")
st.subheader(f"{min_date} - {max_date}")

neutral_color = lambda x: f"rgba(90, 90, 160, {x})"
good_color = lambda x: f"rgba(100, 170, 90, {x})"
bad_color = lambda x: f"rgba(190, 120, 80, {x})"

def colored_text(text, color):
    return f'<div style="background-color: {color}; padding: 10px; border-radius: 5px; display: inline-block;">{text}</div>'

st.markdown(
    """
    <style>
        :hover {
            rgba(90, 90, 160, 0.2);
        }

        [data-testid="stHorizontalBlock"] > div {
            background-color: rgba(90, 90, 160, 0.2);
            border-radius: 15px;
            padding: 10px;
            margin: 2px;
            border-style: none;
            box-shadow: 2px 2px 10px rgba(0, 0, 0, 0.2);
            color: inherit;
            display: flex;
            flex-direction: column;
            text-align: center;
        }
                
        [data-testid="stExpander"] details {
            background-color: rgba(90, 90, 160, 0.2);
            border-radius: 10px;
            margin: 2px 0;
            padding: 0;
            border-style: none;
            color: inherit;
            text-align: left;
        }

        [data-testid="stExpander"] details > div {
            background-color: rgba(90, 90, 160, 0.2);
            padding: 10px;
            border-radius: 0 0 10px 10px;
        }

        [data-testid="stExpander"] div {
            box-shadow: none;
            border-radius: 5px;
        }

        [data-testid="stTable"] {
            background-color: rgba(90, 90, 160, 0.2);
            text-align: center;
        }

        [data-testid="stTable"] div {
            color: inherit;
            text-align: center;
        }

        [data-testid="stPlotlyChart"] {
            background-color: rgba(90, 90, 160, 0.2);
            text-align: center;
        }

    </style>
    """,
    unsafe_allow_html=True
)

columns = st.columns(dashboard_columns, border=True)

for idx, col in enumerate(columns):
    metric_name = metrics[idx]
    metric_title = metric_name.replace("_", " ").title()
    
    measures_df = metric__year_week_day_df.select(
        "year_week", "weekday__name", "date", metric_name
    ).sort(
        "date"
    )
    
    control_data_df = calculate_control_limits(measures_df, metric_name)
    
    current_value = control_data_df[metric_name][0]
    current_central_line = control_data_df["central_line"][0]
    current_lower_control_limit = control_data_df["lower_control_limit"][0]
    current_upper_control_limit = control_data_df["upper_control_limit"][0]

    with col:
        st.markdown(f"**{metric_title}**")

        # Card 1 - KPI
        with st.expander("Current Process", expanded=True):
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

        # Card 2 - Calendar
        with st.expander("Weekly Outlier Matrix", expanded=True):
            calendar_df = (
                control_data_df.with_columns([
                    pl.col("year_week").alias("Year-Week"),
                    pl.col("weekday__name").alias("Weekday"),
                    pl.when(pl.col(metric_name) > pl.col("upper_control_limit"))
                        .then(pl.lit("🔥"))
                        .when(pl.col(metric_name) < pl.col("lower_control_limit"))
                        .then(pl.lit("❄️"))
                        .otherwise(pl.lit(""))
                        .alias(metric_name)
                ])
                .sort("Weekday")
                .pivot(
                    on="Weekday",
                    values=metric_name,
                    index="Year-Week",
                    aggregate_function="first"
                )
                .sort("Year-Week", descending=True)
                .select("Year-Week", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
                .to_pandas()
                .set_index("Year-Week")
            )
    
            st.table(calendar_df.head(6))
    
        # Card 3 - Control Chart
        with st.expander("Process Control Chart", expanded=True):
            process_control_df = control_data_df.select(
                pl.col("date"),
                pl.col(metric_name),
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
                y=process_control_df[metric_name],
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
            st.plotly_chart(fig, key=f"process_control_chart__{metric_name}")

        # Card 4 - Pareto
        with st.expander("Pareto", expanded=True):
            pareto_dimension = "sales_territory__name"
            pareto_dimension_title = pareto_dimension.replace("__", " - ").replace("_", " ").title()
            full_pareto_df = metric__territory_df.select(
                pl.col(pareto_dimension),
                pl.col(metric_name)
            ).sort(
                metric_name,
                descending=True
            )

            n_dimensions = len(full_pareto_df)
            top_n = 10

            pareto_df = full_pareto_df.head(top_n)

            if n_dimensions - top_n > 0:
                    pareto_tail_df = full_pareto_df.tail(
                        n_dimensions - top_n
                    ).select(
                        pl.lit("Others").alias(pareto_dimension),
                        pl.col(metric_name).sum()
                    )
    
                    pareto_df = pareto_df.vstack(
                        pareto_tail_df
                    )
    
            pareto_df = pareto_df.with_columns(
                (pl.col(metric_name).cum_sum()/pl.col(metric_name).sum()).alias("cumulative_percentage")
            ).to_pandas()

            fig = go.Figure()

            fig.add_trace(go.Bar(
                x=pareto_df[pareto_dimension],
                y=pareto_df[metric_name],
                name=metric_title,
                marker_color=neutral_color(1.0),
                yaxis="y1"
            ))
    
            # Add line chart (cumulative percentage)
            fig.add_trace(go.Scatter(
                x=pareto_df[pareto_dimension],
                y=pareto_df["cumulative_percentage"],
                name="Cumulative %",
                mode="lines+markers",
                marker=dict(color=good_color(1.0)),
                yaxis="y2"
            ))
    
            fig.update_layout(
                xaxis=dict(title=pareto_dimension_title),
                yaxis=dict(
                    #title=metric_title,
                    side="left",
                    showgrid=False,
                    rangemode="tozero"
                ),
                yaxis2=dict(
                    #title="Cumulative Percentage",
                    overlaying="y",
                    side="right",
                    range=[0, 1],
                    tickformat=".2%",
                    showgrid=False
                ),
                showlegend=False,
                margin=dict(l=50, r=50, t=50, b=50),
                paper_bgcolor="rgba(0, 0, 0, 0)",
                plot_bgcolor="rgba(0, 0, 0, 0)",
                height=300
            )
    
            # Streamlit UI
            st.plotly_chart(fig, key=f"pareto__{metric_name}")