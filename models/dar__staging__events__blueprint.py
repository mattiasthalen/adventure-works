
from sqlglot import exp, parse_one
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName

# Import shared utility functions
from models._blueprint_utils import create_casted_columns, create_incremental_filter

# Import from our blueprint module
try:
    from models._blueprint_generators import generate_event_blueprints
except:
    from _blueprint_generators import generate_event_blueprints

# Generate blueprints
blueprints = generate_event_blueprints(
        hook_blueprint_path="./models/blueprints/hook",
        bridge_blueprint_path="./models/blueprints/bridges"
    )

@model(
    "dar__staging.@{event_name}",
    is_sql=True,
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        unique_key="_pit_hook__bridge"
    ),
    blueprints=blueprints,
    tags=["puppini_event_bridge"],
    grain=["_pit_hook__bridge"],
    #references="@{references}",
    description="@{description}",
    #column_descriptions="@{column_descriptions}"
)
def entrypoint(evaluator: MacroEvaluator) -> str | exp.Expression:
    """
    Main entry point function for the event blueprint model.
    
    Args:
        evaluator: MacroEvaluator providing access to template variables
        
    Returns:
        SQLGlot expression for the event model
    """
    # Extract variables from the evaluator with defaults to prevent None errors
    bridge_name = evaluator.var("bridge_name") or ""
    column_data_types = evaluator.var("column_data_types") or {}
    column_descriptions = evaluator.var("column_descriptions") or {}
    columns = evaluator.var("columns") or []
    date_columns = evaluator.var("date_columns") or {}
    hook_name = evaluator.var("hook_name") or ""
    primary_pit_hook = evaluator.var("primary_pit_hook") or ""
    
    # Validate required parameters
    if not bridge_name or not hook_name or not primary_pit_hook:
        raise ValueError(f"Missing required variables: bridge_name={bridge_name}, "
                         f"hook_name={hook_name}, primary_pit_hook={primary_pit_hook}")

    # Create event CTE joining bridge and hook tables
    # Get columns from bridge table (excluding date event columns)
    bridge_columns = [
        exp.column(col, table=bridge_name) 
        for col in columns 
        if col not in date_columns.values() and col != "_hook__epoch__date"
    ]
    
    # Get date columns from hook table
    event_columns = [exp.column(col, table=hook_name) for col in date_columns.keys()]
    
    # Create the joined CTE
    cte__events = exp.select(*bridge_columns, *event_columns).from_(f"dar__staging.{bridge_name}").join(
        f"dab.{hook_name}",
        using=primary_pit_hook,
        join_type="left"
    )

    # Create unpivot CTE
    unpivot_columns = ", ".join(date_columns.keys())
    
    # Create the SQL string for UNPIVOT since SQLGlot's Pivot class doesn't generate the correct syntax
    unpivot_sql = f"""
    SELECT
        {primary_pit_hook},
        event,
        event_date
    FROM cte__events
    UNPIVOT (
        event_date FOR event IN (
            {unpivot_columns}
        )
    ) AS pivot__events
    """
    cte__pivot = parse_one(unpivot_sql)

    # Create aggregate CTE
    # Create expressions for converting event names to boolean flags
    event_expressions = [f'MAX(event = \'{old_name}\') AS {new_name}' 
                        for old_name, new_name in date_columns.items()]
    
    # Create SQL for the aggregation to generate event flags
    aggregate_sql = f"""
    SELECT
        {primary_pit_hook},
        CONCAT('epoch__date|', event_date) AS _hook__epoch__date,
        {', '.join(event_expressions)}
    FROM cte__pivot
    GROUP BY ALL
    ORDER BY ALL
    """
    cte__aggregate = parse_one(aggregate_sql)

    # Create final CTE combining bridge data with event flags
    # Get all columns except the bridge hook which we'll reconstruct
    final_columns = [exp.column(col) for col in columns if col != "_pit_hook__bridge"]
    
    # Create the new bridge hook expression that includes the epoch date hook
    bridge_hook_expr = exp.func(
        "CONCAT_WS",
        exp.Literal.string("~"),
        exp.column("_pit_hook__bridge"),
        exp.column("_hook__epoch__date")
    ).as_("_pit_hook__bridge")
    
    # Create the joined CTE
    cte__final = exp.select(
        bridge_hook_expr,
        *final_columns
    ).from_(f"dar__staging.{bridge_name}").join(
        "cte__aggregate",
        using=primary_pit_hook,
        join_type="left"
    )

    # Create casted columns for the final query
    casted_columns = create_casted_columns(column_data_types, column_descriptions)

    # Assemble the final query
    sql = (
        exp.select(*casted_columns)
        .from_("cte__final")
        .where(create_incremental_filter("bridge__record_updated_at", evaluator))
        .with_("cte__events", as_=cte__events)
        .with_("cte__pivot", as_=cte__pivot)
        .with_("cte__aggregate", as_=cte__aggregate)
        .with_("cte__final", as_=cte__final)
    )
    
    return sql