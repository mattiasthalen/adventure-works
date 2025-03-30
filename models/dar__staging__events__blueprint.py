
from sqlglot import exp, parse_one
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName

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
    bridge_name = evaluator.var("bridge_name")
    column_data_types = evaluator.var("column_data_types")
    column_descriptions = evaluator.var("column_descriptions")
    columns = evaluator.var("columns")
    date_columns = evaluator.var("date_columns")
    description = evaluator.var("description")
    hook_name = evaluator.var("hook_name")
    event_name = evaluator.var("event_name")
    primary_pit_hook = evaluator.var("primary_pit_hook")
    
    # Define event CTE
    bridge_columns = [exp.column(col, table=bridge_name) for col in columns if col not in date_columns.values() and col != "_hook__epoch__date"]
    event_columns = [exp.column(col, table=hook_name) for col in date_columns.keys()]
    cte__events = exp.select(*bridge_columns, *event_columns).from_(f"dar__staging.{bridge_name}").join(
        f"dab.{hook_name}",
        using=primary_pit_hook,
        join_type="left"
    )

    # Define pivot CTE
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

    # Parse the SQL string with SQLGlot
    cte__pivot = parse_one(unpivot_sql)
    
    # Aggregated CTE
    cte__aggregate = f"""
    SELECT
        {primary_pit_hook},
        CONCAT('epoch__date|', event_date) AS _hook__epoch__date,
        {', '.join([f'MAX(event = \'{old_name}\') AS {new_name}' for old_name, new_name in date_columns.items()])}
    FROM cte__pivot
    GROUP BY ALL
    ORDER BY ALL
    """

    # Define final CTE
    final_columns = [exp.column(col) for col in columns if col != "_pit_hook__bridge"]
    cte__final = exp.select(
        exp.func(
            "CONCAT_WS",
            exp.Literal.string("~"),
            exp.column("_pit_hook__bridge"),
            exp.column("_hook__epoch__date")
        ).as_("_pit_hook__bridge"),
        *final_columns
    ).from_(f"dar__staging.{bridge_name}").join(
        "cte__aggregate",
        using=primary_pit_hook,
        join_type="left"
    )

    # Final query - Explicitly cast all fields
    casted_columns = []

    for col, data_type in column_data_types.items():
        data_type = "text" if data_type in ("xml", "uniqueidentifier") else data_type
        
        casted_column = exp.cast(exp.column(col), exp.DataType.build(data_type))
        
        description = column_descriptions.get(col)
        casted_column.add_comments(comments=[description])
        
        casted_columns.append(casted_column)

    sql = (
        exp.select(*casted_columns)
        .from_(f"cte__final")
        .where(
            exp.column("bridge__record_updated_at").between(
                low=evaluator.locals["start_ts"],
                high=evaluator.locals["end_ts"]
            )
        )
        .with_("cte__events", as_=cte__events)
        .with_("cte__pivot", as_=cte__pivot)
        .with_("cte__aggregate", as_=cte__aggregate)
        .with_("cte__final", as_=cte__final)
    )
    
    return sql