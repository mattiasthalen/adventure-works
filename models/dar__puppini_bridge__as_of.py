from sqlglot import exp, parse_one
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName

# Import from our blueprint module
try:
    from models._blueprint_generators import import_blueprints
except:
    from _blueprint_generators import import_blueprints

@model(
    "dar._puppini_bridge__as_of",
    is_sql=True,
    kind=ModelKindName.VIEW,
    tags=["puppini_bridge", "unified_star_schema"],
    grain=["@{grain}"],
    #references="@{references}",
    description="Unified viewpoint of all event data: Combined timeline of all business events in the Adventure Works dataset.",
    #column_descriptions="@{column_descriptions}"
)
def entrypoint(evaluator: MacroEvaluator) -> str | exp.Expression:
    
    # Gather all bridge columns and their metadata
    event_blueprints = import_blueprints("./models/blueprints/events")
    
    columns = []
    column_data_types = {}
    column_descriptions = {}

    for blueprint in event_blueprints:
        event_columns = blueprint["columns"]
        event_column_data_types = blueprint["column_data_types"]
        event_column_descriptions = blueprint["column_descriptions"]

        columns.extend(event_columns)
        column_data_types.update(event_column_data_types)
        column_descriptions.update(event_column_descriptions)

    pit_hook_columns = sorted(set([col for col in columns if col.startswith("_pit_hook__") and col != "_pit_hook__bridge"]))
    event_columns = sorted(set([col for col in columns if col.startswith("event__")]))

    columns = [
        "peripheral", 
        "_pit_hook__bridge", 
        *pit_hook_columns, 
        *event_columns,
        "bridge__record_loaded_at",
        "bridge__record_updated_at",
        "bridge__record_valid_from",
        "bridge__record_valid_to",
        "bridge__is_current_record"
    ]

    # Create union of all event bridges
    for idx, blueprint in enumerate(event_blueprints):
        event_name = blueprint["event_name"]
        event_columns = []

        for col in columns:
            if col in blueprint["columns"]:
                event_columns.append(col)
            else:
                event_columns.append(f"NULL AS {col}")
        
        if idx == 0:
            union_sql = f"SELECT {', '.join(event_columns)} FROM dar__staging.{event_name}"
        else:
            union_sql += f" UNION ALL BY NAME SELECT {', '.join(event_columns)} FROM dar__staging.{event_name}"
        
    cte__bridge_union = parse_one(union_sql)

    # Add ghosting
    ghosting_select = []
    for col in columns:
        ghost_select = exp.column(col)

        if col.startswith(("_pit_hook__", "_hook__")):
            ghost_select = exp.func("COALESCE", exp.column(col), exp.Literal.string("ghost_record")).as_(col)
        
        ghosting_select.append(ghost_select)

    cte__ghosting = exp.select(*ghosting_select).from_("cte__bridge_union")

    # Explicit cast and commenting
    casted_columns = []

    for col in columns:
        data_type = column_data_types.get(col)
        data_type = "text" if data_type in ("xml", "uniqueidentifier") else data_type
        
        casted_column = exp.cast(exp.column(col), exp.DataType.build(data_type))
        
        description = column_descriptions.get(col)
        casted_column.add_comments(comments=[description])
        
        casted_columns.append(casted_column)

    sql = (
        exp.select(*casted_columns)
        .from_(f"cte__ghosting")
        .with_("cte__bridge_union", as_=cte__bridge_union)
        .with_("cte__ghosting", as_=cte__ghosting)
    )

    return sql