from sqlglot import exp, parse_one
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName

# Import from our blueprint module
try:
    from models._blueprint_generators import generate_peripheral_blueprints
except:
    from _blueprint_generators import generate_peripheral_blueprints

# Generate blueprints
blueprints = generate_peripheral_blueprints(
    hook_blueprint_path="./models/blueprints/hook"
)

@model(
    "dar.@{peripheral_name}",
    is_sql=True,
    kind=ModelKindName.VIEW,
    blueprints=blueprints,
    tags=["peripheral", "unified_star_schema"],
    grain=["@{grain}"],
    #references="@{references}",
    description="@{description}",
    #column_descriptions="@{column_descriptions}"
)
def entrypoint(evaluator: MacroEvaluator) -> str | exp.Expression:
    column_data_types = evaluator.var("column_data_types")
    column_descriptions = evaluator.var("column_descriptions")
    columns = evaluator.var("columns")
    hook_name = evaluator.var("hook_name")
    grain = evaluator.var("grain")

    cte__source = exp.select(*columns).from_(f"dab.{hook_name}")

    # Create ghost record
    ghost_columns = [exp.Literal.string("ghost_record").as_(grain)]
    for column, data_type in column_data_types.items():

        if column == grain:
            continue

        elif data_type == "text":
            ghost_column = exp.Literal.string("N/A")
        
        elif column.endswith(("__record_loaded_at", "__record_updated_at", "__record_valid_from")):
            ghost_column = exp.cast(exp.Literal.string("1970-01-01 00:00:00"), exp.DataType.build("timestamp"))
        
        elif column.endswith("__record_valid_to"):
            ghost_column = exp.cast(exp.Literal.string("9999-12-31 23:59:59"), exp.DataType.build("timestamp"))
        
        elif column.endswith("__record_version"):
            ghost_column = exp.Literal.number(0)
        
        elif column.endswith("__is_current_record"):
            ghost_column = exp.true()
        
        else:
            ghost_column = exp.Null()
        
        ghost_columns.append(ghost_column.as_(column))

    cte__ghost = exp.select(*ghost_columns)

    # Add ghost record
    cte_union = exp.union(cte__source, cte__ghost)

    # Explicit cast and commenting
    casted_columns = []

    for col, data_type in column_data_types.items():
        data_type = "text" if data_type in ("xml", "uniqueidentifier") else data_type
        
        casted_column = exp.cast(exp.column(col), exp.DataType.build(data_type))
        
        description = column_descriptions.get(col)
        casted_column.add_comments(comments=[description])
        
        casted_columns.append(casted_column)

    sql = (
        exp.select(*casted_columns)
        .from_("cte_union")
        .with_("cte__source", as_=cte__source)
        .with_("cte__ghost", as_=cte__ghost)
        .with_("cte_union", as_=cte_union)
    )

    return sql
