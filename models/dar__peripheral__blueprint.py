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

    # Explicit cast and commenting
    casted_columns = []

    for col, data_type in column_data_types.items():
        data_type = "text" if data_type in ("xml", "uniqueidentifier") else data_type
        
        casted_column = exp.cast(exp.column(col), exp.DataType.build(data_type))
        
        description = column_descriptions.get(col)
        casted_column.add_comments(comments=[description])
        
        casted_columns.append(casted_column)

    sql = exp.select(*casted_columns).from_(f"dab.{hook_name}")

    return sql
