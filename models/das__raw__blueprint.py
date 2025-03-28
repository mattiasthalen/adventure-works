import os

from sqlglot import exp
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model

# Import from our blueprint module
try:
    from models._blueprint_generators import generate_raw_blueprints
except:
    from _blueprint_generators import generate_raw_blueprints

# Generate blueprints
blueprints = generate_raw_blueprints(
    schema_path="./models/raw_schema.yaml"
)

@model(
    "das.@{name}",
    is_sql=True,
    kind="VIEW",
    enabled=True,
    blueprints=blueprints,
    description="@{description}"
)
def entrypoint(evaluator: MacroEvaluator) -> exp.Expression:
    name = evaluator.var("name")
    columns = evaluator.var("columns")
    column_descriptions = evaluator.var("column_descriptions")

    # Build column expressions
    select_columns = []
    for col in columns:
        col_name = col["name"]
        data_type = col["type"]

        data_type = "text" if data_type in ("xml", "uniqueidentifier") else data_type

        # Create a cast expression for the column
        casted_column = exp.cast(exp.column(col_name), exp.DataType.build(data_type.lower()))
        
        description = column_descriptions.get(col_name)
        casted_column.add_comments(comments=[description])
        
        select_columns.append(casted_column)

    
    iceberg_path = os.path.abspath(f"./lakehouse/das/{name}").lstrip('/')
    
    # Build the query with SQLGlot
    sql = exp.select(*select_columns).from_(f"ICEBERG_SCAN('file://{iceberg_path}')")
    
    return sql