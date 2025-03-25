import os

from sqlglot import exp
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model

# Import from our blueprint module
try:
    from models.blueprint_generators import generate_raw_blueprints
except:
    from blueprint_generators import generate_raw_blueprints

# Generate blueprints
blueprints = generate_raw_blueprints(
    schema_path="./models/raw_schema.yaml"
)

@model(
    "das.@{table_name}",
    is_sql=True,
    kind="VIEW",
    enabled=True,
    blueprints=blueprints,
    description="@{description}",
    #column_descriptions="@{column_descriptions}"
)
def entrypoint(evaluator: MacroEvaluator) -> exp.Expression:
    table_name = evaluator.var("table_name")
    columns = evaluator.var("columns")

    # Build column expressions
    select_columns = []
    for col in columns:
        col_name = col["name"]
        data_type = col["type"]

        data_type = "text" if data_type in ("xml", "uniqueidentifier") else data_type
        
        # Create a cast expression for the column
        select_columns.append(
            exp.cast(
                exp.column(col_name),
                exp.DataType.build(data_type.lower())
            )
        )
    
    iceberg_path = os.path.abspath(f"./lakehouse/das/{table_name}").lstrip('/')
    
    # Build the query with SQLGlot
    sql = exp.select(*select_columns).from_(f"ICEBERG_SCAN('file://{iceberg_path}')")
    
    return sql