import os
from typing import Dict, List, Union, Any, Optional

from sqlglot import exp
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model

# Import shared utility functions
from models._blueprint_utils import create_casted_columns

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
    """
    Main entry point function for the raw blueprint model.
    
    This function creates raw data models from Iceberg tables in the lakehouse.
    It performs the following operations:
    1. Extracts configuration variables from the evaluator
    2. Validates that required parameters are present
    3. Constructs a select query from the Iceberg table with proper data types
    4. Adds descriptions to columns as comments
    
    Args:
        evaluator: MacroEvaluator providing access to template variables
        
    Returns:
        SQLGlot expression for the raw model
    """
    # Extract variables from the evaluator with defaults to prevent None errors
    name: str = evaluator.var("name") or ""
    columns: List[Dict[str, str]] = evaluator.var("columns") or []
    column_descriptions: Dict[str, str] = evaluator.var("column_descriptions") or {}

    # Validate required parameters
    if not name or not columns:
        raise ValueError(f"Missing required variables: name={name}, columns={len(columns)}")
    
    # Create the column_data_types dictionary from the columns list
    column_data_types: Dict[str, str] = {col["name"]: col["type"].lower() for col in columns}
    
    # Use the shared function to create casted columns
    select_columns = create_casted_columns(column_data_types, column_descriptions)
    
    # Generate the Iceberg table path directly
    iceberg_path = os.path.abspath(f"./lakehouse/das/{name}").lstrip('/')
    
    # Build the query with SQLGlot
    sql = exp.select(*select_columns).from_(f"ICEBERG_SCAN('file://{iceberg_path}')")
    
    return sql