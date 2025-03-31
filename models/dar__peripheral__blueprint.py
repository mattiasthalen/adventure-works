from typing import Dict, List, Union, Any, Optional
from sqlglot import exp, parse_one
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName

# Import shared utility functions
from models._blueprint_utils import create_casted_columns, create_ghost_column, create_source_cte

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
def entrypoint(evaluator: MacroEvaluator) -> Union[str, exp.Expression]:
    """
    Main entry point function for the peripheral blueprint model.
    
    This function transforms hook data into a peripheral model by creating
    appropriate ghost columns for each data type and handling grain columns.
    It performs the following operations:
    1. Extracts configuration variables from the evaluator
    2. Creates a source CTE from the hook model
    3. Generates ghost columns based on data types
    4. Assembles the final query with proper column casting
    
    Args:
        evaluator: MacroEvaluator providing access to template variables
        
    Returns:
        SQLGlot expression for the peripheral model
    """
    # Extract variables from the evaluator
    column_data_types = evaluator.var("column_data_types") or {}
    column_descriptions = evaluator.var("column_descriptions") or {}
    columns = evaluator.var("columns") or []
    hook_name = evaluator.var("hook_name") or ""
    grain = evaluator.var("grain") or ""
    
    if not hook_name or not grain:
        raise ValueError(f"Missing required variables: hook_name={hook_name}, grain={grain}")

    # Create source CTE from the hook model
    cte__source = create_source_cte(source_name=hook_name, schema="dab", columns=columns)

    # Create ghost record CTE
    ghost_columns = [exp.Literal.string("ghost_record").as_(grain)]
    
    for column, data_type in column_data_types.items():
        ghost_column = create_ghost_column(column, data_type, grain)
        if ghost_column:
            ghost_columns.append(ghost_column)

    cte__ghost = exp.select(*ghost_columns)

    # Union the source and ghost record CTEs
    cte_union = exp.union(cte__source, cte__ghost)

    # Create casted columns for the final select
    casted_columns = create_casted_columns(column_data_types, column_descriptions)

    # Assemble the final query
    sql = (
        exp.select(*casted_columns)
        .from_("cte_union")
        .with_("cte__source", as_=cte__source)
        .with_("cte__ghost", as_=cte__ghost)
        .with_("cte_union", as_=cte_union)
    )

    return sql
