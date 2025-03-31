from sqlglot import exp
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName

# Import shared utility functions
from models._blueprint_utils import (
    create_casted_columns, 
    create_incremental_filter,
    create_bridge_source_columns,
    create_pit_lookup_cte,
    create_bridge_select_columns,
    create_bridge_pit_cte
)

# Import from our blueprint module
try:
    from models._blueprint_generators import generate_bridge_blueprints
except:
    from _blueprint_generators import generate_bridge_blueprints

# Generate blueprints
blueprints = generate_bridge_blueprints(
    hook_config_path="./models/hook__frames.yml"
)

@model(
    "dar__staging.@{name}",
    is_sql=True,
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        unique_key="_pit_hook__bridge"
    ),
    blueprints=blueprints,
    tags=["puppini_bridge"],
    grain=["_pit_hook__bridge"],
    #references="@{references}",
    description="@{description}",
    #column_descriptions="@{column_descriptions}"
)
def entrypoint(evaluator: MacroEvaluator) -> str | exp.Expression:
    """
    Main entry point function for the bridge blueprint model.
    
    Args:
        evaluator: MacroEvaluator providing access to template variables
        
    Returns:
        SQLGlot expression for the bridge model
    """
    # Extract variables from the evaluator
    name = evaluator.var("name")
    source_name = evaluator.var("source_name")
    peripheral = evaluator.var("peripheral")
    column_prefix = evaluator.var("column_prefix")
    primary_hook = evaluator.var("primary_hook")
    hook = evaluator.var("hook")
    dependencies = evaluator.var("dependencies")
    column_descriptions = evaluator.var("column_descriptions")
    column_data_types = evaluator.var("column_data_types")
    
    # Create source columns and bridge CTE
    source_columns = create_bridge_source_columns(peripheral, primary_hook, hook, column_prefix, dependencies)
    cte__bridge = exp.select(*source_columns).from_(f"dab.{source_name}")
    previous_cte = "cte__bridge"

    # Create PIT lookup CTE with dependency joins if needed
    cte__pit_lookup, cte_pit_lookup__select, dependency_tables = create_pit_lookup_cte(dependencies)

    # Create select columns with appropriate aggregation for dependencies
    select_columns = create_bridge_select_columns(peripheral, primary_hook, hook, cte_pit_lookup__select, dependency_tables)
    
    # Update PIT lookup CTE with select columns if dependencies exist
    if dependencies:
        cte__pit_lookup = cte__pit_lookup.select(*select_columns)
        previous_cte = "cte__pit_lookup"

    # Create bridge PIT CTE with the bridge PIT hook
    cte__bridge_pit = create_bridge_pit_cte(primary_hook, cte_pit_lookup__select, previous_cte)

    # Create properly casted columns for the final query
    casted_columns = create_casted_columns(column_data_types, column_descriptions)

    # Assemble the final query
    sql = (
        exp.select(*casted_columns)
        .from_("cte__bridge_pit")
        .where(create_incremental_filter("bridge__record_updated_at", evaluator))
        .with_("cte__bridge", as_=cte__bridge)
    )

    # Add the pit lookup CTE if it exists
    if dependencies:
        sql = sql.with_("cte__pit_lookup", as_=cte__pit_lookup)

    # Add the bridge pit CTE
    sql = sql.with_("cte__bridge_pit", as_=cte__bridge_pit)

    return sql