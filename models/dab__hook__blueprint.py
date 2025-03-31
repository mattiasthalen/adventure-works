from sqlglot import exp
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName

# Import shared utility functions
from models._blueprint_utils import create_casted_columns, create_source_cte, create_scd_columns

# Import from our blueprint module
try:
    from models._blueprint_generators import generate_hook_blueprints
except:
    from _blueprint_generators import generate_hook_blueprints

# Generate blueprints
blueprints = generate_hook_blueprints(
    hook_config_path="./models/hook__frames.yml",
    schema_path="./models/raw_schema.yaml"
)

@model(
    "dab.@{name}",
    is_sql=True,
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        unique_key="@{grain}"
    ),
    blueprints=blueprints,
    grain="@{grain}",
    references="@{references}",
    description="@{description}",
    #column_descriptions="@{column_descriptions}"
)
def entrypoint(evaluator: MacroEvaluator) -> str | exp.Expression:
    """
    Main entry point function for the hook blueprint model.
    """
    # Extract variables from the evaluator
    source_table = evaluator.var("source_table")
    source_primary_keys = evaluator.var("source_primary_keys")
    source_columns = evaluator.var("source_columns")
    column_prefix = evaluator.var("column_prefix") + "__"
    hooks = evaluator.var("hooks")
    columns = evaluator.var("columns")
    column_data_types = evaluator.var("column_data_types")
    column_descriptions = evaluator.var("column_descriptions")

    # Create additional column for record_loaded_at
    loaded_at = exp.func(
        "TO_TIMESTAMP", 
        exp.cast(exp.column("_dlt_load_id"), exp.DataType.build("decimal"))
    ).as_("record_loaded_at")

    # Create source CTE directly using the shared utility function
    cte__source = create_source_cte(source_name=source_table, schema="das", columns=source_columns, additional_columns=[loaded_at])

    # Create SCD columns and SCD CTE
    scd_columns = create_scd_columns(source_primary_keys)
    cte__scd = exp.select(exp.Star(), *scd_columns).from_("cte__source")

    # Process hooks to generate hook CTEs - inlined from original _process_hooks function
    hook_selects = []
    composite_hook_selects = []
    primary_hook_select = None

    for hook in hooks:
        hook_name = hook["name"]

        if "business_key_field" in hook:
            hook_business_key_field = hook["business_key_field"]
            hook_keyset = hook["keyset"]

            hook_select = exp.func(
                "CONCAT",
                exp.Literal.string(f"{hook_keyset}|"),
                exp.cast(exp.column(hook_business_key_field), exp.DataType.build("text"))
            ).as_(hook_name)
            
            hook_selects.append(hook_select)

        elif "composite_key" in hook:
            hook_keys = hook["composite_key"]

            hook_select = exp.func(
                "CONCAT_WS",
                exp.Literal.string("~"),
                *hook_keys
            ).as_(hook_name) 

            composite_hook_selects.append(hook_select)

        if hook.get("primary", False):
            pit_hook_name = f"_pit{hook_name}"

            primary_hook_select = exp.func(
                "CONCAT",
                exp.column(hook_name),
                exp.Literal.string("~epoch__valid_from|"),
                exp.cast(exp.column("record_valid_from"), exp.DataType.build("text"))
            ).as_(pit_hook_name)
    cte__hooks = exp.select(*hook_selects, exp.Star()).from_("cte__scd")
    cte__composite_hooks = exp.select(*composite_hook_selects, exp.Star()).from_("cte__hooks")
    cte__primary_hooks = exp.select(primary_hook_select, exp.Star()).from_("cte__composite_hooks")

    # Create prefixed column CTE - inlined from original _create_prefixed_columns function
    prefixed_columns = []

    for col in columns:
        if col.startswith(("_hook__", "_pit_hook__")):
            column = exp.column(col)
        elif col.startswith(column_prefix):
            stripped_column = col.removeprefix(column_prefix)
            column = exp.column(stripped_column).as_(col)
        else:
            column = exp.column(col)

        prefixed_columns.append(column)
    cte__prefixed = exp.select(*prefixed_columns).from_("cte__primary_hooks")

    # Create properly casted columns for the final query
    casted_columns = create_casted_columns(column_data_types, column_descriptions)

    # Assemble the final query
    sql = (
        exp.select(*casted_columns)
        .from_("cte__prefixed")
        .where(
            exp.column(f"{column_prefix}record_updated_at").between(
                low=evaluator.locals["start_ts"],
                high=evaluator.locals["end_ts"]
            )
        )
        .with_("cte__source", as_=cte__source)
        .with_("cte__scd", as_=cte__scd)
        .with_("cte__hooks", as_=cte__hooks)
        .with_("cte__composite_hooks", as_=cte__composite_hooks)
        .with_("cte__primary_hooks", as_=cte__primary_hooks)
        .with_("cte__prefixed", as_=cte__prefixed)
    )

    return sql