from sqlglot import exp
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName

# Import from our blueprint module
try:
    from models.blueprint_generators import generate_hook_blueprints
except:
    from blueprint_generators import generate_hook_blueprints

# Generate blueprints
blueprints = generate_hook_blueprints(
    hook_config_path="./models/hook__bags.yml",
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
    source_table = evaluator.var("source_table")
    source_primary_keys = evaluator.var("source_primary_keys")
    source_columns = evaluator.var("source_columns")
    column_prefix = evaluator.var("column_prefix") + "__"
    hooks = evaluator.var("hooks")
    columns = evaluator.var("columns")
    column_data_types = evaluator.var("column_data_types")

    # Define source CTE
    loaded_at = exp.func(
        "TO_TIMESTAMP", 
        exp.cast(exp.column("_dlt_load_id"), exp.DataType.build("decimal"))
    ).as_("record_loaded_at")

    cte__source = exp.select(*source_columns, loaded_at).from_(f"das.{source_table}")

    # Add SCD Type 2 fields CTE
    partition_clause = [exp.column(col) for col in source_primary_keys]

    record_version = exp.Window(
        this=exp.RowNumber(),
        partition_by=partition_clause,
        order=exp.Order(expressions=[exp.column("record_loaded_at")])
    ).as_("record_version")

    record_valid_from = (
        exp.Case()
        .when(
            condition=exp.column("record_version").eq(exp.Literal.number(1)),
            then=exp.cast(exp.Literal.string("1970-01-01 00:00:00"), exp.DataType.build("timestamp"))
        )
        .else_(exp.column("record_loaded_at"))
    ).as_("record_valid_from")

    record_valid_to = exp.func(
        "COALESCE",
        exp.Window(
            this=exp.Lead(this=exp.column("record_loaded_at")),
            partition_by=partition_clause,
            order=exp.Order(expressions=[exp.column("record_loaded_at")])
        ),
        exp.cast(exp.Literal.string("9999-12-31 23:59:59"), exp.DataType.build("timestamp"))
    ).as_("record_valid_to")
    
    is_current_record = (
        exp.Case()
        .when(
            condition=exp.column("record_valid_to").eq(exp.cast(exp.Literal.string("9999-12-31 23:59:59"), exp.DataType.build("timestamp"))),
            then=exp.true()
        )
        .else_(exp.false())
    ).as_("is_current_record")

    record_updated_at = (
        exp.Case()
        .when(
            condition=exp.column("is_current_record"),
            then=exp.column("record_loaded_at")
        )
        .else_(exp.column("record_valid_to"))
    ).as_("record_updated_at")
    
    scd_columns = [record_version, record_valid_from, record_valid_to, is_current_record, record_updated_at]

    cte__scd = exp.select(exp.Star(), *scd_columns).from_("cte__source")

    # Define hooks CTE
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
    

    # Prefix all fields CTE
    prefixed_columns = []

    for col in columns:
        if col.startswith(("_hook__", "_pit_hook__")):
            column = exp.column(col)

        if col.startswith(column_prefix):
            stripped_column = col.removeprefix(column_prefix)
            column = exp.column(stripped_column).as_(col)

        prefixed_columns.append(column)

    cte__prefixed = exp.select(*prefixed_columns).from_("cte__primary_hooks")

    # Final query - Explicitly cast all fields
    casted_columns = []

    for col, data_type in column_data_types.items():
        data_type = "text" if data_type in ("xml", "uniqueidentifier") else data_type
        casted_column = exp.cast(exp.column(col), exp.DataType.build(data_type))
        
        casted_columns.append(casted_column)

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