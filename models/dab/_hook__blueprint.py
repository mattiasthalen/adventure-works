import yaml

from sqlglot import exp, parse_one
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model

def load_bags_config(config_path: str) -> dict:
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def generate_blueprints_from_yaml(hook_config_path: str) -> list:
    bags_config = load_bags_config(hook_config_path)

    blueprints = []

    for bag in bags_config["bags"]:

        hooks = {hook["name"]: "binary" for hook in bag["hooks"]}

        blueprint = {
            "name": bag['name'],
            "source_table": bag['source_table'],
            "column_prefix": bag['column_prefix'],
            "hooks": bag['hooks'], 
            "columns": hooks
        }

        blueprints.append(blueprint)

    return blueprints

blueprints = generate_blueprints_from_yaml(
    hook_config_path="./hook/hook__bags.yml"
)

@model(
    "dab.@{name}",
    is_sql=True,
    kind="VIEW",
    blueprints=blueprints[0:3]
)
def entrypoint(evaluator: MacroEvaluator) -> str | exp.Expression:
    source_table = evaluator.var("source_table")
    column_prefix = evaluator.var("column_prefix")
    primary_hook_name = evaluator.var("primary_hook")
    hooks = evaluator.var("hooks")

    source_schema = evaluator.columns_to_types(source_table)
    source_columns = [col for col in source_schema.keys()]

    primary_hook = next((hook for hook in hooks if hook.get("primary")), hooks[0])

    # Define source CTE
    loaded_at = exp.func(
        "TO_TIMESTAMP", 
        exp.cast(exp.column("_dlt_load_id"), exp.DataType.build("decimal"))
    ).as_("record_loaded_at")

    cte__source = exp.select(exp.Star(), loaded_at).from_(f"das.{source_table}")

    # Add SCD Type 2 fields CTE
    partition_clause = [exp.column(primary_hook["business_key_field"])]

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

    # Prefix all fields CTE
    prefixed_columns = [exp.column(col).as_(f"{column_prefix}__{col}") for col in source_columns]
    cte__prefixed = exp.select(exp.Star()).from_("cte__scd")

    # Define hooks CTE
    """ 
    pit_hook = exp.cast(
        exp.func(
            "CONCAT",
            exp.cast(
                exp.column(primary_hook["keyset"]),
                exp.Literal.string("|"),
                exp.column(primary_hook["business_key_field"]),
                exp.DataType.build("text")
            ),
            exp.Literal.string("~epoch__valid_from|"),
            exp.column("record_valid_from")
        ),
        exp.DataType.build("binary")
    ).as_(f"_pit{primary_hook_name}")
 """
    hook_selects = [
        exp.column(hook["business_key_field"]).as_(hook["name"])
        for hook in hooks
    ]

    cte__hooks = exp.select(*hook_selects, exp.Star()).from_("cte__prefixed")

    # Final
    sql = (
        exp.select(exp.Star())
        .from_("cte__hooks")
        .with_("cte__source", as_=cte__source)
        .with_("cte__scd", as_=cte__scd)
        .with_("cte__prefixed", as_=cte__prefixed)
        .with_("cte__hooks", as_=cte__hooks)
    )

    return sql