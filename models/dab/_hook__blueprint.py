import yaml

from sqlglot import exp
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
    primary_hook = evaluator.var("primary_hook")
    hooks = evaluator.var("hooks")
    columns = evaluator.var("columns")

    # Define source CTE
    cte__source = exp.select(exp.Star()).from_(f"das.{source_table}")

    # Prefix all fields CTE
    cte__prefixed = exp.select(exp.Star()).from_("cte__source")

    # Add SCD Type 2 fields CTE
    cte__scd = exp.select(exp.Star()).from_("cte__prefixed")

    # Define hooks CTE
    hook_selects = [
        exp.column(hook["business_key_field"]).as_(hook["name"])
        for hook in hooks
    ]

    cte__hooks = exp.select(*hook_selects, exp.Star()).from_("cte__scd")

    # Final
    sql = (
        exp.select(exp.Star())
        .from_("cte__hooks")
        .with_("cte__source", as_=cte__source)
        .with_("cte__prefixed", as_=cte__prefixed)
        .with_("cte__scd", as_=cte__scd)
        .with_("cte__hooks", as_=cte__hooks)
    )

    return sql