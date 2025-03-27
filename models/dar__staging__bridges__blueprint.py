
from sqlglot import exp
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model import model
from sqlmesh.core.model.kind import ModelKindName

# Import from our blueprint module
try:
    from models.blueprint_generators import generate_bridge_blueprints
except:
    from blueprint_generators import generate_bridge_blueprints

# Generate blueprints
blueprints = generate_bridge_blueprints(
    hook_config_path="./models/hook__bags.yml"
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
    name = evaluator.var("name")
    source_name = evaluator.var("source_name")
    peripheral = evaluator.var("peripheral")
    column_prefix = evaluator.var("column_prefix")
    primary_hook = evaluator.var("primary_hook")
    hook = evaluator.var("hook")
    dependencies = evaluator.var("dependencies")
    column_descriptions = evaluator.var("column_descriptions")
    column_data_types = evaluator.var("column_data_types")
    
    # Define source CTE
    source_columns = [
        exp.Literal.string(peripheral).as_("peripheral"),
        exp.column(primary_hook),
        exp.column(hook)
    ]
    if dependencies:
        foreign_hooks = [dependency["primary_hook"] for dependency in dependencies.values()]
        source_columns.extend([exp.column(hook) for hook in foreign_hooks])

    source_columns.extend(
        [
            exp.column(f"{column_prefix}__record_loaded_at").as_("bridge__record_loaded_at"),
            exp.column(f"{column_prefix}__record_updated_at").as_("bridge__record_updated_at"),
            exp.column(f"{column_prefix}__record_valid_from").as_("bridge__record_valid_from"),
            exp.column(f"{column_prefix}__record_valid_to").as_("bridge__record_valid_to"),
            exp.column(f"{column_prefix}__is_current_record").as_("bridge__is_current_record"),
        ]
    )

    cte__bridge = exp.select(*source_columns).from_(f"dab.{source_name}")
    previous_cte = "cte__bridge"

    # PIT hook lookup
    cte__pit_lookup = exp.Select().from_(f"cte__bridge")
    cte_pit_lookup__select = []

    # Track dependency tables for validity field resolution
    dependency_tables = []
    
    if dependencies:
        for dependency_name in dependencies.keys():
            dependency_tables.append(dependency_name)

        for dependency_name, dependency_config in dependencies.items():
            hook_to_join_on = dependency_config['primary_hook']
            inherited_hooks = dependency_config['inherited_hooks']

            for inherited_hook in inherited_hooks:
                cte_pit_lookup__select.append(exp.column(inherited_hook, table=dependency_name))

            cte__pit_lookup = cte__pit_lookup.join(
                f"dar__staging.{dependency_name}",
                on=exp.and_(
                    exp.EQ(
                        this=exp.column(hook_to_join_on, table="cte__bridge"),
                        expression=exp.column(hook_to_join_on, table=dependency_name)
                    ),
                    exp.LT(
                        this=exp.column("bridge__record_valid_from", table="cte__bridge"),
                        expression=exp.column("bridge__record_valid_to", table=dependency_name)
                    ),
                    exp.GT(
                        this=exp.column("bridge__record_valid_to", table="cte__bridge"),
                        expression=exp.column("bridge__record_valid_from", table=dependency_name)
                    )
                ),
                join_type="left"
            )

    # Prepare select columns for the bridge model
    select_columns = [
        exp.column("peripheral", table="cte__bridge"),
        exp.column(primary_hook, table="cte__bridge"),
        *cte_pit_lookup__select,
        exp.column(hook, table="cte__bridge"),
    ]
    
    # Only use aggregation functions if there are dependencies
    if dependency_tables:
        # GREATEST for record_loaded_at
        loaded_at_columns = [exp.column("bridge__record_loaded_at", table="cte__bridge")]
        for table in dependency_tables:
            loaded_at_columns.append(exp.column("bridge__record_loaded_at", table=table))
        
        # GREATEST for record_updated_at
        updated_at_columns = [exp.column("bridge__record_updated_at", table="cte__bridge")]
        for table in dependency_tables:
            updated_at_columns.append(exp.column("bridge__record_updated_at", table=table))
        
        # GREATEST for record_valid_from
        valid_from_columns = [exp.column("bridge__record_valid_from", table="cte__bridge")]
        for table in dependency_tables:
            valid_from_columns.append(exp.column("bridge__record_valid_from", table=table))
        
        # LEAST for record_valid_to
        valid_to_columns = [exp.column("bridge__record_valid_to", table="cte__bridge")]
        for table in dependency_tables:
            valid_to_columns.append(exp.column("bridge__record_valid_to", table=table))
        
        # Use LEAST for is_current_record which is functionally equivalent to AND for booleans
        # but produces cleaner SQL
        is_current_columns = [exp.column("bridge__is_current_record", table="cte__bridge")]
        
        # Add each dependency's is_current_record
        for table in dependency_tables:
            is_current_columns.append(exp.column("bridge__is_current_record", table=table))
        
        # Create a LEAST expression with all columns (equivalent to AND for booleans)
        is_current_expr = exp.func("LEAST", *is_current_columns)
            
        # Add the validity fields with aggregation functions
        select_columns.extend([
            exp.func("GREATEST", *loaded_at_columns).as_("bridge__record_loaded_at"),
            exp.func("GREATEST", *updated_at_columns).as_("bridge__record_updated_at"),
            exp.func("GREATEST", *valid_from_columns).as_("bridge__record_valid_from"),
            exp.func("LEAST", *valid_to_columns).as_("bridge__record_valid_to"),
            is_current_expr.as_("bridge__is_current_record")
        ])
    else:
        # If no dependencies, just use the bridge table fields directly
        select_columns.extend([
            exp.column("bridge__record_loaded_at", table="cte__bridge"),
            exp.column("bridge__record_updated_at", table="cte__bridge"),
            exp.column("bridge__record_valid_from", table="cte__bridge"),
            exp.column("bridge__record_valid_to", table="cte__bridge"),
            exp.column("bridge__is_current_record", table="cte__bridge")
        ])
    
    # Construct the select statement with all required columns
    if dependencies:
        cte__pit_lookup = cte__pit_lookup.select(*select_columns)
        previous_cte = "cte__pit_lookup"

    # We need to generate the bridge hook
    cte__bridge_pit__fields = []

    for column in cte_pit_lookup__select:
        cte__bridge_pit__fields.append(exp.column(column.name))

    cte__bridge_pit = exp.select(
        exp.Star(),
        exp.func(
            "CONCAT_WS",
            exp.Literal.string("~"),
            exp.func(
                "CONCAT",
                exp.Literal.string("peripheral|"),
                exp.column("peripheral")
            ),
            exp.func(
                "CONCAT",
                exp.Literal.string("epoch__valid_from|"),
                exp.column("bridge__record_valid_from"),
            ),
            exp.column(primary_hook),
            *cte__bridge_pit__fields
        ).as_("_pit_hook__bridge")
    ).from_(previous_cte)

    # Final query - Explicitly cast all fields
    casted_columns = []

    for col, data_type in column_data_types.items():
        data_type = "text" if data_type in ("xml", "uniqueidentifier") else data_type
        
        casted_column = exp.cast(exp.column(col), exp.DataType.build(data_type))
        
        description = column_descriptions.get(col)
        casted_column.add_comments(comments=[description])
        
        casted_columns.append(casted_column)

    sql = (
        exp.select(*casted_columns)
        .from_(f"cte__bridge_pit")
        .where(
            exp.column("bridge__record_updated_at").between(
                low=evaluator.locals["start_ts"],
                high=evaluator.locals["end_ts"]
            )
        )
        .with_("cte__bridge", as_=cte__bridge)
    )

    if dependencies:
        sql = sql.with_("cte__pit_lookup", as_=cte__pit_lookup)
    
    sql = sql.with_("cte__bridge_pit", as_=cte__bridge_pit)
    
    return sql