"""
Shared utility functions for SQLMesh blueprint generators.

This module contains common functions used across different blueprint models to 
promote code reuse and consistency.
"""

from sqlglot import exp

def create_casted_columns(column_data_types, column_descriptions):
    """
    Create properly casted columns with descriptions for the final query.
    
    Args:
        column_data_types: Dictionary mapping column names to data types
        column_descriptions: Dictionary mapping column names to descriptions
        
    Returns:
        List of SQLGlot expressions for casted columns
    """
    casted_columns = []

    for col, data_type in column_data_types.items():
        # Handle special data types
        data_type = "text" if data_type in ("xml", "uniqueidentifier") else data_type
        
        # Create cast expression
        casted_column = exp.cast(exp.column(col), exp.DataType.build(data_type))
        
        # Add description as comment if available
        description = column_descriptions.get(col)
        if description:
            casted_column.add_comments(comments=[description])
        
        casted_columns.append(casted_column)
        
    return casted_columns

def create_source_cte(source_name, schema, columns=None, additional_columns=None):
    """
    Create a source CTE by selecting columns from a specified table.
    
    Args:
        source_name: Name of the source table
        schema: Schema where the source table is located
        columns: List of columns to select (if None, select all columns with Star)
        additional_columns: Additional columns to include in the select
        
    Returns:
        SQLGlot expression for the source CTE
    """
    select_columns = []
    
    if columns:
        select_columns.extend([exp.column(col) for col in columns])
    else:
        select_columns.append(exp.Star())
        
    if additional_columns:
        select_columns.extend(additional_columns)
        
    return exp.select(*select_columns).from_(f"{schema}.{source_name}")

def create_bridge_record_metadata_columns(prefix, rename_to_prefix=None):
    """
    Create standard bridge record metadata columns with optional renaming.
    
    Args:
        prefix: Original column prefix
        rename_to_prefix: New prefix to rename columns to (if None, keep original)
        
    Returns:
        List of SQLGlot expressions for bridge record metadata columns
    """
    metadata_suffixes = [
        "record_loaded_at",
        "record_updated_at",
        "record_valid_from",
        "record_valid_to",
        "is_current_record"
    ]
    
    metadata_columns = []
    
    for suffix in metadata_suffixes:
        # Ensure there's a double underscore between prefix and suffix
        col = f"{prefix}__{suffix}" if not prefix.endswith('__') else f"{prefix}{suffix}"
        if rename_to_prefix:
            metadata_columns.append(exp.column(col).as_(f"{rename_to_prefix}{suffix}"))
        else:
            metadata_columns.append(exp.column(col))
            
    return metadata_columns

def create_temporal_join_conditions(left_table, right_table, join_column, temporal_overlap=True, left_prefix="", right_prefix=""):
    """
    Create join conditions for temporal tables with validity periods.
    
    Args:
        left_table: Name of the left table in the join
        right_table: Name of the right table in the join
        join_column: Column name to join on
        temporal_overlap: Whether to include temporal overlap conditions
        left_prefix: Prefix for column names in the left table (e.g., "bridge__")
        right_prefix: Prefix for column names in the right table (e.g., "bridge__")
        
    Returns:
        SQLGlot expression for the join condition
    """
    # Basic join condition on the specified column
    conditions = [
        exp.EQ(
            this=exp.column(join_column, table=left_table),
            expression=exp.column(join_column, table=right_table)
        )
    ]
    
    # Add temporal overlap conditions if requested
    if temporal_overlap:
        # Use the prefixed column names for validity periods
        left_valid_from = f"{left_prefix}record_valid_from"
        left_valid_to = f"{left_prefix}record_valid_to"
        right_valid_from = f"{right_prefix}record_valid_from"
        right_valid_to = f"{right_prefix}record_valid_to"
        
        conditions.extend([
            # Left record's start time is less than right record's end time
            exp.LT(
                this=exp.column(left_valid_from, table=left_table),
                expression=exp.column(right_valid_to, table=right_table)
            ),
            # Left record's end time is greater than right record's start time
            exp.GT(
                this=exp.column(left_valid_to, table=left_table),
                expression=exp.column(right_valid_from, table=right_table)
            )
        ])
    
    # Combine all conditions with AND
    return exp.and_(*conditions)

def create_scd_columns(primary_keys):
    """
    Create SCD Type 2 columns for versioning and validity tracking.
    
    Args:
        primary_keys: List of primary key column names
        
    Returns:
        List of SQLGlot expressions for SCD columns
    """
    partition_clause = [exp.column(col) for col in primary_keys]

    # Record version using ROW_NUMBER()
    record_version = exp.Window(
        this=exp.RowNumber(),
        partition_by=partition_clause,
        order=exp.Order(expressions=[exp.column("record_loaded_at")])
    ).as_("record_version")

    # Record valid from date
    record_valid_from = (
        exp.Case()
        .when(
            condition=exp.column("record_version").eq(exp.Literal.number(1)),
            then=exp.cast(exp.Literal.string("1970-01-01 00:00:00"), exp.DataType.build("timestamp"))
        )
        .else_(exp.column("record_loaded_at"))
    ).as_("record_valid_from")

    # Record valid to date
    record_valid_to = exp.func(
        "COALESCE",
        exp.Window(
            this=exp.Lead(this=exp.column("record_loaded_at")),
            partition_by=partition_clause,
            order=exp.Order(expressions=[exp.column("record_loaded_at")])
        ),
        exp.cast(exp.Literal.string("9999-12-31 23:59:59"), exp.DataType.build("timestamp"))
    ).as_("record_valid_to")
    
    # Is current record flag
    is_current_record = (
        exp.Case()
        .when(
            condition=exp.column("record_valid_to").eq(
                exp.cast(exp.Literal.string("9999-12-31 23:59:59"), exp.DataType.build("timestamp"))
            ),
            then=exp.true()
        )
        .else_(exp.false())
    ).as_("is_current_record")

    # Record updated at timestamp
    record_updated_at = (
        exp.Case()
        .when(
            condition=exp.column("is_current_record"),
            then=exp.column("record_loaded_at")
        )
        .else_(exp.column("record_valid_to"))
    ).as_("record_updated_at")
    
    return [
        record_version, 
        record_valid_from, 
        record_valid_to, 
        is_current_record, 
        record_updated_at
    ]

def create_ghost_column(column, data_type, grain=None):
    """
    Create an appropriate ghost column based on column name and data type.
    
    Args:
        column: Column name
        data_type: Data type of the column
        grain: Grain column name (if provided, return None when column == grain)
        
    Returns:
        SQLGlot expression for the ghost column, or None if column is the grain
    """
    if grain and column == grain:
        return None

    # Handle different data types and column patterns
    if data_type == "text":
        return exp.Literal.string("N/A").as_(column)
    
    elif column.endswith(("__record_loaded_at", "__record_updated_at", "__record_valid_from")):
        return exp.cast(
            exp.Literal.string("1970-01-01 00:00:00"), 
            exp.DataType.build("timestamp")
        ).as_(column)
    
    elif column.endswith("__record_valid_to"):
        return exp.cast(
            exp.Literal.string("9999-12-31 23:59:59"), 
            exp.DataType.build("timestamp")
        ).as_(column)
    
    elif column.endswith("__record_version"):
        return exp.Literal.number(0).as_(column)
    
    elif column.endswith("__is_current_record"):
        return exp.true().as_(column)
    
    else:
        return exp.Null().as_(column)

def create_incremental_filter(ts_column, evaluator):
    """
    Create an incremental filter expression for time-based filtering.
    
    Args:
        ts_column: Column name containing the timestamp to filter on
        evaluator: MacroEvaluator to access start_ts and end_ts
        
    Returns:
        SQLGlot expression for the incremental filter
    """
    return exp.column(ts_column).between(
        low=evaluator.locals["start_ts"],
        high=evaluator.locals["end_ts"]
    )

# Bridge blueprint specific functions
def create_bridge_source_columns(peripheral, primary_hook, hook, column_prefix, dependencies):
    """
    Create source columns for the bridge model.
    
    Args:
        peripheral: Name of the peripheral table
        primary_hook: Name of the primary hook
        hook: Name of the hook
        column_prefix: Column prefix for the source table
        dependencies: Dictionary of dependencies
        
    Returns:
        List of SQLGlot expressions for the source columns
    """
    # Base columns including peripheral name and hooks
    source_columns = [
        exp.Literal.string(peripheral).as_("peripheral"),
        exp.column(primary_hook),
        exp.column(hook)
    ]
    
    # Add foreign hooks if dependencies exist
    if dependencies:
        foreign_hooks = [dependency["primary_hook"] for dependency in dependencies.values()]
        source_columns.extend([exp.column(hook) for hook in foreign_hooks])

    # Add standard bridge record metadata columns using shared utility
    source_columns.extend(create_bridge_record_metadata_columns(column_prefix, "bridge__"))
    
    return source_columns

def create_pit_lookup_cte(dependencies):
    """
    Create the point-in-time lookup CTE with joins to dependency tables.
    
    Args:
        dependencies: Dictionary of dependencies
        
    Returns:
        Tuple of (SQLGlot expression for pit lookup CTE, list of inherited hook expressions,
                   list of dependency table names)
    """
    # Start with base select from bridge CTE
    cte__pit_lookup = exp.Select().from_("cte__bridge")
    cte_pit_lookup__select = []
    dependency_tables = []
    
    # Process dependencies if they exist
    if dependencies:
        # Track dependency tables
        for dependency_name in dependencies.keys():
            dependency_tables.append(dependency_name)

        # Join each dependency and collect inherited hooks
        for dependency_name, dependency_config in dependencies.items():
            hook_to_join_on = dependency_config['primary_hook']
            inherited_hooks = dependency_config['inherited_hooks']

            # Collect inherited hooks from this dependency
            for inherited_hook in inherited_hooks:
                cte_pit_lookup__select.append(exp.column(inherited_hook, table=dependency_name))

            # Join the dependency table with temporal validity overlapping conditions
            cte__pit_lookup = cte__pit_lookup.join(
                f"dar__staging.{dependency_name}",
                on=create_temporal_join_conditions(
                    "cte__bridge", 
                    dependency_name, 
                    hook_to_join_on, 
                    left_prefix="bridge__", 
                    right_prefix="bridge__"
                ),
                join_type="left"
            )
    
    return cte__pit_lookup, cte_pit_lookup__select, dependency_tables

def create_bridge_select_columns(peripheral, primary_hook, hook, cte_pit_lookup__select, dependency_tables):
    """
    Create select columns for the bridge model, handling dependencies appropriately.
    
    Args:
        peripheral: Name of the peripheral table
        primary_hook: Name of the primary hook
        hook: Name of the hook
        cte_pit_lookup__select: List of expressions for inherited hooks
        dependency_tables: List of dependency table names
        
    Returns:
        List of SQLGlot expressions for the select columns
    """
    # Base columns - peripheral, primary hook, inherited hooks, and hook
    select_columns = [
        exp.column("peripheral", table="cte__bridge"),
        exp.column(primary_hook, table="cte__bridge"),
        *cte_pit_lookup__select,
        exp.column(hook, table="cte__bridge"),
    ]
    
    # Add bridge record metadata columns
    select_columns.extend([
        exp.column("bridge__record_loaded_at", table="cte__bridge"),
        exp.column("bridge__record_updated_at", table="cte__bridge"),
        exp.column("bridge__record_valid_from", table="cte__bridge"),
        exp.column("bridge__record_valid_to", table="cte__bridge"),
        exp.column("bridge__is_current_record", table="cte__bridge"),
    ])
    
    return select_columns

def create_bridge_pit_cte(primary_hook, cte_pit_lookup__select, previous_cte):
    """
    Create the bridge point-in-time CTE with the bridge PIT hook.
    
    Args:
        primary_hook: Name of the primary hook
        cte_pit_lookup__select: List of expressions for inherited hooks
        previous_cte: Name of the previous CTE to select from
        
    Returns:
        SQLGlot expression for the bridge PIT CTE
    """
    # Extract hook fields
    cte__bridge_pit__fields = []
    for column in cte_pit_lookup__select:
        cte__bridge_pit__fields.append(exp.column(column.name))

    # Create the bridge PIT hook expression
    bridge_pit_hook = exp.func(
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
    
    # Create the CTE with the bridge PIT hook
    return exp.select(exp.Star(), bridge_pit_hook).from_(previous_cte)
