import os
from parse_yaml import load_schema, load_bags_config, ensure_directory_exists, map_data_type, format_hook_description

def is_composite_hook(hook):
    """Check if a hook definition is for a composite key"""
    return any(key in hook for key in ['composite_key', 'composite', 'composite_key_fields'])

def get_composite_parts(hook):
    """Get the component parts of a composite key, handling different formats"""
    if 'composite_key' in hook:
        return hook['composite_key']
    elif 'composite' in hook:
        return hook['composite']
    elif 'composite_key_fields' in hook:
        return hook['composite_key_fields']
    return []

def generate_validity_cte(bag, primary_hook):
    """Generate the validity CTE with appropriate partitioning for composite keys"""
    column_prefix = bag['column_prefix']
    
    # Determine partition field(s)
    partition_fields = []
    if not is_composite_hook(primary_hook):
        # Simple primary key
        partition_fields.append(f"{column_prefix}__{primary_hook['business_key_field']}")
    else:
        # Composite key - use all component fields
        components = get_composite_parts(primary_hook)
        for component_name in components:
            # Find this component hook
            for h in bag['hooks']:
                if h['name'] == component_name and 'business_key_field' in h:
                    partition_fields.append(f"{column_prefix}__{h['business_key_field']}")
                    break
    
    # If we couldn't find any fields, use a default approach
    if not partition_fields:
        print(f"Warning: Could not determine partition fields for {bag['name']}")
        return f"""validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY {column_prefix}__record_loaded_at) AS {column_prefix}__record_version,
    '1970-01-01 00:00:00'::TIMESTAMP AS {column_prefix}__record_valid_from,
    '9999-12-31 23:59:59'::TIMESTAMP AS {column_prefix}__record_valid_to,
    TRUE AS {column_prefix}__is_current_record,
    {column_prefix}__record_loaded_at AS {column_prefix}__record_updated_at
  FROM staging
)"""
    
    # Create the validity CTE with proper partitioning
    partition_clause = f"PARTITION BY {', '.join(partition_fields)}"
    
    return f"""validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER ({partition_clause} ORDER BY {column_prefix}__record_loaded_at) AS {column_prefix}__record_version,
    CASE
      WHEN {column_prefix}__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE {column_prefix}__record_loaded_at
    END AS {column_prefix}__record_valid_from,
    COALESCE(
      LEAD({column_prefix}__record_loaded_at) OVER ({partition_clause} ORDER BY {column_prefix}__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS {column_prefix}__record_valid_to,
    {column_prefix}__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS {column_prefix}__is_current_record,
    CASE
      WHEN {column_prefix}__is_current_record
      THEN {column_prefix}__record_loaded_at
      ELSE {column_prefix}__record_valid_to
    END AS {column_prefix}__record_updated_at
  FROM staging
)"""

def generate_hooks_cte(bag):
    """Generate the hooks CTE including composite keys"""
    hooks_cte = "hooks AS (\n  SELECT\n"
    column_prefix = bag['column_prefix']
    hooks = bag['hooks']
    
    # Process regular hooks first
    regular_hooks = []
    composite_hooks = []
    
    for hook in hooks:
        if is_composite_hook(hook):
            composite_hooks.append(hook)
        else:
            regular_hooks.append(hook)
    
    # Generate all regular hooks first
    for hook in regular_hooks:
        keyset = hook.get('keyset', '')
        business_key_field = hook.get('business_key_field', '')
        hook_name = hook['name']
        hooks_cte += f"    CONCAT('{keyset}|', {column_prefix}__{business_key_field}) AS {hook_name},\n"
    
    # Generate composite hooks
    for hook in composite_hooks:
        component_hooks = get_composite_parts(hook)
        hook_name = hook['name']
        hooks_cte += f"    CONCAT_WS('~', {', '.join(component_hooks)}) AS {hook_name},\n"
    
    # Get primary hook
    primary_hook = next((h for h in hooks if h.get('primary')), hooks[0])
    primary_hook_name = primary_hook['name']
    
    # Generate PIT hook
    pit_hook_name = primary_hook_name.replace('_hook', '_pit_hook', 1)
    hooks_cte += f"""    CONCAT_WS('~',
      {primary_hook_name},
      'epoch__valid_from|'||{column_prefix}__record_valid_from
    ) AS {pit_hook_name},\n"""
    
    # Add all columns from validity
    hooks_cte += "    *\n  FROM validity\n)"
    
    return hooks_cte, primary_hook, pit_hook_name

def get_temporal_column_descriptions():
    """Return descriptions for the temporal columns added in hook models"""
    return {
        "record_loaded_at": "Timestamp when this record was loaded into the system",
        "record_updated_at": "Timestamp when this record was last updated",
        "record_version": "Version number for this record",
        "record_valid_from": "Timestamp from which this record version is valid",
        "record_valid_to": "Timestamp until which this record version is valid",
        "is_current_record": "Flag indicating if this is the current valid version of the record"
    }

def get_hook_column_descriptions(bag):
    """Generate descriptions for hook columns"""
    descriptions = {}
    
    # Describe all regular hooks
    for hook in bag['hooks']:
        hook_name = hook['name']
        # Extract the concept from the hook name
        parts = hook_name.split('__')
        if len(parts) >= 2:
            concept = parts[1]
            if len(parts) >= 3:
                qualifier = parts[2]
                descriptions[hook_name] = f"Reference hook to {qualifier} {concept}"
            else:
                descriptions[hook_name] = f"Reference hook to {concept}"
    
    # Describe PIT hook
    primary_hook = next((h for h in bag['hooks'] if h.get('primary')), bag['hooks'][0])
    pit_hook_name = primary_hook['name'].replace('_hook', '_pit_hook', 1)
    descriptions[pit_hook_name] = "Point-in-time hook that combines the primary hook with a validity timestamp"
    
    return descriptions

def generate_hook_bags(output_dir, raw_schema):
    """Generate hook model SQL files based on bags configuration with composite key support"""
    # Ensure output directory exists
    ensure_directory_exists(output_dir)
    
    # Load YAML files
    bags_config = load_bags_config()
    schema = load_schema()
    
    # Counter for generated models
    count = 0
    
    # Process each bag
    for bag in bags_config['bags']:
        success = generate_hook_model_for_bag(bag, schema, output_dir, raw_schema)
        if success:
            count += 1
    
    print(f"Generated {count} hook models in {output_dir}")
    return count

def generate_hook_model_for_bag(bag, schema, output_dir, raw_schema):
    """Generate a hook model SQL file for a specific bag with composite key support"""
    bag_name = bag['name']
    source_table = bag['source_table']
    column_prefix = bag['column_prefix']
    hooks = bag['hooks']
    
    # Skip _dlt tables
    if source_table.startswith('_dlt'):
        return False
    
    # Get primary hook
    primary_hook = next((h for h in hooks if h.get('primary')), hooks[0])
    
    # Get source table schema
    table_columns = {}
    original_description = ""
    if source_table in schema['tables']:
        table_info = schema['tables'][source_table]
        table_columns = table_info['columns']
        original_description = table_info.get('description', '')
    else:
        print(f"Warning: Table {source_table} not found in schema")
        return False
    
    # Extract entity name for description
    entity_name = bag_name.replace('bag__adventure_works__', '')
    
    # Format the description
    table_description = format_hook_description(entity_name, original_description)
    
    # Filter columns
    filtered_columns = {col: props for col, props in table_columns.items() 
                       if not col.startswith('_dlt_') and col != 'modified_date'}
    
    # Get reference hooks
    reference_hooks = [h['name'] for h in hooks if h != primary_hook]
    
    # Generate hooks CTE first to determine the pit_hook_name
    hooks_cte, primary_hook, pit_hook_name = generate_hooks_cte(bag)
    
    # Get column descriptions from source table
    column_descriptions = {}
    for col_name, col_info in filtered_columns.items():
        if 'description' in col_info:
            column_descriptions[f"{column_prefix}__{col_name}"] = col_info['description'].replace("'", "''")
    
    # Add descriptions for temporal and hook columns
    temporal_descriptions = get_temporal_column_descriptions()
    for field, desc in temporal_descriptions.items():
        column_descriptions[f"{column_prefix}__{field}"] = desc
    
    # Add hook column descriptions
    hook_descriptions = get_hook_column_descriptions(bag)
    for hook_name, desc in hook_descriptions.items():
        column_descriptions[hook_name] = desc
    
    # Generate SQL file
    sql_path = os.path.join(output_dir, f"{bag_name}.sql")
    with open(sql_path, 'w') as sql_file:
        # MODEL declaration
        sql_file.write(f"""MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key {pit_hook_name}
  ),
  tags hook,
  grain ({pit_hook_name}, {primary_hook['name']}),
  description '{table_description.replace("'", "''")}'""")
        
        # Add references only if there are any
        if reference_hooks:
            sql_file.write(f",\n  references ({', '.join(reference_hooks)})")
        
        # Add column descriptions if any
        if column_descriptions:
            sql_file.write(",\n  column_descriptions (\n")
            col_desc_items = list(column_descriptions.items())
            for i, (col_name, desc) in enumerate(col_desc_items):
                sql_file.write(f"    {col_name} = '{desc}'")
                if i < len(col_desc_items) - 1:
                    sql_file.write(",")
                sql_file.write("\n")
            sql_file.write("  )")
        
        sql_file.write("\n);\n\n")
        
        # Staging CTE
        sql_file.write("WITH staging AS (\n  SELECT\n")
        
        # For shifts, we need special handling of time fields
        is_shifts_table = source_table == "raw__adventure_works__shifts"
        time_fields = ["start_time", "end_time"] if is_shifts_table else []
        
        # Add all columns, with transformation for time fields if needed
        for i, col in enumerate(filtered_columns.keys()):
            if col in time_fields:
                # Apply the time transformation for start_time and end_time
                sql_file.write(f"    MAKE_TIME(REGEXP_EXTRACT({col}, 'PT(\\d+)H', 1)::INT, 0, 0) AS {column_prefix}__{col},\n")
            else:
                sql_file.write(f"    {col} AS {column_prefix}__{col},\n")
        
        sql_file.write(f"    modified_date AS {column_prefix}__modified_date,\n")
        sql_file.write(f"    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS {column_prefix}__record_loaded_at\n")
        sql_file.write(f"  FROM {raw_schema}.{source_table}\n")
        sql_file.write("), ")
        
        # Validity CTE
        validity_cte = generate_validity_cte(bag, primary_hook)
        sql_file.write(f"{validity_cte}, ")
        
        # Hooks CTE
        sql_file.write(hooks_cte + "\n")
        
        # Final SELECT
        sql_file.write("SELECT\n")
        sql_file.write(f"  {pit_hook_name}::BLOB,\n")
        
        # Add all hooks
        for hook in hooks:
            sql_file.write(f"  {hook['name']}::BLOB,\n")
        
        # Add data columns with types
        for col in filtered_columns.keys():
            data_type = filtered_columns[col].get('data_type', 'text')
            
            # For time fields, we need to map properly
            if col in time_fields:
                sql_type = "TIME"
            else:
                sql_type = map_data_type(data_type, col)
            
            sql_file.write(f"  {column_prefix}__{col}::{sql_type},\n")
        
        # Add system columns
        sql_file.write(f"""  {column_prefix}__modified_date::DATE,
  {column_prefix}__record_loaded_at::TIMESTAMP,
  {column_prefix}__record_updated_at::TIMESTAMP,
  {column_prefix}__record_version::TEXT,
  {column_prefix}__record_valid_from::TIMESTAMP,
  {column_prefix}__record_valid_to::TIMESTAMP,
  {column_prefix}__is_current_record::BOOL
FROM hooks
WHERE 1 = 1
AND {column_prefix}__record_updated_at BETWEEN @start_ts AND @end_ts""")
    
    return True