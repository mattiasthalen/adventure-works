import os
from parse_yaml import load_schema, load_bags_config, ensure_directory_exists, map_data_type

def generate_hook_bags(output_dir, raw_schema):
    """Generate hook model SQL files based on bags configuration"""
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
    """Generate a hook model SQL file for a specific bag"""
    bag_name = bag['name']
    source_table = bag['source_table']
    column_prefix = bag['column_prefix']
    hooks = bag['hooks']
    
    # Skip _dlt tables
    if source_table.startswith('_dlt'):
        return False
    
    # Get primary hook
    primary_hook = next((h for h in hooks if h.get('primary')), hooks[0])
    primary_hook_name = primary_hook['name']
    primary_key_field = primary_hook['business_key_field']
    
    # Get source table schema
    table_columns = {}
    if source_table in schema['tables']:
        table_columns = schema['tables'][source_table]['columns']
    else:
        print(f"Warning: Table {source_table} not found in schema")
        return False
    
    # Filter columns
    filtered_columns = {col: props for col, props in table_columns.items() 
                       if not col.startswith('_dlt_') and col != 'modified_date'}
    
    # Get reference hooks
    reference_hooks = [h['name'] for h in hooks if h != primary_hook]
    
    # Generate SQL file
    sql_path = os.path.join(output_dir, f"{bag_name}.sql")
    with open(sql_path, 'w') as sql_file:
        # MODEL declaration
        sql_file.write(f"""MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit{primary_hook_name},
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit{primary_hook_name}, {primary_hook_name})""")
        
        # Add references only if there are any
        if reference_hooks:
            sql_file.write(f",\n  references ({', '.join(reference_hooks)})")
        
        sql_file.write("\n);\n\n")
        
        # Staging CTE
        sql_file.write("WITH staging AS (\n  SELECT\n")
        for i, col in enumerate(filtered_columns.keys()):
            sql_file.write(f"    {col} AS {column_prefix}__{col},\n")
        
        sql_file.write(f"    modified_date AS {column_prefix}__modified_date,\n")
        sql_file.write(f"    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS {column_prefix}__record_loaded_at\n")
        sql_file.write(f"  FROM {raw_schema}.{source_table}\n")
        sql_file.write("), ")
        
        # Validity CTE
        sql_file.write(f"""validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY {column_prefix}__{primary_key_field} ORDER BY {column_prefix}__record_loaded_at) AS {column_prefix}__record_version,
    CASE
      WHEN {column_prefix}__record_version = 1
      THEN '1970-01-01 00:00:00'::TIMESTAMP
      ELSE {column_prefix}__record_loaded_at
    END AS {column_prefix}__record_valid_from,
    COALESCE(
      LEAD({column_prefix}__record_loaded_at) OVER (PARTITION BY {column_prefix}__{primary_key_field} ORDER BY {column_prefix}__record_loaded_at),
      '9999-12-31 23:59:59'::TIMESTAMP
    ) AS {column_prefix}__record_valid_to,
    {column_prefix}__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS {column_prefix}__is_current_record,
    CASE
      WHEN {column_prefix}__is_current_record
      THEN {column_prefix}__record_loaded_at
      ELSE {column_prefix}__record_valid_to
    END AS {column_prefix}__record_updated_at
  FROM staging
), """)
        
        # Hooks CTE
        sql_file.write("hooks AS (\n  SELECT\n")
        
        # Process hooks
        for i, hook in enumerate(hooks):
            hook_name = hook['name']
            keyset = hook['keyset']
            business_key_field = hook['business_key_field']
            
            # Generate PIT hook for primary
            if hook == primary_hook:
                sql_file.write(f"""    CONCAT(
      '{keyset}|',
      {column_prefix}__{business_key_field},
      '~epoch__valid_from|',
      {column_prefix}__record_valid_from
    )::BLOB AS _pit{hook_name},\n""")
            
            # Generate regular hook
            sql_file.write(f"    CONCAT('{keyset}|', {column_prefix}__{business_key_field}) AS {hook_name},\n")
        
        # Add columns from validity
        sql_file.write("    *\n  FROM validity\n)\n")
        
        # Final SELECT
        sql_file.write("SELECT\n")
        sql_file.write(f"  _pit{primary_hook_name}::BLOB,\n")
        
        # Add all hooks
        for hook in hooks:
            sql_file.write(f"  {hook['name']}::BLOB,\n")
        
        # Add data columns with types
        for col in filtered_columns.keys():
            data_type = filtered_columns[col].get('data_type', 'text')
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