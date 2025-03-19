import os
from parse_yaml import load_bags_config, load_schema, ensure_directory_exists, format_peripheral_description, map_data_type

def generate_peripherals(output_dir, hook_schema, target_schema):
    """Generate peripheral views in the gold layer based on silver bags"""
    ensure_directory_exists(output_dir)
    
    bags_config = load_bags_config()
    schema = load_schema()
    count = 0
    
    for bag in bags_config['bags']:
        success = generate_peripheral_for_bag(bag, output_dir, hook_schema, schema, target_schema)
        if success:
            count += 1
    
    print(f"Generated {count} peripheral views in {output_dir}")
    return count

def get_peripheral_description(bag_name, schema):
    """Generate a description for the peripheral based on the source table"""
    # Extract entity name from bag name
    parts = bag_name.split('__')
    if len(parts) < 3:
        return f"Peripheral view for {bag_name}"
    
    entity_name = parts[2]
    source_table = None
    
    # Get source table from bags_config
    bags_config = load_bags_config()
    for bag in bags_config['bags']:
        if bag['name'] == bag_name:
            source_table = bag['source_table']
            break
    
    if not source_table or source_table not in schema['tables']:
        return f"Business viewpoint of {entity_name} data"
    
    # Get description from schema
    table_info = schema['tables'][source_table]
    original_description = table_info.get('description', '')
    
    # Create a more specific peripheral description
    peripheral_description = format_peripheral_description(entity_name, original_description)
    
    return peripheral_description

def get_peripheral_column_descriptions(bag, schema):
    """Generate descriptions for columns in the peripheral view"""
    source_table = bag['source_table']
    column_prefix = bag['column_prefix']
    
    if source_table not in schema['tables']:
        return {}
    
    # Get column descriptions from source table
    table_columns = schema['tables'][source_table]['columns']
    
    # Build peripheral column descriptions
    descriptions = {}
    for col_name, col_info in table_columns.items():
        # Skip internal and excluded columns
        if col_name.startswith('_dlt_') or col_name == 'modified_date':
            continue
        
        if 'description' in col_info:
            prefixed_col = f"{column_prefix}__{col_name}"
            descriptions[prefixed_col] = col_info['description'].replace("'", "''")
    
    # Add descriptions for system columns
    descriptions[f"{column_prefix}__modified_date"] = "Date when this record was last modified"
    descriptions[f"{column_prefix}__record_loaded_at"] = "Timestamp when this record was loaded into the system"
    descriptions[f"{column_prefix}__record_updated_at"] = "Timestamp when this record was last updated"
    descriptions[f"{column_prefix}__record_version"] = "Version number for this record"
    descriptions[f"{column_prefix}__record_valid_from"] = "Timestamp from which this record version is valid"
    descriptions[f"{column_prefix}__record_valid_to"] = "Timestamp until which this record version is valid"
    descriptions[f"{column_prefix}__is_current_record"] = "Flag indicating if this is the current valid version of the record"
    
    return descriptions

def get_column_list_and_types(bag, schema):
    """Get a list of columns and their types from the source table"""
    source_table = bag['source_table']
    column_prefix = bag['column_prefix']
    
    if source_table not in schema['tables']:
        return [], {}
    
    # Get columns from source table
    table_columns = schema['tables'][source_table]['columns']
    
    column_list = []
    column_types = {}
    
    # Process each column
    for col_name, col_info in table_columns.items():
        # Skip internal columns
        if col_name.startswith('_dlt_') or col_name == 'modified_date':
            continue
        
        prefixed_col = f"{column_prefix}__{col_name}"
        column_list.append(prefixed_col)
        
        # Determine SQL type
        data_type = col_info.get('data_type', 'text')
        sql_type = map_data_type(data_type, col_name)
        column_types[prefixed_col] = sql_type
    
    # Add system columns
    system_columns = [
        f"{column_prefix}__modified_date",
        f"{column_prefix}__record_loaded_at",
        f"{column_prefix}__record_updated_at",
        f"{column_prefix}__record_version",
        f"{column_prefix}__record_valid_from",
        f"{column_prefix}__record_valid_to",
        f"{column_prefix}__is_current_record"
    ]
    
    # Add types for system columns
    system_types = {
        f"{column_prefix}__modified_date": "DATE",
        f"{column_prefix}__record_loaded_at": "TIMESTAMP",
        f"{column_prefix}__record_updated_at": "TIMESTAMP",
        f"{column_prefix}__record_version": "TEXT",
        f"{column_prefix}__record_valid_from": "TIMESTAMP",
        f"{column_prefix}__record_valid_to": "TIMESTAMP",
        f"{column_prefix}__is_current_record": "BOOLEAN"
    }
    
    column_list.extend(system_columns)
    column_types.update(system_types)
    
    return column_list, column_types

def generate_ghost_value(field_name, sql_type):
    """Generate appropriate ghost value based on SQL type"""
    sql_type = sql_type.upper()
    
    if sql_type in ['TEXT', 'STRING', 'VARCHAR']:
        return "'N/A'"
    elif sql_type in ['BIGINT', 'INT', 'INTEGER', 'DOUBLE', 'FLOAT', 'DECIMAL']:
        return "NULL"
    elif sql_type == 'BOOLEAN':
        return "FALSE"
    elif sql_type in ['DATE', 'TIMESTAMP', 'TIME']:
        return "NULL"
    elif sql_type == 'BINARY':
        return "NULL"
    else:
        return "NULL"

def generate_peripheral_for_bag(bag, output_dir, hook_schema, schema, target_schema):
    """Generate a peripheral view for a specific bag with explicit CTEs"""
    bag_name = bag['name']
    
    # Skip _dlt tables
    if bag_name.startswith('_dlt'):
        return False
    
    # Extract peripheral name from bag name
    parts = bag_name.split('__')
    if len(parts) < 3:
        return False
    
    peripheral_name = parts[2]
    sql_path = os.path.join(output_dir, f"{peripheral_name}.sql")
    
    # Find primary hook
    primary_hook = next((h for h in bag['hooks'] if h.get('primary')), bag['hooks'][0])
    primary_pit_hook = f"_pit{primary_hook['name']}"
    
    # Get column information
    column_list, column_types = get_column_list_and_types(bag, schema)
    
    # Get descriptions
    peripheral_description = get_peripheral_description(bag_name, schema)
    column_descriptions = get_peripheral_column_descriptions(bag, schema)
    column_prefix = bag['column_prefix']
    
    with open(sql_path, 'w') as file:
        # Write MODEL declaration with description
        file.write(f"""MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain ({primary_pit_hook}),
  description '{peripheral_description.replace("'", "''")}'""")
        
        # Add column descriptions if any
        if column_descriptions:
            file.write(",\n  column_descriptions (\n")
            col_desc_items = list(column_descriptions.items())
            for i, (col_name, desc) in enumerate(col_desc_items):
                file.write(f"    {col_name} = '{desc}'")
                if i < len(col_desc_items) - 1:
                    file.write(",")
                file.write("\n")
            file.write("  )")
        
        file.write("\n);\n\n")
        
        # Write the CTE for source data
        file.write("WITH cte__source AS (\n  SELECT\n")
        file.write(f"    {primary_pit_hook},\n")
        
        # Add all columns from the source table
        for i, col in enumerate(column_list):
            file.write(f"    {col}")
            if i < len(column_list) - 1:
                file.write(",\n")
            else:
                file.write("\n")
                
        file.write(f"  FROM {hook_schema}.{bag_name}\n),\n\n")
        
        # Write the CTE for ghost record
        file.write("cte__ghost_record AS (\n  SELECT\n")
        file.write(f"    'ghost_record' AS {primary_pit_hook},\n")
        
        # Add ghost values for each column
        for i, col in enumerate(column_list):
            if col == f"{column_prefix}__record_loaded_at":
                file.write(f"    TIMESTAMP '1970-01-01 00:00:00' AS {col}")
            elif col == f"{column_prefix}__record_updated_at":
                file.write(f"    TIMESTAMP '1970-01-01 00:00:00' AS {col}")
            elif col == f"{column_prefix}__record_version":
                file.write(f"    0 AS {col}")
            elif col == f"{column_prefix}__record_valid_from":
                file.write(f"    TIMESTAMP '1970-01-01 00:00:00' AS {col}")
            elif col == f"{column_prefix}__record_valid_to":
                file.write(f"    TIMESTAMP '9999-12-31 23:59:59' AS {col}")
            elif col == f"{column_prefix}__is_current_record":
                file.write(f"    TRUE AS {col}")
            else:
                ghost_value = generate_ghost_value(col, column_types.get(col, "TEXT"))
                file.write(f"    {ghost_value} AS {col}")
                
            if i < len(column_list) - 1:
                file.write(",\n")
            else:
                file.write("\n")
                
        file.write("  FROM (SELECT 1) dummy\n),\n\n")
        
        # Write the CTE that unions source and ghost
        file.write("cte__final AS (\n")
        file.write("  SELECT * FROM cte__source\n")
        file.write("  UNION ALL\n")
        file.write("  SELECT * FROM cte__ghost_record\n")
        file.write(")\n\n")
        
        # Write the final SELECT with explicit casting
        file.write("SELECT\n")
        file.write(f"  {primary_pit_hook}::BLOB,\n")
        
        # Add all columns with appropriate casting
        for i, col in enumerate(column_list):
            sql_type = column_types.get(col, "TEXT")
            file.write(f"  {col}::{sql_type}")
            if i < len(column_list) - 1:
                file.write(",\n")
            else:
                file.write("\n")
                
        file.write("FROM cte__final\n")
        file.write(";\n")
        file.write("\n")
        file.write("@IF(\n")
        file.write("  @runtime_stage = 'evaluating',\n")
        file.write(f"  COPY {target_schema}.{peripheral_name} TO './export/{target_schema}/{peripheral_name}.parquet' (FORMAT parquet, COMPRESSION zstd)\n")
        file.write(");")
    
    return True