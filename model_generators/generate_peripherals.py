import os
from parse_yaml import load_bags_config, load_schema, ensure_directory_exists, format_peripheral_description

def generate_peripherals(output_dir, hook_schema):
    """Generate peripheral views in the gold layer based on silver bags"""
    ensure_directory_exists(output_dir)
    
    bags_config = load_bags_config()
    schema = load_schema()
    count = 0
    
    for bag in bags_config['bags']:
        success = generate_peripheral_for_bag(bag, output_dir, hook_schema, schema)
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

def generate_peripheral_for_bag(bag, output_dir, hook_schema, schema):
    """Generate a peripheral view for a specific bag"""
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
    
    # Build exclusion list
    exclude_columns = []
    for hook in bag['hooks']:
        exclude_columns.append(hook['name'])
    
    # Get descriptions
    peripheral_description = get_peripheral_description(bag_name, schema)
    column_descriptions = get_peripheral_column_descriptions(bag, schema)
    
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
        
        # Write SELECT statement
        file.write("SELECT\n")
        file.write("  *\n")
        file.write(f"  EXCLUDE ({', '.join(exclude_columns)})\n")
        file.write(f"FROM {hook_schema}.{bag_name}")
    
    return True