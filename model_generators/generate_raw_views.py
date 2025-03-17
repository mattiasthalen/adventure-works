import os
from parse_yaml import load_schema, get_filtered_tables, ensure_directory_exists, map_data_type, format_raw_description

def generate_raw_views(output_dir):
    """Generate raw model SQL files for all tables in the schema"""
    # Ensure output directory exists
    ensure_directory_exists(output_dir)
    
    # Load schema
    schema = load_schema()
    
    # Get tables (excluding DLT tables)
    tables = get_filtered_tables(schema)
    
    # Counter for generated models
    count = 0
    
    # Process each table
    for table_name, table_info in tables.items():
        # Generate SQL file path
        sql_file_path = os.path.join(output_dir, f"{table_name}.sql")
        
        # Generate SQL content
        sql_content = generate_sql_for_table(table_name, table_info)
        
        # Write to file
        with open(sql_file_path, 'w') as file:
            file.write(sql_content)
        
        count += 1
    
    print(f"Generated {count} raw models in {output_dir}")
    return count

def generate_sql_for_table(table_name, table_info):
    """Generate SQL VIEW model for a table"""
    # Get table description
    original_description = table_info.get('description', '')
    entity_name = table_name.replace('raw__adventure_works__', '')
    
    # Format the description
    table_description = format_raw_description(entity_name, original_description)
    
    # Model declaration with description
    sql = f"""MODEL (
  kind VIEW,
  enabled TRUE,
  description '{table_description.replace("'", "''")}'"""
    
    # Add column_descriptions if any
    column_descriptions = {}
    for col_name, col_info in table_info['columns'].items():
        # Skip internal DLT columns
        if col_name.startswith('_dlt') and col_name != '_dlt_load_id':
            continue
        
        if 'description' in col_info:
            # Escape single quotes in description
            column_descriptions[col_name] = col_info['description'].replace("'", "''")
    
    if column_descriptions:
        sql += ",\n  column_descriptions (\n"
        for i, (col_name, desc) in enumerate(column_descriptions.items()):
            sql += f"    {col_name} = '{desc}'"
            if i < len(column_descriptions) - 1:
                sql += ","
            sql += "\n"
        sql += "  )"
    
    sql += "\n);\n\n"
    
    # Begin SELECT statement
    sql += "SELECT\n"
    
    # Process columns
    column_statements = []
    for col_name, col_info in table_info['columns'].items():
        # Skip internal DLT columns except _dlt_load_id which we need
        if col_name.startswith('_dlt') and col_name != '_dlt_load_id':
            continue
        
        # Determine appropriate data type for casting
        data_type = col_info.get('data_type', 'text')
        sql_type = map_data_type(data_type, col_name)
        
        # Add column with type casting
        column_statements.append(f"    {col_name}::{sql_type}")
    
    # Add columns to SQL
    sql += ",\n".join(column_statements)
    
    # Add FROM clause with Iceberg scan
    sql += f"\nFROM ICEBERG_SCAN(\n  \"file://\" || @project_path || \"/lakehouse/das/{table_name}\"\n)\n;"
    
    return sql