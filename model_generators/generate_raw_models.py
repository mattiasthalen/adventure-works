import os
from parse_yaml import load_schema, get_filtered_tables, ensure_directory_exists, map_data_type

def generate_raw_models():
    """Generate raw model SQL files for all tables in the schema"""
    # Define output directory
    output_dir = './models/bronze/'
    
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

def generate_sql_for_table(table_name, table_info):
    """Generate SQL VIEW model for a table"""
    # Model declaration
    sql = "MODEL (\n  kind VIEW,\n  enabled TRUE\n);\n\n"
    
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
    sql += f"\nFROM ICEBERG_SCAN(\n  \"file://\" || @project_path || \"/lakehouse/bronze/{table_name}\"\n)\n;"
    
    return sql

if __name__ == "__main__":
    generate_raw_models()