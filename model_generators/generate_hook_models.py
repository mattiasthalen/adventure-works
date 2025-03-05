import os
import yaml

def generate_hook_sql_files():
    # Define paths
    bags_path = './hook/hook__bags.yml'
    schema_path = './pipelines/schemas/export/adventure_works.schema.yaml'
    output_dir = './models/silver/'
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Load YAML files
    with open(bags_path, 'r') as file:
        bags_config = yaml.safe_load(file)
    
    with open(schema_path, 'r') as file:
        schema = yaml.safe_load(file)
    
    # Process each bag
    for bag in bags_config['bags']:
        bag_name = bag['name']
        source_table = bag['source_table']
        column_prefix = bag['column_prefix']
        hooks = bag['hooks']
        
        # Skip _dlt tables
        if source_table.startswith('_dlt'):
            continue
        
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
            continue
        
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
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column {column_prefix}__record_updated_at
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
            sql_file.write(f"  FROM bronze.{source_table}\n")
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
                data_type = filtered_columns[col].get('data_type', 'text').lower()
                
                # Map data type to SQL type
                if data_type == 'bigint':
                    cast_type = "BIGINT"
                elif data_type == 'double':
                    cast_type = "DOUBLE"
                elif data_type == 'bool':
                    cast_type = "BOOLEAN"
                elif data_type == 'text':
                    cast_type = "TEXT"
                elif data_type == 'timestamp':
                    cast_type = "TIMESTAMP"
                elif data_type == 'date':
                    cast_type = "DATE"
                else:
                    cast_type = "TEXT"
                
                sql_file.write(f"  {column_prefix}__{col}::{cast_type},\n")
            
            # Add system columns
            sql_file.write(f"""  {column_prefix}__modified_date::DATE,
  {column_prefix}__record_loaded_at::TIMESTAMP,
  {column_prefix}__record_updated_at::TIMESTAMP,
  {column_prefix}__record_version::TEXT,
  {column_prefix}__record_valid_from::TIMESTAMP,
  {column_prefix}__record_valid_to::TIMESTAMP,
  {column_prefix}__is_current_record::TEXT
FROM hooks
WHERE 1 = 1
AND {column_prefix}__record_updated_at BETWEEN @start_ts AND @end_ts""")
    
    print(f"Generated SQL files for Adventure Works bags in {output_dir}")

if __name__ == "__main__":
    generate_hook_sql_files()