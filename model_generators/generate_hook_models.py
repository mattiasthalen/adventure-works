import os
import yaml


def get_sql_type(col_info):
    """Get SQL data type from schema column info."""
    data_type = col_info.get('type', {}).get('name', 'VARCHAR')
    if data_type == 'string':
        return 'VARCHAR'
    elif data_type == 'integer':
        return 'INT'
    elif data_type == 'number':
        return 'DOUBLE'
    elif data_type == 'boolean':
        return 'BOOLEAN'
    elif data_type == 'timestamp':
        return 'TIMESTAMP'
    elif data_type == 'date':
        return 'DATE'
    else:
        return 'VARCHAR'


def get_hook_columns(columns):
    """Find columns that end with '_id' to generate hooks."""
    return [col_name for col_name in columns.keys() if col_name.endswith('_id')]


def sort_columns(columns, primary_key):
    """Sort columns into primary key, foreign keys, and other columns."""
    foreign_keys = [col for col in columns.keys() if col.endswith('_id') and col != primary_key]
    other_columns = [col for col in columns.keys() if col != primary_key and col not in foreign_keys]
    
    return [primary_key] + sorted(foreign_keys) + sorted(other_columns)


def load_schema():
    """Load the schema file and extract raw tables."""
    with open('./pipelines/schemas/export/adventure_works.schema.yaml', 'r') as schema_file:
        schema = yaml.safe_load(schema_file)
    
    # Get tables from schema
    tables = [table_name for table_name in schema['tables'].keys() 
             if table_name.startswith("raw__")]
    
    return schema, tables


def extract_table_metadata(schema, table):
    """Extract metadata for a table from the schema."""
    table_schema = schema['tables'][table]
    columns = {col_name: col_info 
              for col_name, col_info in table_schema.get('columns', {}).items()
              if not col_name.startswith('_dlt')}
    
    # Get single primary key with PK_NOT_FOUND as default
    primary_key = next((col_name for col_name, col_info in columns.items() 
                      if col_info.get('primary_key')), 'PK_NOT_FOUND')
    
    # Get entity name from primary key by removing '_id'
    entity_name = primary_key[:-3] if primary_key.endswith('_id') else primary_key
    
    return columns, primary_key, entity_name


def generate_hook_statements(entity_name, primary_key, hook_columns):
    """Generate hook statements for a table."""
    hook_statements = []
    
    # Add PIT hook for primary key
    pit_hook = f"""CONCAT(
      '{entity_name}|adventure_works|',
      {entity_name}__{primary_key},
      '~epoch|valid_from|',
      {entity_name}__record_valid_from
    ) AS _pit_hook__{entity_name}"""
    hook_statements.append(pit_hook)
            
    # Add regular hook for primary key
    primary_hook = f"CONCAT('{entity_name}|adventure_works|', {entity_name}__{primary_key}) AS _hook__{entity_name}"
    hook_statements.append(primary_hook)
    
    # Add hooks for foreign keys
    for col in sorted(hook_columns):
        if col != primary_key:  # Skip primary key as it's already handled
            referenced_entity = col[:-3]  # Remove '_id' suffix
            hook = f"CONCAT('{referenced_entity}|adventure_works|', {entity_name}__{col}) AS _hook__{referenced_entity}"
            hook_statements.append(hook)
    
    return hook_statements


def generate_final_column_list(hook_statements, sorted_columns, columns, entity_name):
    """Generate the final column list with proper type casting."""
    final_columns = []
    
    # Add hook columns first with proper BLOB casting
    for hook_stmt in hook_statements:
        # Extract the alias (everything after "AS ")
        alias = hook_stmt.strip().split(" AS ")[-1]
        hook_name = alias.strip()
        final_columns.append(f"{hook_name}::BLOB")
        
    # Add entity columns with proper casting
    for column in sorted_columns:
        column_info = columns.get(column, {})
        data_type = get_sql_type(column_info)
        final_columns.append(f"{entity_name}__{column}::{data_type}")
        
    # Add auditing columns
    audit_columns = [
        f"{entity_name}__record_loaded_at::TIMESTAMP",
        f"{entity_name}__record_version::INT",
        f"{entity_name}__record_valid_from::TIMESTAMP",
        f"{entity_name}__record_valid_to::TIMESTAMP",
        f"{entity_name}__is_current_record::BOOLEAN",
        f"{entity_name}__record_updated_at::TIMESTAMP"
    ]
    
    final_columns.extend(audit_columns)
    return final_columns


def generate_sql_model(table, entity_name, primary_key, sorted_columns, hook_definitions, formatted_columns, formatted_final_columns):
    """Generate the SQL model content."""
    return f"""MODEL (
  kind VIEW,
  enabled TRUE
);

WITH staging AS (
  SELECT
    {formatted_columns},
    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS {entity_name}__record_loaded_at
  FROM bronze.{table}
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY {entity_name}__{primary_key} ORDER BY {entity_name}__record_loaded_at) AS {entity_name}__record_version,
    CASE
      WHEN {entity_name}__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE {entity_name}__record_loaded_at
    END AS {entity_name}__record_valid_from,
    COALESCE(
      LEAD({entity_name}__record_loaded_at) OVER (PARTITION BY {entity_name}__{primary_key} ORDER BY {entity_name}__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS {entity_name}__record_valid_to,
    {entity_name}__record_valid_to = @max_ts::TIMESTAMP AS {entity_name}__is_current_record,
    CASE
      WHEN {entity_name}__is_current_record
      THEN {entity_name}__record_loaded_at
      ELSE {entity_name}__record_valid_to
    END AS {entity_name}__record_updated_at
  FROM staging
), hooks AS (
  SELECT
    {hook_definitions},
    *
  FROM validity
)
SELECT
  {formatted_final_columns}
FROM hooks"""


def write_model_file(file_path, content):
    """Write model content to a file."""
    # Ensure the directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    with open(file_path, 'w') as file:
        file.write(content)


def process_table(schema, table):
    """Process a single table and generate its model file."""
    model_name = table.replace("raw__", "bag__")
    file_path = f"./models/silver/{model_name}.sql"
    
    # Extract table metadata
    columns, primary_key, entity_name = extract_table_metadata(schema, table)
    
    # Generate hooks
    hook_columns = get_hook_columns(columns)
    hook_statements = generate_hook_statements(entity_name, primary_key, hook_columns)
    hook_definitions = ',\n    '.join(hook_statements)
    
    # Sort and format columns
    sorted_columns = sort_columns(columns, primary_key)
    prefixed_columns = [f"{column} AS {entity_name}__{column}" for column in sorted_columns]
    formatted_columns = ',\n    '.join(prefixed_columns)
    
    # Generate final column list
    final_columns = generate_final_column_list(hook_statements, sorted_columns, columns, entity_name)
    formatted_final_columns = ',\n  '.join(final_columns)
    
    # Generate the SQL model
    model_content = generate_sql_model(
        table, entity_name, primary_key, sorted_columns, hook_definitions, 
        formatted_columns, formatted_final_columns
    )
    
    # Write the model file
    write_model_file(file_path, model_content)
    
    return model_name


def generate_hook_models():
    """Main function to generate hook models for all tables."""
    schema, tables = load_schema()
    
    print(f"Found {len(tables)} tables:", tables)
    
    generated_models = []
    for table in tables:
        model_name = process_table(schema, table)
        generated_models.append(model_name)
    
    print(f"Generated {len(generated_models)} hook models in ./models/silver/")
    return generated_models
    

if __name__ == "__main__":
    generate_hook_models()