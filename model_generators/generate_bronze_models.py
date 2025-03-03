import os
import yaml
import glob

def generate_bronze_models():
    """Generate bronze layer SQL models that extract data from the lakehouse."""
    # Read the schema file
    with open('./pipelines/schemas/export/adventure_works.schema.yaml', 'r') as schema_file:
        schema = yaml.safe_load(schema_file)
    
    # Get tables from schema that start with raw__
    tables = [table_name for table_name in schema['tables'].keys() 
             if table_name.startswith("raw__")]
    
    print(f"Found {len(tables)} tables in schema")
    
    # Create SQL files for each table
    models_created = 0
    for table in tables:
        # Use the table name directly as it already has the raw__ prefix
        model_name = table
        file_path = f"./models/bronze/{model_name}.sql"
        
        # Get columns from schema
        table_schema = schema['tables'][table]
        columns = table_schema.get('columns', {}).keys()
        
        # Sort columns alphabetically but put primary key first
        primary_key = next((col_name for col_name, col_info in table_schema.get('columns', {}).items() 
                       if col_info.get('primary_key')), None)
        
        if primary_key and primary_key in columns:
            sorted_columns = [primary_key] + sorted([col for col in columns if col != primary_key and not col.startswith('_dlt')])
        else:
            sorted_columns = sorted([col for col in columns if not col.startswith('_dlt')])
        
        # Add _dlt_load_id as the last column
        formatted_columns = ',\n  '.join(sorted_columns + ['_dlt_load_id'])
        
        # Generate the SQL body
        body = f"""MODEL (
  kind VIEW,
  enabled TRUE
);

SELECT
  {formatted_columns}
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/{model_name}"
)
"""
        
        # Ensure the models/bronze directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'w') as file:
            file.write(body)
        
        models_created += 1
    
    print(f"Generated {models_created} bronze models in ./models/bronze/")

def check_existing_models():
    """Check for existing models in the bronze directory to avoid overwriting."""
    bronze_models = glob.glob("./models/bronze/*.sql")
    if bronze_models:
        print(f"Warning: Found {len(bronze_models)} existing models in ./models/bronze/")
        print("These will be overwritten if you continue.")
        response = input("Do you want to continue? (y/n): ")
        return response.lower() == 'y'
    return True

if __name__ == "__main__":
    if check_existing_models():
        generate_bronze_models()
    else:
        print("Operation cancelled by user.")