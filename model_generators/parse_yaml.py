import os
import yaml

def load_schema(schema_path='./pipelines/schemas/export/adventure_works.schema.yaml'):
    """Load and return the schema from YAML file"""
    with open(schema_path, 'r') as file:
        return yaml.safe_load(file)

def load_bags_config(bags_path='./hook/hook__bags.yml'):
    """Load and return the bags configuration from YAML file"""
    with open(bags_path, 'r') as file:
        return yaml.safe_load(file)

def get_filtered_tables(schema):
    """Return tables that are not DLT metadata tables"""
    return {table_name: table_info for table_name, table_info in schema['tables'].items() 
            if not table_name.startswith('_dlt')}

def ensure_directory_exists(directory_path):
    """Ensure the specified directory exists"""
    os.makedirs(directory_path, exist_ok=True)

def map_data_type(data_type, column_name=None):
    """Map YAML data type to SQL data type"""
    if not data_type:
        return "TEXT"
        
    data_type = data_type.lower()
    
    # Special handling for specific fields
    if column_name == 'rowguid':
        return 'UUID'
    elif column_name == 'modified_date':
        return 'DATE'
    
    # Standard type mapping
    if data_type == 'bigint':
        return 'BIGINT'
    elif data_type == 'double':
        return 'DOUBLE'
    elif data_type == 'bool':
        return 'BOOLEAN'
    elif data_type == 'text':
        return 'TEXT'
    elif data_type == 'timestamp':
        return 'TIMESTAMP'
    else:
        return 'TEXT'