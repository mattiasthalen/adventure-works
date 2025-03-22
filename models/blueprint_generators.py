import yaml

def load_yaml(path: str) -> dict:
    """Load a YAML file and return its contents as a dictionary."""
    with open(path, 'r') as file:
        return yaml.safe_load(file)

def map_data_type_to_sql(data_type: str) -> str:
    """Map data types to appropriate SQL types."""
    type_map = {
        "xml": "text",
        "uniqueidentifier": "text", 
        "binary": "binary",
        "timestamp": "timestamp",
        "date": "date",
        "bigint": "bigint",
        "int": "int",
        "double": "double",
        "bool": "boolean",
        "text": "text"
    }
    return type_map.get(data_type, "TEXT")

def generate_raw_blueprints(schema_path: str) -> list:
    """
    Generate a list of blueprint dictionaries from the schema YAML file
    for raw table models.
    """
    raw_schema = load_yaml(schema_path)
    
    blueprints = []
    
    for table_name, table_schema in raw_schema["tables"].items():
        description = table_schema.get("description", "")
        
        # Create column descriptions dictionary
        column_descriptions = {}
        columns = []
        
        for col_name, col_props in table_schema["columns"].items():
            data_type = col_props.get("data_type", "text")
            col_description = col_props.get("description", "")
            
            # Don't include some internal columns
            if col_name.startswith("_dlt_") and col_name != "_dlt_load_id":
                continue
                
            columns.append({
                "name": col_name,
                "type": data_type
            })
            
            column_descriptions[col_name] = col_description
        
        blueprint = {
            "table_name": table_name,
            "description": description,
            "columns": columns,
            "column_descriptions": column_descriptions
        }
        
        blueprints.append(blueprint)
    
    return blueprints

def generate_hook_blueprints(hook_config_path: str, schema_path: str) -> list:
    """
    Generate a list of blueprint dictionaries for hook models from 
    the hook configuration and schema YAML files.
    """
    bags_config = load_yaml(hook_config_path)
    raw_schema = load_yaml(schema_path)

    blueprints = []

    for bag in bags_config["bags"]:
        name = bag["name"]
        source_table = bag['source_table']
        column_prefix = bag['column_prefix']
        hooks = bag['hooks']
        schema = raw_schema["tables"][source_table]

        description = schema["description"]

        source_primary_keys = [
            col_name
            for col_name, col_properties
            in schema["columns"].items()
            if col_properties.get("primary_key", False)
        ]

        source_columns = [col for col in schema["columns"].keys() if not col.startswith("_dlt_")]

        # Generate list & dicts of columns
        columns = []
        prefixed_columns = {}
        column_data_types = {}
        column_descriptions = {}
        grain = None
        references = []

        for hook in hooks:
            if hook.get("primary", False):
                hook_name = hook["name"]
                pit_hook_name = f"_pit{hook_name}"

                grain = pit_hook_name
                
                columns.append(pit_hook_name)
                column_data_types[pit_hook_name] = "binary"
                column_descriptions[pit_hook_name] = f"Point in time version of {hook_name}."

        for hook in hooks:
            hook_name = hook["name"]
            hook_primary = hook.get("primary", False)
            hook_keyset = hook.get("keyset")
            hook_key_field = hook.get("business_key_field")
            hook_composite = hook.get("composite_key")

            if not hook_primary:
                references.append(hook_name)

            columns.append(hook_name)
            column_data_types[hook_name] = "binary"

            description = f"Hook for {hook_key_field} using keyset: {hook_keyset}."
            
            if hook_composite:
                description = f"Hook using: {', '.join(hook_composite)}."
            
            if hook_primary:
                description = description.replace("Hook", "Primary hook")

            column_descriptions[hook_name] = description

        prefix_column = lambda x: f"{column_prefix}__{x}"

        for col_name, col_properties in schema["columns"].items():
            if col_name.startswith("_dlt_"):
                continue

            prefixed = prefix_column(col_name)

            columns.append(prefixed)
            prefixed_columns[col_name] = prefixed
            column_data_types[prefixed] = col_properties["data_type"]
            column_descriptions[prefixed] = col_properties["description"]
        
        metacols_dict = [
            {"name": "record_loaded_at", "data_type": "timestamp", "description": "Timestamp when this record was loaded into the system"},
            {"name": "record_updated_at", "data_type": "timestamp", "description": "Timestamp when this record was last updated"},
            {"name": "record_version", "data_type": "int", "description": "Version number for this record"},
            {"name": "record_valid_from", "data_type": "timestamp", "description": "Timestamp from which this record version is valid"},
            {"name": "record_valid_to", "data_type": "timestamp", "description": "Timestamp until which this record version is valid"},
            {"name": "is_current_record", "data_type": "boolean", "description": "Flag indicating if this is the current valid version of the record"}
        ]

        for col in metacols_dict:
            prefixed = prefix_column(col["name"])
            columns.append(prefixed)
            
            column_data_types[prefixed] = col["data_type"]
            column_descriptions[prefixed] = col["description"]

        blueprint = {
            "name": name,
            "description": description,
            "grain": grain,
            "references": references,
            "source_table": source_table,
            "source_primary_keys": source_primary_keys,
            "source_columns": source_columns,
            "column_prefix": column_prefix,
            "hooks": hooks,
            "columns": columns,
            "column_data_types": column_data_types,
            "column_descriptions": column_descriptions,
        }

        blueprints.append(blueprint)

    return blueprints