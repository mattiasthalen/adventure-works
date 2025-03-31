from typing import Dict, List, Tuple, Any, Optional
import networkx as nx
import os
import yaml

def load_yaml(path: str) -> Dict[str, Any]:
    """Load a YAML file and return its contents as a dictionary.
    
    Args:
        path: Path to the YAML file to load
        
    Returns:
        Dictionary containing the loaded YAML content
    """
    with open(path, 'r') as file:
        return yaml.safe_load(file)

def export_blueprints(blueprints: List[Dict[str, Any]], output_path: str, name_field: str = "name") -> None:
    """Export blueprints to individual YAML files in the specified directory.
    
    Args:
        blueprints: List of blueprint dictionaries to export
        output_path: Directory path where blueprints will be saved
        name_field: Key in blueprint dictionary to use as filename (default: "name")
    """
    os.makedirs(output_path, exist_ok=True)
    
    for blueprint in blueprints:
        name = blueprint[name_field]
        with open(f"{output_path}/{name}.yml", "w") as f:
            yaml.dump(blueprint, f)

def import_blueprints(directory_path: str) -> List[Dict[str, Any]]:
    """Import all blueprint YAML files from a directory.
    This is the inverse operation of export_blueprints.
    
    Args:
        directory_path: Path to directory containing blueprint YAML files
        
    Returns:
        List of blueprint dictionaries loaded from YAML files
    """
    blueprints = []
    
    # Check if the directory exists
    if not os.path.isdir(directory_path):
        print(f"Warning: Directory {directory_path} does not exist.")
        return blueprints
        
    # List all files in the directory
    for filename in os.listdir(directory_path):
        # Check if the file is a YAML file
        if filename.endswith(('.yml', '.yaml')):
            file_path = os.path.join(directory_path, filename)
            try:
                blueprint = load_yaml(file_path)
                blueprints.append(blueprint)
            except Exception as e:
                print(f"Error loading {file_path}: {e}")
                
    return blueprints

def map_data_type_to_sql(data_type: str) -> str:
    """Map source data types to appropriate SQL data types.
    
    Args:
        data_type: Source data type name
        
    Returns:
        Mapped SQL data type string, defaults to "TEXT" if type not found
    """
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

def process_raw_table_schema(name: str, schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a raw table schema into column definitions and descriptions.
    
    Args:
        name: Name of the table
        schema: Schema definition for the table
        
    Returns:
        Dictionary containing the blueprint for this table
    """
    description = schema.get("description", "")
    
    # Create column descriptions dictionary
    column_descriptions = {}
    columns = []
    
    for col_name, col_props in schema["columns"].items():
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
    
    return {
        "name": name,
        "description": description,
        "columns": columns,
        "column_descriptions": column_descriptions
    }

def generate_raw_blueprints(schema_path: str) -> List[Dict[str, Any]]:
    """
    Generate a list of blueprint dictionaries from the schema YAML file
    for raw table models.
    
    Args:
        schema_path: Path to the raw schema YAML file
        
    Returns:
        List of raw blueprint dictionaries
    """
    raw_schema = load_yaml(schema_path)
    
    blueprints = []
    
    for name, schema in raw_schema["tables"].items():
        blueprint = process_raw_table_schema(name, schema)
        blueprints.append(blueprint)
    
    export_blueprints(blueprints, "./models/blueprints/raw")
    
    return blueprints

def extract_primary_keys(schema: Dict[str, Any]) -> List[str]:
    """
    Extract primary keys from a table schema.
    
    Args:
        schema: Table schema dictionary
        
    Returns:
        List of primary key column names
    """
    return [
        col_name
        for col_name, col_properties in schema["columns"].items()
        if col_properties.get("primary_key", False)
    ]

def extract_source_columns(schema: Dict[str, Any]) -> List[str]:
    """
    Extract non-DLT columns from a table schema.
    
    Args:
        schema: Table schema dictionary
        
    Returns:
        List of source column names
    """
    return [col for col in schema["columns"].keys() if not col.startswith("_dlt_")]

def generate_hook_columns_and_metadata(hooks: List[Dict[str, Any]], column_prefix: str, schema: Dict[str, Any]) -> Tuple[List[str], List[str], Dict[str, str], Dict[str, str], List[str], List[str]]:
    """
    Generate columns and related metadata for hook blueprints.
    
    Args:
        hooks: List of hook configurations
        column_prefix: Prefix for columns
        schema: Source table schema
        
    Returns:
        Tuple of (columns, prefixed_columns, column_data_types, column_descriptions, grain, references)
    """
    columns = []
    prefixed_columns = {}
    column_data_types = {}
    column_descriptions = {}
    grain = None
    references = []

    # Process primary hooks first
    for hook in hooks:
        if hook.get("primary", False):
            hook_name = hook["name"]
            pit_hook_name = f"_pit{hook_name}"

            grain = pit_hook_name
            
            columns.append(pit_hook_name)
            column_data_types[pit_hook_name] = "binary"
            column_descriptions[pit_hook_name] = f"Point in time version of {hook_name}."

    # Process all hooks and create descriptions
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

    # Process source columns with prefix
    prefix_column = lambda x: f"{column_prefix}__{x}"

    for col_name, col_properties in schema["columns"].items():
        if col_name.startswith("_dlt_"):
            continue

        prefixed = prefix_column(col_name)

        columns.append(prefixed)
        prefixed_columns[col_name] = prefixed
        column_data_types[prefixed] = col_properties["data_type"]
        column_descriptions[prefixed] = col_properties["description"]
    
    # Add metadata columns
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
        
    return columns, prefixed_columns, column_data_types, column_descriptions, grain, references

def create_hook_blueprint(name: str, description: str, grain: List[str], references: List[str], 
                          source_table: str, source_primary_keys: List[str], 
                          source_columns: List[str], column_prefix: str, hooks: List[Dict[str, Any]], 
                          columns: List[str], column_data_types: Dict[str, str], 
                          column_descriptions: Dict[str, str]) -> Dict[str, Any]:
    """
    Create a hook blueprint dictionary.
    
    Args:
        name: Blueprint name
        description: Blueprint description
        grain: Blueprint grain
        references: Blueprint references
        source_table: Source table name
        source_primary_keys: Source primary keys
        source_columns: Source columns
        column_prefix: Column prefix
        hooks: Hooks configuration
        columns: Columns list
        column_data_types: Column data types
        column_descriptions: Column descriptions
        
    Returns:
        Dictionary representing the hook blueprint
    """
    return {
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

def generate_hook_blueprints(hook_config_path: str, schema_path: str) -> list:
    """
    Generate a list of blueprint dictionaries for hook models from 
    the hook configuration and schema YAML files.
    """
    frames_config = load_yaml(hook_config_path)
    raw_schema = load_yaml(schema_path)

    blueprints = []

    for frame in frames_config["frames"]:
        name = frame["name"]
        source_table = frame['source_table']
        column_prefix = frame['column_prefix']
        hooks = frame['hooks']
        schema = raw_schema["tables"][source_table]

        description = schema["description"]

        # Extract source primary keys and columns
        source_primary_keys = extract_primary_keys(schema)
        source_columns = extract_source_columns(schema)

        # Generate columns and metadata
        columns, prefixed_columns, column_data_types, column_descriptions, grain, references = \
            generate_hook_columns_and_metadata(hooks, column_prefix, schema)

        # Create blueprint dictionary
        blueprint = create_hook_blueprint(
            name=name,
            description=description,
            grain=grain,
            references=references,
            source_table=source_table,
            source_primary_keys=source_primary_keys,
            source_columns=source_columns,
            column_prefix=column_prefix,
            hooks=hooks,
            columns=columns,
            column_data_types=column_data_types,
            column_descriptions=column_descriptions
        )

        blueprints.append(blueprint)

    export_blueprints(blueprints, "./models/blueprints/hook")

    return blueprints

def build_directed_graph(frames: Dict[str, Dict[str, Any]]) -> Tuple[nx.DiGraph, Dict[str, str]]:
    """
    Create a directed acyclic graph (DAG) where each node is a frame and edges represent dependencies.
    
    Args:
        frames: List of frame configurations
        
    Returns:
        directed_acyclical_graph: NetworkX DiGraph
        primary_hooks: Dictionary mapping frame names to their primary hooks
    """
    # We need to generate a DAG so that we can have a cascading inheritance of hooks
    directed_acyclical_graph = nx.DiGraph()

    # We also need to keep track of the primary hook in each frame
    primary_hooks = {}

    # First we need to register each frame as a node in the DAG and capture the primary hook
    for frame in frames:
        frame_name = frame["name"]
        directed_acyclical_graph.add_node(frame_name)
        primary_hook = next((hook["name"] for hook in frame["hooks"] if hook.get("primary", False)))
        primary_hooks[frame_name] = primary_hook
    
    # Then we need to register the hooks as edges - representing dependencies between frames
    # The direction should be: A frame with a foreign hook depends on a frame where that hook is primary
    for frame in frames:
        from_frame = frame["name"]  # The frame containing the reference (depends on others)
        
        for hook in frame["hooks"]:
            # Skip the primary hook of this frame
            if hook.get("primary", False):
                continue
            
            foreign_hook = hook["name"]
            
            # Find the frame where this foreign hook is defined as a primary hook
            to_frame = None
            for target_frame in frames:
                primary_hook = next((h["name"] for h in target_frame["hooks"] if h.get("primary", False)), None)
                if primary_hook == foreign_hook:
                    to_frame = target_frame["name"]
                    break
            
            # Skip if we couldn't find a frame with this primary hook
            if not to_frame:
                continue
                
            # Add an edge: from_frame -> to_frame means from_frame DEPENDS ON to_frame
            directed_acyclical_graph.add_edge(
                u_of_edge=from_frame,      # This frame depends on...
                v_of_edge=to_frame,        # ...this frame
                name=foreign_hook
            )
    
    return directed_acyclical_graph, primary_hooks

def process_node_dependencies(node: str, graph_dict: Dict[str, Any], directed_acyclical_graph: nx.DiGraph) -> Dict[str, Dict[str, Any]]:
    """
    Process dependencies for a node, creating a structured mapping with primary and inherited hooks.
    
    Args:
        node: The node to process
        graph_dict: Dictionary containing graph structure
        directed_acyclical_graph: NetworkX DiGraph
        
    Returns:
        dependencies: Dictionary of structured dependencies
    """
    dependencies = {}
    
    for direct_upstream, direct_hook in graph_dict[node]["direct_upstream_nodes"]:
        # Create the structure for this dependency
        dependency_structure = {
            "primary_hook": direct_hook,
            "inherited_hooks": []
        }
        
        # Collect all hooks in this path
        all_hooks = [direct_hook]  # Start with the direct hook (for tracking)
        
        # Check each edge in the graph to find dependencies of this upstream
        def collect_all_hooks(current_frame):
            # Find this frame's direct dependencies in the graph
            for u, v, edge_data in directed_acyclical_graph.edges(current_frame, data=True):
                # The hook name is stored in the edge data
                hook_name = edge_data.get('name', 'unnamed')
                if hook_name not in all_hooks:
                    all_hooks.append(hook_name)
                    # Add to inherited hooks only (excludes the primary hook)
                    dependency_structure["inherited_hooks"].append(hook_name)
                
                # Recursively collect hooks from this dependency
                collect_all_hooks(v)
        
        # Start collection from the direct upstream
        collect_all_hooks(direct_upstream)
        
        # Store the dependency structure
        dependencies[direct_upstream] = dependency_structure
        
    return dependencies

def create_column_descriptions(peripheral: str, pit_hook: str, hook_value: str, hook_part: str, dependencies: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    """
    Create the column_description dictionary for a bridge blueprint.
    
    Args:
        peripheral: Name of the peripheral table
        pit_hook: Point-in-time hook
        hook_value: Original hook value
        hook_part: Processed hook part
        dependencies: Dictionary of dependencies
        
    Returns:
        column_description: Dictionary of column descriptions
    """
    column_description = {}
    
    # First add the peripheral description
    column_description["peripheral"] = "Name of the peripheral table the bridge record belongs to."
    
    # Then add the bridge pit hook
    column_description["_pit_hook__bridge"] = "Point-in-time hook for the bridge record."
    
    # Add the pit hook description (main one gets added first)
    if pit_hook:
        # Parse hook parts (format: _hook__concept__optional_qualifier)
        hook_parts = hook_part.split('__')
        
        pit_description = ""
        hook_description = ""
        if len(hook_parts) > 2:  # Has qualifier
            concept = hook_parts[1]
            qualifier = hook_parts[2]
            pit_description = f"Point-in-time hook to the concept {concept}, with qualifier {qualifier}"
            hook_description = f"Hook to the concept {concept}, with qualifier {qualifier}"
        else:  # No qualifier
            concept = hook_parts[1] if len(hook_parts) > 1 else hook_part
            pit_description = f"Point-in-time hook to the concept {concept}"
            hook_description = f"Hook to the concept {concept}"
        
        column_description[pit_hook] = pit_description
        
        # Process dependency hooks
        for direct_upstream, dependency in dependencies.items():
            # Replace frame__ with bridge__ in dependency reference
            bridge_dependency = direct_upstream.replace("frame__", "bridge__", 1)
            
            # Get the peripheral name for this dependency
            dep_peripheral = bridge_dependency.replace("bridge__", "", 1)
            
            # Start with the primary hook
            original_hook = dependency["primary_hook"]
            hook_part = original_hook.replace("_hook", "", 1)
            pit_primary_hook = "_pit_hook" + hook_part
            
            # Add to column_description
            # Parse hook parts (format: _hook__concept__optional_qualifier)
            hook_parts = hook_part.split('__')
            
            if len(hook_parts) > 2:  # Has qualifier
                concept = hook_parts[1]
                qualifier = hook_parts[2]
                column_description[pit_primary_hook] = f"Point-in-time hook to the concept {concept}, with qualifier {qualifier}"
            else:  # No qualifier
                concept = hook_parts[1] if len(hook_parts) > 1 else hook_part
                column_description[pit_primary_hook] = f"Point-in-time hook to the concept {concept}"
            
            # Add all inherited hooks
            if dependency.get("inherited_hooks"):
                for hook in dependency["inherited_hooks"]:
                    # Transform from _hook__xyz to _pit_hook__xyz
                    hook_part = hook.replace("_hook", "", 1)
                    pit_hook = "_pit_hook" + hook_part
                    
                    # Add to column_description
                    # Parse hook parts (format: _hook__concept__optional_qualifier)
                    hook_parts = hook_part.split('__')
                    
                    if len(hook_parts) > 2:  # Has qualifier
                        concept = hook_parts[1]
                        qualifier = hook_parts[2]
                        column_description[pit_hook] = f"Point-in-time hook to the concept {concept}, with qualifier {qualifier}"
                    else:  # No qualifier
                        concept = hook_parts[1] if len(hook_parts) > 1 else hook_part
                        column_description[pit_hook] = f"Point-in-time hook to the concept {concept}"
        
        # Add the regular hook description last
        if hook_value and hook_description:
            column_description[hook_value] = hook_description
    
    # Add bridge record metadata columns at the very end
    column_description["bridge__record_loaded_at"] = "Timestamp when this bridge record was loaded."
    column_description["bridge__record_updated_at"] = "Timestamp when this bridge record was last updated."
    column_description["bridge__record_valid_from"] = "Timestamp from which this bridge record is valid."
    column_description["bridge__record_valid_to"] = "Timestamp until which this bridge record is valid."
    column_description["bridge__is_current_record"] = "Flag indicating if this is the current valid version of the bridge record."
    
    return column_description

def create_column_data_types(column_description: Dict[str, str]) -> Dict[str, str]:
    """
    Create the column_data_types dictionary based on column descriptions.
    
    Args:
        column_description: Dictionary of column descriptions
        
    Returns:
        column_data_types: Dictionary of column data types
    """
    column_data_types = {}
    
    # Set peripheral as text
    column_data_types["peripheral"] = "text"
    
    # Set all hooks as binary
    for key in column_description.keys():
        if key.startswith("_hook") or key.startswith("_pit_hook"):
            column_data_types[key] = "binary"
    
    # Set timestamp and boolean fields
    column_data_types["bridge__record_loaded_at"] = "timestamp"
    column_data_types["bridge__record_updated_at"] = "timestamp"
    column_data_types["bridge__record_valid_from"] = "timestamp"
    column_data_types["bridge__record_valid_to"] = "timestamp"
    column_data_types["bridge__is_current_record"] = "boolean"
    
    return column_data_types

def generate_bridge_blueprints(hook_config_path: str = None) -> list:
    """
    Generate bridge blueprints based on hook configurations.
    
    Args:
        hook_config_path: Path to the hook config YAML file
        
    Returns:
        List of bridge blueprint dictionaries
    """
    frames_config = load_yaml(hook_config_path)
    frames = frames_config["frames"]

    # Build the directed acyclic graph (DAG) and get primary hooks
    directed_acyclical_graph, primary_hooks = build_directed_graph(frames)
    
    # Convert graph to a dictionary representation with node relationships
    graph_dict = {}
    
    # First pass: get immediate dependencies and classify nodes
    for node in directed_acyclical_graph.nodes():
        # Get the immediate upstream nodes - these are the nodes that this node directly depends on
        immediate_upstream = []
        for upstream_node in directed_acyclical_graph.successors(node):
            edge_data = directed_acyclical_graph.get_edge_data(node, upstream_node)
            edge_name = edge_data.get('name', 'unnamed') if edge_data else 'unnamed'
            immediate_upstream.append((upstream_node, edge_name))
        
        # Add node to dictionary with classification
        graph_dict[node] = {
            "direct_upstream_nodes": immediate_upstream,
            "is_leaf": directed_acyclical_graph.in_degree(node) == 0,
            "is_root": directed_acyclical_graph.out_degree(node) == 0
        }
    
    # Second pass: process dependency structure for each node
    for node in directed_acyclical_graph.nodes():
        # Process and enhance the dependency structure
        dependencies = process_node_dependencies(node, graph_dict, directed_acyclical_graph)
        graph_dict[node]["dependencies"] = dependencies
        
        # Also maintain original references for compatibility
        graph_dict[node]["upstream_nodes"] = graph_dict[node]["direct_upstream_nodes"]

    # Generate the final blueprints
    graph_list = []
    
    for node in graph_dict:
        # Create target name by replacing 'frame' prefix with 'bridge'
        target_name = node.replace('frame__', 'bridge__', 1)
        peripheral = target_name.replace("bridge__", "", 1)
        
        # Get column prefix from frame config
        column_prefix = None
        for frame_config in frames:
            if frame_config["name"] == node:
                column_prefix = frame_config.get("column_prefix")
                break
        
        # Use default prefix if not found
        if column_prefix is None:
            column_prefix = peripheral.replace("adventure_works", "").strip("_")
        
        # Find primary hook for this node
        hook_value = None
        pit_hook = None
        hook_part = None
        
        for frame in frames:
            if frame["name"] == node:
                for hook in frame.get("hooks", []):
                    if hook.get("primary", False):
                        hook_value = hook["name"]
                        # Create pit-prefixed version
                        hook_part = hook_value.replace("_hook", "", 1)
                        pit_hook = "_pit_hook" + hook_part
                        break
                break
        
        # Create column descriptions
        column_description = create_column_descriptions(
            peripheral=peripheral,
            pit_hook=pit_hook,
            hook_value=hook_value,
            hook_part=hook_part if hook_part else "",
            dependencies=graph_dict[node]["dependencies"]
        )
        
        # Create column data types
        column_data_types = create_column_data_types(column_description)
        
        # Create the blueprint dictionary
        node_dict = {
            "name": target_name,
            "source_name": node,
            "peripheral": peripheral,
            "column_prefix": column_prefix,
            "description": f"Puppini bridge for the peripheral table {peripheral}",
            "column_descriptions": column_description,
            "column_data_types": column_data_types
        }
        
        # Add hook information if it exists
        if hook_value:
            node_dict["primary_hook"] = pit_hook
            node_dict["hook"] = hook_value
        
        # Process dependencies
        processed_dependencies = {}
        for direct_upstream, dependency in graph_dict[node]["dependencies"].items():
            # Replace frame__ with bridge__ in dependency reference
            bridge_dependency = direct_upstream.replace("frame__", "bridge__", 1)
            
            # Create dependency structure
            dep_structure = {
                "primary_hook": dependency["primary_hook"]
            }
            
            # Process inherited hooks
            inherited_hooks = []
            
            # Add primary hook with _pit_hook__ prefix
            original_hook = dependency["primary_hook"]
            hook_part = original_hook.replace("_hook", "", 1)
            pit_primary_hook = "_pit_hook" + hook_part
            inherited_hooks.append(pit_primary_hook)
            
            # Add all other inherited hooks
            if dependency.get("inherited_hooks"):
                for hook in dependency["inherited_hooks"]:
                    hook_part = hook.replace("_hook", "", 1)
                    pit_hook = "_pit_hook" + hook_part
                    inherited_hooks.append(pit_hook)
            
            # Add inherited hooks to structure
            if inherited_hooks:
                dep_structure["inherited_hooks"] = inherited_hooks
            
            processed_dependencies[bridge_dependency] = dep_structure
        
        # Add dependencies to blueprint
        if processed_dependencies:
            node_dict["dependencies"] = processed_dependencies
            
        graph_list.append(node_dict)

    # Export blueprints to files
    export_blueprints(graph_list, "./models/blueprints/bridges")

    return graph_list

def process_date_columns(hook_blueprint: Dict[str, Any]) -> Tuple[List[str], List[str], Dict[str, str], Dict[str, str]]:
    """
    Process date columns from a hook blueprint and generate event-related metadata.
    
    Args:
        hook_blueprint: Hook blueprint dictionary
        
    Returns:
        Tuple of (date_columns, event_columns, event_column_data_types, event_column_descriptions)
    """
    event_rename = lambda x: f"event__{x.replace('_date', '')}"
    
    # Find date columns and create mappings
    date_columns = {col_name: event_rename(col_name) 
                  for col_name, col_type in hook_blueprint["column_data_types"].items() 
                  if col_type == "date"}
    
    # Create column descriptions for event flags
    event_describe = lambda x: f"Flag indicating a {event_rename(x).split('__')[2]} event for this {x.split('__')[0]}."
    event_column_descriptions = {event_rename(col): event_describe(col) for col in date_columns}
    
    # Set data types for event flags to boolean
    event_column_data_types = {col: "boolean" for col in date_columns.values()}
    
    # Return list of event column names
    event_columns = list(date_columns.values())
    
    return date_columns, event_columns, event_column_data_types, event_column_descriptions

def create_event_blueprint(bridge: Dict[str, Any], hook_blueprint: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create an event blueprint from bridge and hook blueprints.
    
    Args:
        bridge: Bridge blueprint dictionary
        hook_blueprint: Hook blueprint dictionary
        
    Returns:
        Dictionary containing the event blueprint
    """
    bridge_name = bridge["name"]
    hook_name = bridge_name.replace("bridge__", "frame__")
    event_name = bridge_name.replace("bridge__", "event__")
    primary_pit_hook = hook_blueprint["grain"]
    
    # Start with columns and metadata from bridge
    column_data_types = bridge["column_data_types"].copy()
    column_descriptions = bridge["column_descriptions"].copy()
    columns = list(bridge["column_data_types"].keys())
    
    # Process date columns into event flags
    date_columns, event_cols, event_types, event_descriptions = process_date_columns(hook_blueprint)
    
    # Update metadata with event information
    columns.extend(event_cols)
    column_data_types.update(event_types)
    column_descriptions.update(event_descriptions)
    
    # Add date hook
    columns.append("_hook__epoch__date")
    column_data_types["_hook__epoch__date"] = "binary"
    column_descriptions["_hook__epoch__date"] = "Hook to the concept epoch, with qualifier date."
    
    return {
        "event_name": event_name,
        "description": f"Event viewpoint of {bridge_name}.",
        "hook_name": hook_name,
        "bridge_name": bridge_name,
        "primary_pit_hook": primary_pit_hook,
        "date_columns": date_columns,
        "columns": columns,
        "column_data_types": column_data_types,
        "column_descriptions": column_descriptions
    }

def generate_event_blueprints(hook_blueprint_path: str, bridge_blueprint_path: str) -> list:
    """
    Generate event blueprints from hook and bridge blueprints.
    
    Args:
        hook_blueprint_path: Path to hook blueprints directory
        bridge_blueprint_path: Path to bridge blueprints directory
        
    Returns:
        List of event blueprint dictionaries
    """
    hook_blueprints = import_blueprints(hook_blueprint_path)
    bridge_blueprints = import_blueprints(bridge_blueprint_path)

    blueprints = []

    for bridge in bridge_blueprints:
        hook_name = bridge["name"].replace("bridge__", "frame__")
        hook_blueprint = next((hook for hook in hook_blueprints if hook["name"] == hook_name), None)
        
        if hook_blueprint:
            blueprint = create_event_blueprint(bridge, hook_blueprint)
            blueprints.append(blueprint)

    export_blueprints(
        blueprints,
        name_field="event_name",
        output_path="./models/blueprints/events"
    )

    return blueprints

def filter_hook_columns(hook_blueprint: Dict[str, Any]) -> Tuple[List[str], Dict[str, str], Dict[str, str]]:
    """
    Filter out hook columns from a hook blueprint and extract relevant metadata.
    
    Args:
        hook_blueprint: Hook blueprint dictionary
        
    Returns:
        Tuple of (columns, column_data_types, column_descriptions)
    """
    # Filter out hook columns
    columns = [col for col in hook_blueprint["columns"] if not col.startswith("_hook__")]
    
    # Get data types and descriptions for non-hook columns
    column_data_types = {col: hook_blueprint["column_data_types"][col] for col in columns}
    column_descriptions = {col: hook_blueprint["column_descriptions"][col] for col in columns}
    
    return columns, column_data_types, column_descriptions

def create_peripheral_blueprint(hook_blueprint: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a peripheral blueprint from a hook blueprint.
    
    Args:
        hook_blueprint: Hook blueprint dictionary
        
    Returns:
        Dictionary containing the peripheral blueprint
    """
    hook_name = hook_blueprint["name"]
    description = hook_blueprint["description"]
    peripheral_name = hook_name.replace("frame__", "")
    grain = hook_blueprint["grain"]
    
    # Get column metadata (filtering out hook columns)
    columns, column_data_types, column_descriptions = filter_hook_columns(hook_blueprint)
    
    return {
        "peripheral_name": peripheral_name,
        "description": description,
        "hook_name": hook_name,
        "grain": grain,
        "columns": columns,
        "column_data_types": column_data_types,
        "column_descriptions": column_descriptions
    }

def generate_peripheral_blueprints(hook_blueprint_path: str) -> list:
    """
    Generate peripheral blueprints from hook blueprints.
    
    Args:
        hook_blueprint_path: Path to hook blueprints directory
        
    Returns:
        List of peripheral blueprint dictionaries
    """
    hook_blueprints = import_blueprints(hook_blueprint_path)

    blueprints = []

    for hook in hook_blueprints:
        blueprint = create_peripheral_blueprint(hook)
        blueprints.append(blueprint)

    export_blueprints(
        blueprints,
        name_field="peripheral_name",
        output_path="./models/blueprints/peripherals"
    )

    return blueprints
        
if __name__ == "__main__":
    raw_blueprints = generate_raw_blueprints(
        schema_path="./models/raw_schema.yaml"
    )

    hook_blueprints = generate_hook_blueprints(
        hook_config_path="./models/hook__frames.yml",
        schema_path="./models/raw_schema.yaml"
    )

    bridge_blueprints = generate_bridge_blueprints(
        hook_config_path="./models/hook__frames.yml"
    )

    event_blueprints = generate_event_blueprints(
        hook_blueprint_path="./models/blueprints/hook",
        bridge_blueprint_path="./models/blueprints/bridges"
    )

    peripheral_blueprints = generate_peripheral_blueprints(
        hook_blueprint_path="./models/blueprints/hook"
    )