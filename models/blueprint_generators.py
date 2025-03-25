import networkx as nx
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

def generate_bridge_blueprints(hook_config_path: str = None) -> list:
    bags_config = load_yaml(hook_config_path)
    bags = bags_config["bags"]

    # We need to generate a DAG so that we can have a cascading inheritance of hooks
    directed_acyclical_graph = nx.DiGraph()

    # We also need to keep track of the primary hook in each bag
    primary_hooks = {}

    # First we need to register each bag as a node in the DAG and capture the primary hook
    for bag in bags:
        bag_name = bag["name"]

        directed_acyclical_graph.add_node(bag_name)

        primary_hook = next((hook["name"] for hook in bag["hooks"] if hook.get("primary", False)))
        primary_hooks[bag_name] = primary_hook
    
    # Then we need to register the hooks as edges - representing dependencies between bags
    # The direction should be: A bag with a foreign hook depends on a bag where that hook is primary
    for bag in bags:
        from_bag = bag["name"]  # The bag containing the reference (depends on others)
        
        for hook in bag["hooks"]:
            # Skip the primary hook of this bag
            if hook.get("primary", False):
                continue
            
            foreign_hook = hook["name"]
            
            # Find the bag where this foreign hook is defined as a primary hook
            to_bag = None
            for target_bag in bags:
                primary_hook = next((h["name"] for h in target_bag["hooks"] if h.get("primary", False)), None)
                if primary_hook == foreign_hook:
                    to_bag = target_bag["name"]
                    break
            
            # Skip if we couldn't find a bag with this primary hook
            if not to_bag:
                continue
                
            # Add an edge: from_bag -> to_bag means from_bag DEPENDS ON to_bag
            directed_acyclical_graph.add_edge(
                u_of_edge=from_bag,      # This bag depends on...
                v_of_edge=to_bag,        # ...this bag
                name=foreign_hook
            )

    # Convert graph to a dictionary representation with node relationships as tuples
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
            "is_leaf": directed_acyclical_graph.in_degree(node) == 0,   # A leaf has no incoming edges (nothing depends on it)
            "is_root": directed_acyclical_graph.out_degree(node) == 0    # A root has no outgoing edges (has no dependencies)
        }
    
    # Second pass: compute all upstream dependencies (transitive closure) and group them
    for node in directed_acyclical_graph.nodes():
        # Initialize flat set of all upstream dependencies (for tracking)
        all_upstream_flat = set(upstream for upstream, _ in graph_dict[node]["direct_upstream_nodes"])
        
        # Create a hierarchical structure for the dependencies
        upstream_tree = {}
        
        # For each direct upstream dependency
        for direct_upstream, hook_name in graph_dict[node]["direct_upstream_nodes"]:
            # Initialize a list for this direct upstream's dependencies
            upstream_tree[direct_upstream] = {
                "hook": hook_name,
                "dependencies": []
            }
            
            # BFS to find all transitive dependencies of this direct upstream
            to_process = [direct_upstream]
            visited = {direct_upstream}  # Track visited nodes to avoid cycles
            
            while to_process:
                current = to_process.pop(0)
                
                # Get this node's direct dependencies
                for next_upstream, next_hook in graph_dict[current]["direct_upstream_nodes"]:
                    # Add to the full flat set of all upstreams
                    all_upstream_flat.add(next_upstream)
                    
                    # Add to this direct upstream's dependency tree if not already visited
                    if next_upstream not in visited:
                        visited.add(next_upstream)
                        to_process.append(next_upstream)
                        
                        # Only add direct dependencies of the current node to the tree
                        if current == direct_upstream:
                            upstream_tree[direct_upstream]["dependencies"].append({
                                "name": next_upstream,
                                "hook": next_hook
                            })
        
        # Store both flat and hierarchical representations
        graph_dict[node]["all_upstream_nodes"] = sorted(list(all_upstream_flat))
        graph_dict[node]["upstream_tree"] = upstream_tree
        
        # Create a mapping of upstream bags to the hooks they provide
        all_upstream_hooks = {}
        
        # Add direct hooks (from immediate dependencies)
        for upstream_bag, hook_name in graph_dict[node]["direct_upstream_nodes"]:
            if upstream_bag not in all_upstream_hooks:
                all_upstream_hooks[upstream_bag] = []
            all_upstream_hooks[upstream_bag].append(hook_name)
        
        # Add inherited hooks from indirect dependencies
        for upstream_bag in all_upstream_flat:
            # Skip direct dependencies (already handled)
            if upstream_bag in [direct[0] for direct in graph_dict[node]["direct_upstream_nodes"]]:
                continue
                
            # Get the primary hook for this upstream bag
            for bag in bags:
                if bag["name"] == upstream_bag:
                    primary_hook = next((hook["name"] for hook in bag["hooks"] if hook.get("primary", False)), None)
                    if primary_hook:
                        if upstream_bag not in all_upstream_hooks:
                            all_upstream_hooks[upstream_bag] = []
                        all_upstream_hooks[upstream_bag].append(primary_hook)
                    break
        
        graph_dict[node]["all_upstream_hooks"] = all_upstream_hooks
        
        # Create a structure that maps each hook to the bags that provide it
        hooks_to_bags = {}
        for upstream_bag, hooks in all_upstream_hooks.items():
            for hook in hooks:
                if hook not in hooks_to_bags:
                    hooks_to_bags[hook] = []
                hooks_to_bags[hook].append(upstream_bag)
        
        graph_dict[node]["hooks_to_bags"] = hooks_to_bags
        
        # Create a structured dependencies mapping with primary hook and inherited hooks
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
            def collect_all_hooks(current_bag):
                # Find this bag's direct dependencies in the graph
                for u, v, edge_data in directed_acyclical_graph.edges(current_bag, data=True):
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
        
        # Store the dependencies data - needed for internal processing
        graph_dict[node]["dependencies"] = dependencies
        
        # Also maintain the original "upstream_nodes" and "direct_upstream_nodes" for internal processing
        graph_dict[node]["upstream_nodes"] = graph_dict[node]["direct_upstream_nodes"]

    # After all processing is complete, create a list of dictionaries
    graph_list = []
    
    for node in graph_dict:
        # Create target_name by replacing 'bag' prefix with 'bridge'
        target_name = node.replace('bag__', 'bridge__', 1)
        
        # Create a new dictionary with source_name, target_name, peripheral, description, and column_description fields
        peripheral = target_name.replace("bridge__", "", 1)  # Remove the bridge__ prefix
        description = f"Puppini bridge for the peripheral table {peripheral}"
        
        # Lookup the column prefix from the bag config
        column_prefix = None
        for bag_config in bags:
            if bag_config["name"] == node:
                column_prefix = bag_config.get("column_prefix")
                break
        
        # If we couldn't find a column prefix, use a default derived from the peripheral name
        if column_prefix is None:
            column_prefix = peripheral.replace("adventure_works", "").strip("_")
            
        node_dict = {
            "source_name": node,
            "target_name": target_name,
            "peripheral": peripheral,
            "column_prefix": column_prefix,
            "description": description
        }
        
        # Add column_description dictionary for all hooks
        column_description = {}
        
        # Start by creating an ordered column_description dictionary
        # First add the peripheral description
        column_description["peripheral"] = "Name of the peripheral table the bridge record belongs to."
        
        # Then add the bridge pit hook
        column_description["_pit_hook__bridge"] = "Point-in-time hook for the bridge record."
        
        # Find the hook for this node (if it exists)
        hook_value = None
        pit_hook = None
        for bag in bags:
            if bag["name"] == node:
                # Find the primary hook for this bag
                for hook in bag.get("hooks", []):
                    if hook.get("primary", False):
                        hook_value = hook["name"]
                        break
                break
        
        # Add hook and primary_hook (pit-prefixed) if it exists
        if hook_value:
            # Create the pit-prefixed version for primary_hook
            hook_part = hook_value.replace("_hook", "", 1)  # Remove the first occurrence of _hook
            pit_hook = "_pit_hook" + hook_part
            node_dict["primary_hook"] = pit_hook
            
            # Set the original hook as 'hook'
            node_dict["hook"] = hook_value
            
            # Prepare descriptions for the hooks
            # Parse hook parts (format: _hook__concept__optional_qualifier)
            hook_parts = hook_part.split('__')
            
            # Add the pit hook next (will be added to column_description later)
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
            
            # Add the pit hook description (main one gets added first)
            column_description[pit_hook] = pit_description
            # Regular hook will be added last
        
        # Create the dependencies structure
        dependencies = {}
        for direct_upstream, dependency in graph_dict[node].get("dependencies", {}).items():
            # Replace bag__ with bridge__ in dependency reference
            bridge_dependency = direct_upstream.replace("bag__", "bridge__", 1)
            
            # Get the peripheral name for this dependency
            dep_peripheral = bridge_dependency.replace("bridge__", "", 1)
            
            # Create the dependency structure
            dep_structure = {
                "primary_hook": dependency["primary_hook"]
            }
            
            # Start inherited hooks with the primary hook and prefix all with _pit_hook__
            inherited_hooks = []
            # Add the primary hook first with _pit_hook__ prefix
            # Original format: _hook__xyz, we want: _pit_hook__xyz
            original_hook = dependency["primary_hook"]
            hook_part = original_hook.replace("_hook", "", 1)  # Remove the first occurrence of _hook
            pit_primary_hook = "_pit_hook" + hook_part
            inherited_hooks.append(pit_primary_hook)
            
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
            
            # Add all inherited hooks with _pit_hook__ prefix
            if dependency.get("inherited_hooks"):
                for hook in dependency["inherited_hooks"]:
                    # Transform from _hook__xyz to _pit_hook__xyz
                    hook_part = hook.replace("_hook", "", 1)  # Remove the first occurrence of _hook
                    pit_hook = "_pit_hook" + hook_part
                    inherited_hooks.append(pit_hook)
                    
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
            
            # Only add inherited_hooks if there are any
            if inherited_hooks:
                dep_structure["inherited_hooks"] = inherited_hooks
            
            dependencies[bridge_dependency] = dep_structure
        
        # Only add dependencies if they exist
        if dependencies:
            node_dict["dependencies"] = dependencies
        
        # Add the regular hook description last (if it exists)
        if hook_value and 'hook_description' in locals():
            column_description[hook_value] = hook_description
        
        # Add bridge record metadata columns at the very end
        column_description["bridge__record_loaded_at"] = "Timestamp when this bridge record was loaded."
        column_description["bridge__record_updated_at"] = "Timestamp when this bridge record was last updated."
        column_description["bridge__record_valid_from"] = "Timestamp from which this bridge record is valid."
        column_description["bridge__record_valid_to"] = "Timestamp until which this bridge record is valid."
        column_description["bridge__is_current_record"] = "Flag indicating if this is the current valid version of the bridge record."
        
        # Create column_data_types dictionary
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
        
        # Add column_description and column_data_types to the node_dict
        if column_description:
            node_dict["column_descriptions"] = column_description
            
        node_dict["column_data_types"] = column_data_types
            
        graph_list.append(node_dict)

    return graph_list

if __name__ == "__main__":
    import json

    blueprints = generate_bridge_blueprints(
        hook_config_path="./models/hook__bags.yml"
    )

    # Print debug information
    print("\nGraph structure for a leaf & a root:")
    for blueprint in blueprints:
        if blueprint["target_name"] in ("bridge__adventure_works__sales_order_details", "bridge__adventure_works__product_categories"):
            print(json.dumps(blueprint, indent=4))