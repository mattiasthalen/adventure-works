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

def get_bag_paths(bags_config):
    """
    Find all paths from each bag to endpoint bags (bags without foreign hooks).
    
    Returns a dictionary mapping source bags to dictionaries of target bags,
    each containing possible join paths between them.
    """
    # Identify which hooks belong to which bags and extract concepts
    hook_info = {}  # Maps hook_name to (bag_name, concept, is_primary)
    bag_hooks = {}  # Maps bag_name to list of hook names
    
    for bag in bags_config['bags']:
        bag_name = bag['name']
        bag_hooks[bag_name] = []
        
        for hook in bag['hooks']:
            hook_name = hook['name']
            is_primary = hook.get('primary', False)
            
            # Extract concept from hook name (_hook__person__individual -> person)
            parts = hook_name.split('__')
            concept = parts[1] if len(parts) > 1 else ""
            
            hook_info[hook_name] = (bag_name, concept, is_primary)
            bag_hooks[bag_name].append(hook_name)
    
    # Group hooks by concept
    concept_to_hooks = {}
    for hook_name, (_, concept, _) in hook_info.items():
        if concept not in concept_to_hooks:
            concept_to_hooks[concept] = []
        concept_to_hooks[concept].append(hook_name)
    
    # Identify endpoint bags (those without foreign hooks)
    endpoint_bags = []
    for bag_name, hooks in bag_hooks.items():
        if all(hook_info[hook][2] for hook in hooks):  # All hooks are primary
            endpoint_bags.append(bag_name)
    
    # Build a graph representing connections between bags via concepts
    bag_graph = {}
    for bag_name in bag_hooks:
        bag_graph[bag_name] = set()
    
    for bag_name, hooks in bag_hooks.items():
        for hook_name in hooks:
            _, concept, _ = hook_info[hook_name]
            
            # Find bags that can be reached through this concept
            for other_hook in concept_to_hooks.get(concept, []):
                if hook_name != other_hook:
                    other_bag, _, _ = hook_info[other_hook]
                    if bag_name != other_bag:
                        bag_graph[bag_name].add(other_bag)
    
    # Find all paths from each bag to endpoint bags
    paths = {}
    
    def find_all_paths(current, target, path=None, visited=None):
        """Find all paths from current bag to target bag"""
        if path is None:
            path = []
        if visited is None:
            visited = set()
            
        # Avoid cycles
        if current in visited:
            return []
            
        # Update path and visited
        new_path = path + [current]
        new_visited = visited.union({current})
        
        # If we reached the target, return this path
        if current == target:
            return [new_path]
            
        # Explore all neighbors
        all_paths = []
        for neighbor in bag_graph[current]:
            paths_from_neighbor = find_all_paths(neighbor, target, new_path, new_visited)
            all_paths.extend(paths_from_neighbor)
            
        return all_paths
    
    # Find paths from each bag to each endpoint
    for source_bag in bag_hooks:
        paths[source_bag] = {}
        
        for endpoint in endpoint_bags:
            if source_bag == endpoint:
                continue
                
            all_paths = find_all_paths(source_bag, endpoint)
            
            if all_paths:
                paths[source_bag][endpoint] = []
                
                for path in all_paths:
                    # Convert bag path to hook path
                    hook_path = []
                    for i in range(len(path) - 1):
                        from_bag = path[i]
                        to_bag = path[i + 1]
                        
                        # Find hook in from_bag that connects to concept in to_bag
                        connecting_hooks = []
                        for hook in bag_hooks[from_bag]:
                            _, concept, _ = hook_info[hook]
                            
                            # Check if to_bag has a hook with this concept
                            if any(hook_info[h][1] == concept for h in bag_hooks[to_bag]):
                                connecting_hooks.append(hook)
                                
                        if connecting_hooks:
                            hook_path.append(connecting_hooks[0])
                    
                    # Add path with qualifier if multiple paths exist
                    path_info = {
                        'bag_path': path,
                        'hook_path': hook_path
                    }
                    
                    if len(paths[source_bag][endpoint]) > 0:
                        path_info['qualifier'] = f"path_{len(paths[source_bag][endpoint])}"
                        
                    paths[source_bag][endpoint].append(path_info)
    
    return paths

def print_bag_paths(paths):
    """
    Print the bag paths in a readable format.
    
    Args:
        paths: Dictionary returned by get_bag_paths
    """
    print("\n=== BAG PATHS ===\n")
    
    for source_bag, targets in sorted(paths.items()):
        if not targets:
            print(f"{source_bag}: No paths to endpoint bags")
            continue
            
        print(f"{source_bag}:")
        
        for target_bag, path_list in sorted(targets.items()):
            print(f"  → {target_bag} ({len(path_list)} paths):")
            
            for i, path_info in enumerate(path_list):
                qualifier = path_info.get('qualifier', '')
                qualifier_str = f" ({qualifier})" if qualifier else ""
                
                # Format bag path
                bag_path = ' → '.join(path_info['bag_path'])
                
                # Format hook path
                hook_path = ' → '.join(path_info['hook_path'])
                
                print(f"    Path {i+1}{qualifier_str}:")
                print(f"      Bags: {bag_path}")
                print(f"      Hooks: {hook_path}")
                print()
    
    print("=== END BAG PATHS ===\n")