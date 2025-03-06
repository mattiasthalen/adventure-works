import os
import yaml
import networkx as nx

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

def build_dependency_graph(bags_config):
    """
    Build a directed graph representing dependencies between bags based on hooks.
    Returns a NetworkX DiGraph where nodes are bag names and edges represent dependencies.
    """
    # Create directed graph
    G = nx.DiGraph()
    
    # Map hooks to their primary bags
    hook_to_primary_bag = {}
    for bag in bags_config['bags']:
        bag_name = bag['name']
        G.add_node(bag_name)  # Add node even if it has no dependencies
        
        # Find primary hook
        primary_hook = None
        for hook in bag['hooks']:
            if hook.get('primary', False):
                primary_hook = hook
                break
        
        if primary_hook is None and len(bag['hooks']) > 0:
            primary_hook = bag['hooks'][0]  # Default to first hook if none marked primary
            
        if primary_hook:
            hook_to_primary_bag[primary_hook['name']] = bag_name
    
    # Create edges based on hook dependencies
    for bag in bags_config['bags']:
        bag_name = bag['name']
        
        # Find primary hook for this bag
        this_primary_hook = None
        for hook in bag['hooks']:
            if hook.get('primary', False):
                this_primary_hook = hook
                break
        
        if this_primary_hook is None and len(bag['hooks']) > 0:
            this_primary_hook = bag['hooks'][0]
        
        # Find foreign hooks (non-primary hooks)
        foreign_hooks = []
        if this_primary_hook:
            foreign_hooks = [h for h in bag['hooks'] if h['name'] != this_primary_hook['name']]
        
        # For each foreign hook, find its primary bag and add dependency
        for hook in foreign_hooks:
            if hook['name'] in hook_to_primary_bag:
                dependent_bag = hook_to_primary_bag[hook['name']]
                if dependent_bag != bag_name:  # Avoid self-loops
                    G.add_edge(dependent_bag, bag_name)  # dependent_bag -> bag_name
    
    return G

def get_hooks_by_concept(bags_config):
    """
    Group hooks by their core business concept.
    Returns a dictionary where keys are concepts and values are lists of hooks.
    """
    # Extract concepts from hook names
    hooks_by_concept = {}
    
    for bag in bags_config['bags']:
        for hook in bag['hooks']:
            # Extract concept from hook name (e.g., "_hook__person__employee" -> "person")
            parts = hook['name'].split('__')
            if len(parts) >= 2:
                concept = parts[1]
                
                if concept not in hooks_by_concept:
                    hooks_by_concept[concept] = []
                
                hook_info = {
                    'name': hook['name'],
                    'bag': bag['name'],
                    'is_primary': hook.get('primary', False)
                }
                hooks_by_concept[concept].append(hook_info)
    
    return hooks_by_concept

def group_bags_by_concept(bags_config):
    """
    Group bags by their primary hook's concept.
    Returns a dictionary where keys are concepts and values are lists of bag names.
    """
    # Group bags by concept
    bags_by_concept = {}
    
    for bag in bags_config['bags']:
        # Find primary hook
        primary_hook = None
        for hook in bag['hooks']:
            if hook.get('primary', False):
                primary_hook = hook
                break
        
        if primary_hook is None and len(bag['hooks']) > 0:
            primary_hook = bag['hooks'][0]
            
        if primary_hook:
            # Extract concept from hook name
            parts = primary_hook['name'].split('__')
            if len(parts) >= 2:
                concept = parts[1]
                
                if concept not in bags_by_concept:
                    bags_by_concept[concept] = []
                
                bags_by_concept[concept].append(bag['name'])
    
    return bags_by_concept

def find_qualified_hooks(bags_config):
    """
    Identify hooks that have the same concept but different qualifiers.
    Returns a dictionary mapping base concepts to lists of qualified hooks.
    """
    qualified_hooks = {}
    
    for bag in bags_config['bags']:
        for hook in bag['hooks']:
            parts = hook['name'].split('__')
            if len(parts) > 2:  # Has qualifier
                concept = parts[1]
                qualifier = parts[2]
                
                if concept not in qualified_hooks:
                    qualified_hooks[concept] = []
                
                qualified_hooks[concept].append({
                    'name': hook['name'],
                    'concept': concept,
                    'qualifier': qualifier,
                    'bag': bag['name']
                })
    
    # Filter to only include concepts with multiple qualifiers
    return {k: v for k, v in qualified_hooks.items() if len(v) > 1}