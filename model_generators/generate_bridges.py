import os
import networkx as nx
from parse_yaml import load_bags_config, ensure_directory_exists

def generate_bridges(
    output_dir,
    secondary_output_dir,
    hook_schema,
    measure_schema,
    bridge_schema
):
    """Generate bridge model SQL files based on bags configuration"""
    # Ensure output directories exist
    ensure_directory_exists(output_dir)
    ensure_directory_exists(secondary_output_dir)
    
    # Load YAML files
    bags_config = load_bags_config()
    
    # Build hook dependency graph
    hook_graph, bag_info = build_hook_dependency_graph(bags_config)
    
    # Calculate transitive dependencies and collect all inherited PIT hooks
    collect_transitive_dependencies(hook_graph, bag_info)
    
    # Determine build order using topological sort
    build_order = list(nx.topological_sort(hook_graph))
    
    # Generate intermediate bridges for each bag
    intermediate_count = 0
    for bag_name in build_order:
        if bag_name in bag_info:
            success = generate_intermediate_bridge(
                bag_name, 
                bag_info[bag_name], 
                hook_graph, 
                bag_info, 
                output_dir,
                hook_schema,
                measure_schema,
                bridge_schema
            )
            if success:
                intermediate_count += 1
    
    # Generate unified bridge for all intermediate bridges
    success = generate_unified_bridge(
        bag_info, 
        build_order, 
        secondary_output_dir,
        bridge_schema
    )
    unified_count = 1 if success else 0
    
    print(f"Generated {intermediate_count} intermediate bridges in {output_dir}")
    print(f"Generated {unified_count} unified bridge in {secondary_output_dir}")
    
    return intermediate_count, unified_count

def find_measure_model(bag_name, measure_schema):
    """Check if a measure model exists for the given bag"""
    table_name = bag_name.replace('bag__adventure_works__', '')
    measure_model_path = f'./models/{measure_schema}/measure__adventure_works__{table_name}.sql'
    return os.path.exists(measure_model_path)

def get_measure_fields(bag_name, measure_schema):
    """Extract measure fields from measure model file"""
    table_name = bag_name.replace('bag__adventure_works__', '')
    measure_model_path = f'./models/{measure_schema}/measure__adventure_works__{table_name}.sql'
    
    if not os.path.exists(measure_model_path):
        return None, []
    
    with open(measure_model_path, 'r') as f:
        content = f.read()
    
    # Find the final SELECT section
    select_section = content.split('FROM cte__epoch')[0].split('SELECT')[-1]
    
    # Extract measure fields and epoch hook
    measure_fields = []
    epoch_date_field = None
    
    for line in select_section.split('\n'):
        line = line.strip()
        if '_hook__epoch__date' in line:
            epoch_date_field = '_hook__epoch__date'
        elif line.startswith('measure__'):
            field_name = line.split('::')[0].strip()
            measure_fields.append(field_name)
    
    return epoch_date_field, measure_fields

def build_hook_dependency_graph(bags_config):
    """Build a directed graph representing hook dependencies between bags"""
    graph = nx.DiGraph()
    bag_info = {}
    
    # Create mapping from hook name to primary bag
    hook_to_primary_bag = {}
    for bag in bags_config['bags']:
        primary_hook = next((h for h in bag['hooks'] if h.get('primary')), bag['hooks'][0])
        hook_to_primary_bag[primary_hook['name']] = bag['name']
    
    # Add all bags as nodes
    for bag in bags_config['bags']:
        bag_name = bag['name']
        graph.add_node(bag_name)
        
        # Store relevant bag info for later use
        primary_hook = next((h for h in bag['hooks'] if h.get('primary')), bag['hooks'][0])
        bag_info[bag_name] = {
            'column_prefix': bag['column_prefix'],
            'source_table': bag['source_table'],
            'primary_hook': primary_hook,
            'hooks': bag['hooks'],
            'dependencies': [],
            'all_pit_hooks': set([f"_pit{primary_hook['name']}"]),  # Initialize with own primary hook
            'all_dependencies': set()  # For storing transitive dependencies
        }
    
    # Add edges based on hook dependencies
    for bag in bags_config['bags']:
        bag_name = bag['name']
        primary_hook = next((h for h in bag['hooks'] if h.get('primary')), bag['hooks'][0])
        
        # Find dependencies (bags that contain hooks used by this bag)
        for hook in bag['hooks']:
            if hook == primary_hook:
                continue  # Skip primary hook
                
            # Find source bags for this hook
            for other_bag in bags_config['bags']:
                if other_bag['name'] == bag_name:
                    continue  # Skip self
                    
                other_primary = next((h for h in other_bag['hooks'] if h.get('primary')), other_bag['hooks'][0])
                if hook['name'] == other_primary['name'] or any(h['name'] == hook['name'] for h in other_bag['hooks'] if h.get('primary')):
                    # Add dependency edge from dependent bag to this bag
                    graph.add_edge(other_bag['name'], bag_name)
                    bag_info[bag_name]['dependencies'].append({
                        'bag_name': other_bag['name'],
                        'hook': hook
                    })
    
    return graph, bag_info

def collect_transitive_dependencies(hook_graph, bag_info):
    """Collect all transitive dependencies and their PIT hooks"""
    # Process bags in topological order (dependencies first)
    for bag_name in nx.topological_sort(hook_graph):
        if bag_name not in bag_info:
            continue
            
        # For each direct dependency
        for dep in bag_info[bag_name]['dependencies']:
            dep_bag_name = dep['bag_name']
            dep_hook = dep['hook']
            
            # Add dependency's primary PIT hook
            bag_info[bag_name]['all_pit_hooks'].add(f"_pit{dep_hook['name']}")
            
            # Add dependency to the set of all dependencies
            bag_info[bag_name]['all_dependencies'].add(dep_bag_name)
            
            # Inherit all of dependency's inherited PIT hooks
            if dep_bag_name in bag_info:
                bag_info[bag_name]['all_pit_hooks'].update(bag_info[dep_bag_name]['all_pit_hooks'])
                bag_info[bag_name]['all_dependencies'].update(bag_info[dep_bag_name]['all_dependencies'])

def build_references_list(bag_info):
    """Build list of PIT hooks to reference in the model"""
    return sorted(list(bag_info['all_pit_hooks']))

def generate_base_cte(bridge_name, primary_hook, dependencies, column_prefix, bag_name, hook_schema, measure_schema):
    """Generate the base CTE that selects from the bag including measures"""
    # Check for measures
    epoch_date_field, measure_fields = get_measure_fields(bag_name, measure_schema)
    
    sql = f"""WITH cte__bridge AS (
  SELECT
    '{bridge_name.replace("uss_bridge__", "")}' AS peripheral,
    _pit{primary_hook['name']},
    {primary_hook['name']},
"""
    
    # Add foreign hooks
    for dep in dependencies:
        sql += f"    {dep['hook']['name']},\n"
    
    # Add epoch date field if present
    if epoch_date_field:
        sql += f"    {epoch_date_field},\n"
    
    # Add measure fields if present
    for field in measure_fields:
        sql += f"    {field},\n"
    
    # Add system fields
    sql += f"""    {column_prefix}__record_loaded_at AS bridge__record_loaded_at,
    {column_prefix}__record_updated_at AS bridge__record_updated_at,
    {column_prefix}__record_valid_from AS bridge__record_valid_from,
    {column_prefix}__record_valid_to AS bridge__record_valid_to,
    {column_prefix}__is_current_record AS bridge__is_current_record
  FROM {hook_schema}.{bag_name}"""
    
    # Add LEFT JOIN to measure model if needed
    if measure_fields and epoch_date_field:
        table_name = bag_name.replace('bag__adventure_works__', '')
        measure_model = f"measure__adventure_works__{table_name}"
        sql += f"""
  LEFT JOIN {measure_schema}.{measure_model} USING (_pit{primary_hook['name']})"""
    
    sql += "\n)"
    
    return sql

def generate_join_fragments(dependencies, all_bag_info, bridge_schema):
    """Generate SQL fragments for joining to dependency bridges"""
    join_fragments = []
    
    for dep in dependencies:
        dep_bag = all_bag_info.get(dep['bag_name'])
        if not dep_bag:
            continue
            
        dep_bridge = f"uss_bridge__{dep['bag_name'].replace('bag__adventure_works__', '')}"
        dep_hook = dep['hook']
        
        join_fragment = f"""  LEFT JOIN {bridge_schema}.{dep_bridge}
  ON cte__bridge.{dep_hook['name']} = {dep_bridge}.{dep_hook['name']}
  AND cte__bridge.bridge__record_valid_from >= {dep_bridge}.bridge__record_valid_from
  AND cte__bridge.bridge__record_valid_to <= {dep_bridge}.bridge__record_valid_to"""
        
        join_fragments.append({
            'sql': join_fragment,
            'bridge_name': dep_bridge,
            'hook': dep_hook
        })
    
    return join_fragments

def build_pit_hooks_list(bag_info, all_bag_info):
    """Build a list of all PIT hooks with consistent ordering"""
    # Start with primary PIT hook
    primary_pit_hook = f"cte__bridge._pit{bag_info['primary_hook']['name']}"
    
    # Collect all dependency PIT hooks
    dependency_pit_hooks = []
    
    # Add PIT hooks from immediate dependencies
    for dep in sorted(bag_info['dependencies'], key=lambda x: x['bag_name']):
        dep_bag_name = dep['bag_name']
        if dep_bag_name not in all_bag_info:
            continue
            
        dep_bridge = f"uss_bridge__{dep_bag_name.replace('bag__adventure_works__', '')}"
        dep_hook = dep['hook']
        
        # Add the immediate dependency's PIT hook
        dep_pit_hook = f"{dep_bridge}._pit{dep_hook['name']}"
        dependency_pit_hooks.append(dep_pit_hook)
        
        # Add any inherited PIT hooks
        for pit_hook in sorted(all_bag_info[dep_bag_name]['all_pit_hooks']):
            if pit_hook != f"_pit{dep_hook['name']}":  # Skip the hook we just added
                dependency_pit_hooks.append(f"{dep_bridge}.{pit_hook}")
    
    # Sort dependency hooks alphabetically
    dependency_pit_hooks.sort()
    
    # Combine all hooks with primary hook first
    pit_hooks = [primary_pit_hook] + dependency_pit_hooks
    
    return pit_hooks

def generate_temporal_calculations(dependencies, all_bag_info):
    """Generate SQL for temporal field calculations with dependencies"""
    sql = ""
    
    # Build list of bridge names for dependencies
    bridge_names = []
    for dep in dependencies:
        dep_bag_name = dep['bag_name']
        if dep_bag_name in all_bag_info:
            bridge_name = f"uss_bridge__{dep_bag_name.replace('bag__adventure_works__', '')}"
            bridge_names.append(bridge_name)
    
    # Generate GREATEST for record_loaded_at
    sql += "    GREATEST(\n"
    sql += "        cte__bridge.bridge__record_loaded_at"
    for bridge_name in bridge_names:
        sql += f",\n        {bridge_name}.bridge__record_loaded_at"
    sql += "\n    ) AS bridge__record_loaded_at,\n"
    
    # Generate GREATEST for record_updated_at
    sql += "    GREATEST(\n"
    sql += "        cte__bridge.bridge__record_updated_at"
    for bridge_name in bridge_names:
        sql += f",\n        {bridge_name}.bridge__record_updated_at"
    sql += "\n    ) AS bridge__record_updated_at,\n"
    
    # Generate GREATEST for record_valid_from
    sql += "    GREATEST(\n"
    sql += "        cte__bridge.bridge__record_valid_from"
    for bridge_name in bridge_names:
        sql += f",\n        {bridge_name}.bridge__record_valid_from"
    sql += "\n    ) AS bridge__record_valid_from,\n"
    
    # Generate LEAST for record_valid_to
    sql += "    LEAST(\n"
    sql += "        cte__bridge.bridge__record_valid_to"
    for bridge_name in bridge_names:
        sql += f",\n        {bridge_name}.bridge__record_valid_to"
    sql += "\n    ) AS bridge__record_valid_to,\n"
    
    # Generate is_current_record check
    sql += "    LIST_HAS_ALL(\n"
    sql += "      ARRAY[True],\n"
    sql += "        ARRAY[\n"
    sql += "          cte__bridge.bridge__is_current_record"
    for bridge_name in bridge_names:
        sql += f",\n          {bridge_name}.bridge__is_current_record"
    sql += "\n        ]\n    ) AS bridge__is_current_record"
    
    return sql

def generate_pit_lookup_cte(bag_info, dependencies, all_bag_info, join_fragments, bag_name, measure_schema):
    """Generate PIT lookup CTE with joins and measures"""
    # Get measure fields
    epoch_date_field, measure_fields = get_measure_fields(bag_name, measure_schema)
    
    sql = "cte__pit_lookup AS (\n"
    sql += "  SELECT\n"
    sql += "    cte__bridge.peripheral,\n"
    
    # Add all PIT hooks
    pit_hooks = build_pit_hooks_list(bag_info, all_bag_info)
    for pit_hook in pit_hooks:
        sql += f"    {pit_hook},\n"
    
    # Add primary hook
    sql += f"    cte__bridge.{bag_info['primary_hook']['name']},\n"
    
    # Add epoch date hook if present
    if epoch_date_field:
        sql += f"    cte__bridge.{epoch_date_field},\n"
    
    # Add measure fields if present
    for field in measure_fields:
        sql += f"    cte__bridge.{field},\n"
    
    # Add temporal calculations
    sql += generate_temporal_calculations(dependencies, all_bag_info)
    
    # Add FROM clause with joins
    sql += "\n  FROM cte__bridge\n"
    for jf in join_fragments:
        sql += jf['sql'] + "\n"
    
    sql += ")"
    
    return sql

def generate_pit_hook_cte(bag_info, all_bag_info, epoch_date_field=None, join_fragments=None):
    """Generate CTE that creates the bridge PIT hook"""
    
    sql = "cte__bridge_pit_hook AS (\n"
    sql += "  SELECT\n"
    sql += "    *,\n"
    sql += "    CONCAT_WS(\n"
    sql += "      '~',\n"
    sql += "      peripheral,\n"
    sql += "      'epoch__valid_from'||bridge__record_valid_from"
    
    # Include the epoch date hook if it exists
    if epoch_date_field:
        sql += f",\n      {epoch_date_field}::TEXT"
    
    # Add all PIT hooks to the concatenation
    for pit_hook in sorted(bag_info['all_pit_hooks']):
        sql += f",\n      {pit_hook}::TEXT"
    
    sql += "\n    ) AS _pit_hook__bridge\n"
    
    if join_fragments:
        sql += "  FROM cte__pit_lookup\n"
    else:
        sql += "  FROM cte__bridge\n"
    
    sql += ")"
    
    return sql

def generate_final_select(bag_info, bag_name, measure_schema):
    """Generate the final SELECT statement with measures"""
    # Get measure fields
    epoch_date_field, measure_fields = get_measure_fields(bag_name, measure_schema)
    
    sql = "SELECT\n"
    sql += "  peripheral::TEXT,\n"
    sql += "  _pit_hook__bridge::BLOB,\n"
    
    # Add all PIT hooks
    for pit_hook in sorted(bag_info['all_pit_hooks']):
        sql += f"  {pit_hook}::BLOB,\n"
    
    # Add primary hook
    sql += f"  {bag_info['primary_hook']['name']}::BLOB,\n"
    
    # Add epoch date hook if present
    if epoch_date_field:
        sql += f"  {epoch_date_field}::BLOB,\n"
    
    # Add measure fields if present
    for field in measure_fields:
        sql += f"  {field}::INT,\n"
    
    # Add temporal fields
    sql += """  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_pit_hook
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts"""
    
    return sql
    
def generate_intermediate_bridge(
    bag_name, 
    bag_info, 
    hook_graph, 
    all_bag_info, 
    output_dir,
    hook_schema,
    measure_schema,
    bridge_schema
):
    """Generate SQL for an intermediate bridge model"""
    column_prefix = bag_info['column_prefix']
    primary_hook = bag_info['primary_hook']
    dependencies = bag_info['dependencies']
    
    # Extract concept from primary hook
    hook_parts = primary_hook['name'].split('__')
    if len(hook_parts) < 2:
        return False  # Invalid hook name format
    
    # Create file path for bridge model
    bridge_name = f"uss_bridge__{bag_name.replace('bag__adventure_works__', '')}"
    sql_path = os.path.join(output_dir, f"{bridge_name}.sql")
    
    # Build components of the SQL
    references = build_references_list(bag_info)
    base_cte = generate_base_cte(bridge_name, primary_hook, dependencies, column_prefix, bag_name, hook_schema, measure_schema)
    join_fragments = generate_join_fragments(dependencies, all_bag_info, bridge_schema)
    
    # Get measure fields including epoch date field
    epoch_date_field, measure_fields = get_measure_fields(bag_name, measure_schema)
    
    with open(sql_path, 'w') as sql_file:
        # Write MODEL declaration
        sql_file.write(f"""MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge,
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags bridge,
  grain (_pit_hook__bridge),
  references ({', '.join(references)})
);

""")
        
        # Write base CTE
        sql_file.write(base_cte)
        
        # If we have dependencies, add join logic
        if dependencies:
            sql_file.write(",\n")
            pit_lookup_cte = generate_pit_lookup_cte(bag_info, dependencies, all_bag_info, join_fragments, bag_name, measure_schema)
            sql_file.write(pit_lookup_cte)
            sql_file.write(",\n")
        else:
            sql_file.write(",\n")
        
        # Add PIT hook generation
        pit_hook_cte = generate_pit_hook_cte(
            bag_info, 
            all_bag_info, 
            epoch_date_field=epoch_date_field,
            join_fragments=join_fragments if dependencies else None
        )
        sql_file.write(pit_hook_cte)
        
        sql_file.write("\n")
        
        # Write final SELECT statement
        final_select = generate_final_select(bag_info, bag_name, measure_schema)
        sql_file.write(final_select)
    
    return True

def generate_unified_bridge(bag_info, build_order, output_dir, bridge_schema):
    """Generate a unified bridge that contains all intermediate bridges"""
    bridge_name = "_bridge__as_of"
    sql_path = os.path.join(output_dir, f"{bridge_name}.sql")
    
    # Create list of intermediate bridges to include
    intermediate_bridges = []
    all_pit_hooks = set()
    all_epoch_date_hooks = set()
    all_measure_fields = set()
    
    for bag_name in build_order:
        if bag_name in bag_info:
            bridge = f"uss_bridge__{bag_name.replace('bag__adventure_works__', '')}"
            intermediate_bridges.append(bridge)
            all_pit_hooks.update(bag_info[bag_name]['all_pit_hooks'])
            
            # Include epoch date field and measures
            epoch_date_field, measure_fields = get_measure_fields(bag_name, "silver")  # Default to silver for backward compatibility
            if epoch_date_field:
                all_epoch_date_hooks.add(epoch_date_field)
            all_measure_fields.update(measure_fields)
    
    with open(sql_path, 'w') as sql_file:
        # Write MODEL declaration
        references = ', '.join(sorted(all_pit_hooks))
        if all_epoch_date_hooks:
            references += ', ' + ', '.join(sorted(all_epoch_date_hooks))
        
        sql_file.write(f"""MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__bridge),
  references ({references})
);

""")
        
        # Write UNION query
        sql_file.write("WITH cte__bridge_union AS (\n")
        for i, bridge in enumerate(intermediate_bridges):
            sql_file.write(f"  SELECT * FROM {bridge_schema}.{bridge}")
            if i < len(intermediate_bridges) - 1:
                sql_file.write("\n  UNION ALL BY NAME\n")
            else:
                sql_file.write("\n")
        sql_file.write(")\n")
        
        # Write final SELECT with consistent column order
        sql_file.write("SELECT\n")
        sql_file.write("  peripheral::TEXT,\n")
        sql_file.write("  _pit_hook__bridge::BLOB,\n")
        
        # Include all PIT hooks (sorted for consistency)
        for pit_hook in sorted(all_pit_hooks):
            sql_file.write(f"  {pit_hook}::BLOB,\n")
        
        # Include epoch date hook if present
        for epoch_hook in sorted(all_epoch_date_hooks):
            sql_file.write(f"  {epoch_hook}::BLOB,\n")
        
        # Include measure fields
        for measure in sorted(all_measure_fields):
            sql_file.write(f"  {measure}::INT,\n")
        
        # Add temporal fields
        sql_file.write("""  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_union""")
    
    return True