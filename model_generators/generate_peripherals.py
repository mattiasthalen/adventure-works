import os
from parse_yaml import load_bags_config, ensure_directory_exists

def generate_peripherals(output_dir, hook_schema):
    """Generate peripheral views in the gold layer based on silver bags"""
    ensure_directory_exists(output_dir)
    
    bags_config = load_bags_config()
    count = 0
    
    for bag in bags_config['bags']:
        success = generate_peripheral_for_bag(bag, output_dir, hook_schema)
        if success:
            count += 1
    
    print(f"Generated {count} peripheral views in {output_dir}")
    return count

def generate_peripheral_for_bag(bag, output_dir, hook_schema):
    """Generate a peripheral view for a specific bag"""
    bag_name = bag['name']
    
    # Skip _dlt tables
    if bag_name.startswith('_dlt'):
        return False
    
    # Extract peripheral name from bag name
    parts = bag_name.split('__')
    if len(parts) < 3:
        return False
    
    peripheral_name = parts[2]
    sql_path = os.path.join(output_dir, f"{peripheral_name}.sql")
    
    # Find primary hook
    primary_hook = next((h for h in bag['hooks'] if h.get('primary')), bag['hooks'][0])
    primary_pit_hook = f"_pit{primary_hook['name']}"
    
    # Build exclusion list
    exclude_columns = []
    for hook in bag['hooks']:
        exclude_columns.append(hook['name'])
    
    sql = f"""MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain ({primary_pit_hook})
);

SELECT
  *
  EXCLUDE ({', '.join(exclude_columns)})
FROM {hook_schema}.{bag_name}"""
    
    with open(sql_path, 'w') as file:
        file.write(sql)
    
    return True