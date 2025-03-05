import os
from parse_yaml import load_schema, load_bags_config, ensure_directory_exists

def generate_uss_bridges():
    """Generate USS bridge model SQL files based on bags configuration"""
    # Define output directory
    output_dir = './models/silver/'
    
    # Ensure output directory exists
    ensure_directory_exists(output_dir)
    
    # Load YAML files
    bags_config = load_bags_config()
    
    # Counter for generated models
    count = 0
    
    # Process each bag
    for bag in bags_config['bags']:
        success = generate_uss_bridge_for_bag(bag, output_dir)
        if success:
            count += 1
    
    print(f"Generated {count} USS bridge models in {output_dir} out of {len(bags_config['bags'])} bags")

def generate_uss_bridge_for_bag(bag, output_dir):
    """Generate a USS bridge model SQL file for a specific bag"""
    bag_name = bag['name']
    column_prefix = bag['column_prefix']
    hooks = bag['hooks']
    
    # Skip if no hooks
    if not hooks:
        return False
    
    # Get primary hook
    primary_hook = next((h for h in hooks if h.get('primary')), hooks[0])
    primary_hook_name = primary_hook['name']
    
    # Get reference hooks
    reference_hooks = [h['name'] for h in hooks if h != primary_hook]
    
    # Generate bridges for all bags, even those with only a primary hook
    
    # Generate SQL file
    bridge_name = 'uss_bridge__' + bag_name.replace('bag__adventure_works__', '')
    sql_path = os.path.join(output_dir, f"{bridge_name}.sql")
    
    with open(sql_path, 'w') as sql_file:
        # MODEL declaration
        sql_file.write(f"""MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit{primary_hook_name})""")
        
        # Add references only if there are any
        if reference_hooks:
            sql_file.write(f",\n  references ({', '.join(reference_hooks)})")
        
        sql_file.write("\n);\n\n")
        
        # SELECT statement
        sql_file.write(f"SELECT\n")
        
        # Peripheral name
        table_name = bag_name.replace('bag__adventure_works__', '')
        sql_file.write(f"  '{table_name}' AS peripheral,\n")
        
        # Hook columns
        for hook in hooks:
            sql_file.write(f"  {hook['name']}::BLOB,\n")
        
        # Add system columns with standardized bridge column names
        sql_file.write(f"""  {column_prefix}__record_loaded_at::TIMESTAMP AS bridge__record_loaded_at,
  {column_prefix}__record_updated_at::TIMESTAMP AS bridge__record_updated_at,
  {column_prefix}__record_version::TEXT AS bridge__record_version,
  {column_prefix}__record_valid_from::TIMESTAMP AS bridge__record_valid_from,
  {column_prefix}__record_valid_to::TIMESTAMP AS bridge__record_valid_to,
  {column_prefix}__is_current_record::TEXT AS bridge__is_current_record
FROM silver.{bag_name}
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts""")
    
    return True

if __name__ == "__main__":
    generate_uss_bridges()