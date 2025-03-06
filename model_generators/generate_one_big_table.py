import os
from parse_yaml import load_bags_config, ensure_directory_exists

def generate_one_big_table():
    """Generate a single comprehensive table joining all peripherals to the bridge"""
    output_dir = './models/gold/'
    ensure_directory_exists(output_dir)
    
    # Load bag config to identify peripherals and their hooks
    bags_config = load_bags_config()
    
    # Build mapping from peripheral names to primary PIT hooks
    peripheral_to_pit_hook = {}
    for bag in bags_config['bags']:
        parts = bag['name'].split('__')
        if len(parts) >= 3:
            peripheral_name = parts[2]
            # Skip if we've already processed this peripheral
            if peripheral_name in peripheral_to_pit_hook:
                continue
                
            # Find primary hook
            primary_hook = next((h for h in bag['hooks'] if h.get('primary')), bag['hooks'][0])
            primary_pit_hook = f"_pit{primary_hook['name']}"
            peripheral_to_pit_hook[peripheral_name] = primary_pit_hook
    
    # Generate the SQL for as_is and as_of versions
    generate_one_big_table_sql(peripheral_to_pit_hook, "as_is", output_dir)
    generate_one_big_table_sql(peripheral_to_pit_hook, "as_of", output_dir)
    
    print(f"Generated 2 'one big table' views in {output_dir}")

def generate_one_big_table_sql(peripheral_to_pit_hook, temporal_type, output_dir):
    """Generate SQL for a specific one big table view (as_is or as_of)"""
    sql_path = os.path.join(output_dir, f"_one_big_table__{temporal_type}.sql")
    
    # Start building SQL
    sql = f"""MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss
);

SELECT
  *
FROM gold._bridge__{temporal_type}"""
    
    # Add JOINs for each peripheral
    for peripheral, pit_hook in peripheral_to_pit_hook.items():
        sql += f"\nLEFT JOIN gold.{peripheral} USING ({pit_hook})"
    
    # Write to file
    with open(sql_path, 'w') as file:
        file.write(sql)

if __name__ == "__main__":
    generate_one_big_table()