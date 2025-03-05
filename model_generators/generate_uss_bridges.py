import os
import yaml
from parse_yaml import load_bags_config, ensure_directory_exists

def generate_uss_bridges():
    """Generate USS bridge models based on configuration"""
    
    # Define output directory
    output_dir = './models/silver/'
    ensure_directory_exists(output_dir)
    
    # Load configurations
    bags_config = load_bags_config()
    
    # Load bridge configurations
    bridges_config = load_bridges_config()
    
    # Generate bridges
    count = 0
    for bridge_config in bridges_config.get('bridges', []):
        success = generate_uss_bridge(bridge_config, bags_config, output_dir)
        if success:
            count += 1
    
    print(f"Generated {count} USS bridges in {output_dir}")
    return count

def load_bridges_config(path='./hook/uss_bridges.yml'):
    """Load USS bridge configurations"""
    if not os.path.exists(path):
        # Create default configuration if not exists
        bridges_config = {
            'bridges': [
                {
                    'name': 'uss_bridge__products',
                    'stage': 'products',
                    'primary_table': 'bag__adventure_works__products',
                    'joins': [
                        {
                            'table': 'bag__adventure_works__product_subcategories',
                            'on': '_hook__product_subcategory',
                            'joins': [
                                {
                                    'table': 'bag__adventure_works__product_categories',
                                    'on': '_hook__product_category'
                                }
                            ]
                        }
                    ]
                },
                {
                    'name': 'uss_bridge__orders',
                    'stage': 'orders',
                    'primary_table': 'bag__adventure_works__sales_order_headers',
                    'joins': [
                        {
                            'table': 'bag__adventure_works__customers',
                            'on': '_hook__customer'
                        },
                        {
                            'table': 'bag__adventure_works__addresses',
                            'on': '_hook__address__billing',
                            'qualifier': 'billing'
                        },
                        {
                            'table': 'bag__adventure_works__addresses',
                            'on': '_hook__address__shipping',
                            'qualifier': 'shipping'
                        }
                    ]
                }
            ]
        }
        
        with open(path, 'w') as f:
            yaml.dump(bridges_config, f, default_flow_style=False)
        
        print(f"Created default USS bridges configuration at {path}")
        return bridges_config
    
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def generate_uss_bridge(bridge_config, bags_config, output_dir):
    """Generate a USS bridge model from configuration"""
    bridge_name = bridge_config['name']
    stage = bridge_config['stage']
    primary_table = bridge_config['primary_table']
    
    # Find primary table configuration
    primary_bag_config = next((b for b in bags_config['bags'] if b['name'] == primary_table), None)
    if not primary_bag_config:
        print(f"Primary table {primary_table} not found in bags configuration")
        return False
    
    # Get primary hook and columns prefix
    primary_hook = next((h for h in primary_bag_config['hooks'] if h.get('primary')), 
                        primary_bag_config['hooks'][0])
    primary_hook_name = primary_hook['name']
    
    # Collect tables, hooks, and generate join SQL
    tables = [primary_table]
    table_aliases = {primary_table: primary_table}
    pit_hooks = {primary_table: f"_pit{primary_hook_name}"}
    referenced_pit_hooks = []
    join_sql_parts = []
    
    def collect_tables_and_hooks(joins, parent_table, level=0):
        for join in joins:
            table = join['table']
            on_hook = join['on']
            qualifier = join.get('qualifier', '')
            
            # Create unique identifier for this table instance
            table_id = f"{table}_{qualifier}" if qualifier else table
            
            # Add table if not already included
            if table_id not in tables:
                tables.append(table_id)
                table_aliases[table_id] = table
            
            # Find bag configuration
            bag_config = next((b for b in bags_config['bags'] if b['name'] == table), None)
            if not bag_config:
                print(f"Table {table} not found in bags configuration")
                continue
            
            # Get column prefix
            table_prefix = bag_config['column_prefix']
            
            # Find primary hook
            hook = next((h for h in bag_config['hooks'] if h.get('primary')), bag_config['hooks'][0])
            base_pit_hook = f"_pit{hook['name']}"
            
            # Add qualifier if present
            pit_hook = f"{base_pit_hook}__{qualifier}" if qualifier else base_pit_hook
            
            pit_hooks[table_id] = pit_hook
            if pit_hook != f"_pit{primary_hook_name}":
                referenced_pit_hooks.append(pit_hook)
            
            # Generate join SQL
            parent_actual = table_aliases[parent_table]
            parent_config = next((b for b in bags_config['bags'] if b['name'] == parent_actual), None)
            if not parent_config:
                continue
            
            parent_prefix = parent_config['column_prefix']
            indent = "  " * (level + 1)
            
            join_sql = f"""{indent}LEFT JOIN silver.{table} AS {table_id}
{indent}  ON {parent_table}.{on_hook} = {table_id}.{on_hook}
{indent}  AND {parent_table}.{parent_prefix}__record_valid_from <= {table_id}.{table_prefix}__record_valid_to
{indent}  AND {parent_table}.{parent_prefix}__record_valid_to >= {table_id}.{table_prefix}__record_valid_from"""
            
            join_sql_parts.append(join_sql)
            
            # Process nested joins
            if 'joins' in join:
                collect_tables_and_hooks(join['joins'], table_id, level + 1)
    
    # Collect tables, hooks, and joins
    if 'joins' in bridge_config:
        collect_tables_and_hooks(bridge_config['joins'], primary_table)
    
    # Start building SQL
    sql = f"""MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column bridge__record_updated_at
  ),
  tags uss,
  grain (_pit{primary_hook_name}),
  references ({', '.join(referenced_pit_hooks)})
);

WITH bridge AS (
  SELECT
    '{stage}' AS stage,
    {primary_table}._pit{primary_hook_name},
"""
    
    # Add pit hooks for joined tables
    for table_id in tables[1:]:
        actual_table = table_aliases[table_id]
        pit_hook = pit_hooks[table_id]
        
        # If table_id has a qualifier, alias the pit hook
        if table_id != actual_table:
            base_hook = '_pit' + pit_hook.split('_pit')[1].split('__')[0]
            sql += f"    {table_id}.{base_hook} AS {pit_hook},\n"
        else:
            sql += f"    {table_id}.{pit_hook},\n"
    
    # Add record timing fields
    loaded_at_fields = []
    updated_at_fields = []
    valid_from_fields = []
    valid_to_fields = []
    
    for table_id in tables:
        actual_table = table_aliases[table_id]
        bag_config = next((b for b in bags_config['bags'] if b['name'] == actual_table), None)
        if not bag_config:
            continue
        
        prefix = bag_config['column_prefix']
        loaded_at_fields.append(f"{table_id}.{prefix}__record_loaded_at")
        updated_at_fields.append(f"{table_id}.{prefix}__record_updated_at")
        valid_from_fields.append(f"{table_id}.{prefix}__record_valid_from")
        valid_to_fields.append(f"{table_id}.{prefix}__record_valid_to")
    
    sql += f"""    GREATEST(
      {',\n      '.join(loaded_at_fields)}
    ) AS bridge__record_loaded_at,
    GREATEST(
      {',\n      '.join(updated_at_fields)}
    ) AS bridge__record_updated_at,
    GREATEST(
      {',\n      '.join(valid_from_fields)}
    ) AS bridge__record_valid_from,
    LEAST(
      {',\n      '.join(valid_to_fields)}
    ) AS bridge__record_valid_to
  FROM silver.{primary_table}
"""
    
    # Add joins
    sql += "\n".join(join_sql_parts)
    
    # Add final CTE and SELECT
    sql += f"""\n), final AS (
  SELECT
    *,
    bridge__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current_record
  FROM bridge
)

SELECT
  stage::TEXT,
  _pit{primary_hook_name}::BLOB,
"""
    
    # Add pit hooks for joined tables to SELECT
    for pit_hook in referenced_pit_hooks:
        sql += f"  {pit_hook}::BLOB,\n"
    
    # Add record timing fields to SELECT
    sql += """  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::TEXT
FROM final
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts"""
    
    # Write to file
    output_path = os.path.join(output_dir, f"{bridge_name}.sql")
    with open(output_path, 'w') as f:
        f.write(sql)
    
    return True

if __name__ == "__main__":
    generate_uss_bridges()