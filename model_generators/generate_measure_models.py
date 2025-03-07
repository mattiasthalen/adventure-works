import os
from parse_yaml import load_bags_config, load_schema, ensure_directory_exists

def generate_measure_models():
    """Generate measure models for bags with date fields"""
    output_dir = './models/silver/'
    ensure_directory_exists(output_dir)
    
    bags_config = load_bags_config()
    schema = load_schema()
    
    count = 0
    for bag in bags_config['bags']:
        if generate_measure_model_for_bag(bag, schema, output_dir):
            count += 1
    
    print(f"Generated {count} measure models in {output_dir}")

def find_date_fields(bag, schema):
    """Find date fields in a bag's source table that are truly temporal"""
    source_table = bag['source_table']
    if source_table not in schema['tables']:
        return []
    
    columns = schema['tables'][source_table]['columns']
    
    date_fields = []
    for col_name, col_info in columns.items():
        if col_name.startswith('_dlt_'):
            continue
        
        data_type = col_info.get('data_type', '').lower()
        col_name_lower = col_name.lower()
        
        # Primarily rely on data type
        is_date_type = data_type in ['date', 'timestamp']
        
        # Check for specific temporal suffixes
        has_date_suffix = col_name_lower.endswith('_date') or col_name_lower.endswith('_at')
        
        if is_date_type or has_date_suffix:
            date_fields.append(col_name)
    
    return date_fields

def generate_measure_name(field_name, table_name):
    """Generate a descriptive measure name based on field and table"""
    clean_field = field_name.lower()
    
    # Remove date prefixes/suffixes
    for prefix in ['date_', 'dt_']:
        if clean_field.startswith(prefix):
            clean_field = clean_field[len(prefix):]
    
    for suffix in ['_date', '_dt']:
        if clean_field.endswith(suffix):
            clean_field = clean_field[:-len(suffix)]
    
    # Map common field names to standardized measures
    if clean_field in ['create', 'created']:
        return f"measure__{table_name}_created"
    elif clean_field in ['order', 'ordered']:
        return f"measure__{table_name}_placed"
    elif clean_field in ['ship', 'shipped']:
        return f"measure__{table_name}_shipped"
    elif clean_field in ['due']:
        return f"measure__{table_name}_due"
    elif clean_field in ['modified', 'update', 'updated']:
        return f"measure__{table_name}_modified"
    elif clean_field in ['start', 'started']:
        return f"measure__{table_name}_started"
    elif clean_field in ['end', 'ended', 'finish', 'finished']:
        return f"measure__{table_name}_finished"
    else:
        return f"measure__{table_name}_{clean_field}"

def generate_measure_model_for_bag(bag, schema, output_dir):
    """Generate a measure model for a specific bag if it has date fields"""
    bag_name = bag['name']
    
    if not bag_name.startswith('bag__adventure_works__'):
        return False
    
    #source_table = bag['source_table']
    column_prefix = bag['column_prefix']
    
    date_fields = find_date_fields(bag, schema)
    if not date_fields:
        return False
    
    primary_hook = next((h for h in bag['hooks'] if h.get('primary')), bag['hooks'][0])
    primary_hook_name = primary_hook['name']
    
    table_name = bag_name.replace('bag__adventure_works__', '')
    measure_model_name = f"measure__adventure_works__{table_name}"
    sql_path = os.path.join(output_dir, f"{measure_model_name}.sql")
    
    with open(sql_path, 'w') as sql_file:
        sql_file.write(f"""MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit{primary_hook_name},
    batch_size 288, -- cron every 5m: 24h * 60m / 5m = 288
  ),
  tags hook,
  grain (_pit{primary_hook_name}, _hook__epoch__date),
  references (_pit{primary_hook_name}, _hook__epoch__date)
);

WITH cte__source AS (
  SELECT
    _pit{primary_hook_name},
""")
        
        # Add date fields
        for i, field in enumerate(date_fields):
            sql_file.write(f"    {column_prefix}__{field}")
            if i < len(date_fields) - 1:
                sql_file.write(",")
            sql_file.write("\n")
        
        sql_file.write(f"""  FROM silver.{bag_name}
  WHERE 1 = 1
  AND {column_prefix}__record_updated_at BETWEEN @start_ts AND @end_ts
)""")
        
        # Generate CTEs for each date field
        cte_names = []
        for field in date_fields:
            measure_name = generate_measure_name(field, table_name)
            cte_name = f"cte__{field}"
            cte_names.append(cte_name)
            
            sql_file.write(f""", {cte_name} AS (
  SELECT
    _pit{primary_hook_name},
    {column_prefix}__{field}::DATE AS measure_date,
    1 AS {measure_name}
  FROM cte__source
  WHERE {column_prefix}__{field} IS NOT NULL
)""")
        
        # Combine measures with FULL OUTER JOINs
        sql_file.write(f""", cte__measures AS (
  SELECT
    *
  FROM {cte_names[0]}
""")
        
        for cte_name in cte_names[1:]:
            sql_file.write(f"""  FULL OUTER JOIN {cte_name} USING (_pit{primary_hook_name}, measure_date)
""")
        
        # Create epoch date hook
        sql_file.write("""), cte__epoch AS (
  SELECT
    *,
    CONCAT('epoch__date|', measure_date) AS _hook__epoch__date
  FROM cte__measures
)

SELECT
  _pit""" + primary_hook_name + """::BLOB,
  _hook__epoch__date::BLOB,
""")
        
        # Add measure fields to final SELECT
        measure_fields = []
        for field in date_fields:
            measure_name = generate_measure_name(field, table_name)
            measure_fields.append(f"  {measure_name}::INT")
        
        sql_file.write(",\n".join(measure_fields))
        sql_file.write("\nFROM cte__epoch")
    
    return True

if __name__ == "__main__":
    generate_measure_models()