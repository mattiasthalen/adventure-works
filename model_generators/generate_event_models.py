import os
from parse_yaml import load_bags_config, load_schema, ensure_directory_exists

def generate_event_models(output_dir, bridge_schema, hook_schema):
    """Generate event models for all bags with at least modified_date"""
    ensure_directory_exists(output_dir)
    
    bags_config = load_bags_config()
    schema = load_schema()
    count = 0
    
    for bag in bags_config['bags']:
        # Find date fields in source table
        date_fields = find_date_fields(bag, schema)
        
        # Add common timestamp field if no date fields found
        if not date_fields:
            date_fields = ["modified_date"]
        
        # Generate event model for the bag
        success = generate_event_model(bag, date_fields, output_dir, bridge_schema, hook_schema)
        if success:
            count += 1
    
    print(f"Generated {count} event models in {output_dir}")
    return count

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

def map_date_to_event_name(field, entity_name, column_prefix):
    """Map a date field to an appropriate event name"""
    prefixed_field = f"{column_prefix}__{field}"
    field_base = field.lower()
    event_name = f"event__{entity_name}"
    
    # Create specific event names to avoid duplicates
    if 'scheduled_start' in field_base:
        event_name += '_scheduled_started'
    elif 'scheduled_end' in field_base:
        event_name += '_scheduled_ended'
    elif 'actual_start' in field_base:
        event_name += '_actual_started'
    elif 'actual_end' in field_base:
        event_name += '_actual_ended'
    elif 'order_date' in field_base:
        event_name += '_placed'
    elif 'due_date' in field_base:
        event_name += '_due'
    elif 'ship_date' in field_base:
        event_name += '_shipped'
    elif 'modified_date' in field_base:
        event_name += '_modified'
    elif 'start_date' in field_base:
        event_name += '_started'
    elif 'end_date' in field_base:
        event_name += '_ended'
    elif 'created' in field_base:
        event_name += '_created'
    elif 'updated' in field_base:
        event_name += '_updated'
    else:
        event_name += f'_{field_base.replace("_date", "")}'
    
    return prefixed_field, event_name

def extract_hooks_from_bridge(entity_name, bridge_schema, bridge_name):
    """Extract all hooks from an existing bridge model SQL file"""
    bridge_path = f"./models/{bridge_schema}/{bridge_name}.sql"
    hooks = []
    
    try:
        if os.path.exists(bridge_path):
            with open(bridge_path, 'r') as file:
                content = file.read()
                
                # Find references section
                if "references (" in content:
                    references_start = content.find("references (") + len("references (")
                    references_end = content.find(")", references_start)
                    if references_end > references_start:
                        references = content[references_start:references_end].strip()
                        hooks = [r.strip() for r in references.split(',')]
                        
                        # Filter to only _pit hooks that aren't the bridge itself
                        hooks = [h for h in hooks if h.startswith('_pit') and h != '_pit_hook__bridge']
    except Exception as e:
        print(f"Error extracting hooks from bridge {bridge_name}: {e}")
    
    return hooks

def get_hooks_from_bag(bag):
    """Extract all hook names from a bag, prefixed with _pit"""
    hooks = []
    
    # Primary hook first
    primary_hook = next((h for h in bag['hooks'] if h.get('primary')), bag['hooks'][0])
    primary_hook_name = primary_hook['name']
    hooks.append(f"_pit{primary_hook_name}")
    
    # Then non-primary hooks
    for hook in bag['hooks']:
        if hook != primary_hook:
            hooks.append(f"_pit{hook['name']}")
    
    return hooks

def generate_model_declaration(reference_hooks, primary_hook_name):
    """Generate the MODEL declaration section"""
    return f"""MODEL (
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _pit_hook__bridge
  ),
  tags event,
  grain (_pit_hook__bridge),
  references (
    {',\n    '.join(reference_hooks)},
    _hook__epoch__date
  )
);
"""

def generate_bridge_cte(reference_hooks, bridge_schema, bridge_name):
    """Generate the bridge CTE section"""
    return f"""WITH cte__bridge AS (
  SELECT
    peripheral,
    _pit_hook__bridge,
    {',\n    '.join(reference_hooks)},
    bridge__record_loaded_at,
    bridge__record_updated_at,
    bridge__record_valid_from,
    bridge__record_valid_to,
    bridge__is_current_record
  FROM {bridge_schema}.{bridge_name}
),
"""

def generate_events_cte(primary_hook_name, event_mappings, hook_schema, bag_name):
    """Generate the events CTE section"""
    events_cte = f"""cte__events AS (
  SELECT
    pivot__events._pit{primary_hook_name},
    CONCAT('epoch__date|', pivot__events.event_date) AS _hook__epoch__date,
"""
    
    # Add event mappings
    for i, (field, event_name) in enumerate(event_mappings):
        events_cte += f"    MAX(CASE WHEN pivot__events.event = '{field}' THEN 1 END) AS {event_name}"
        if i < len(event_mappings) - 1:
            events_cte += ",\n"
        else:
            events_cte += "\n"
    
    # Add UNPIVOT and GROUP BY
    events_cte += f"""  FROM {hook_schema}.{bag_name}
  UNPIVOT (
    event_date FOR event IN (
      {',\n      '.join([field for field, _ in event_mappings])}
    )
  ) AS pivot__events
  GROUP BY ALL
  ORDER BY _hook__epoch__date
),
"""
    return events_cte

def generate_final_cte(primary_hook_name):
    """Generate the final CTE section"""
    return """final AS (
  SELECT
    *,
    CONCAT_WS(
      '~',
      _pit_hook__bridge::TEXT,
      _hook__epoch__date::TEXT
    ) AS _pit_hook__bridge
  FROM cte__bridge
  LEFT JOIN cte__events USING(_pit"""+ primary_hook_name + """)
)
"""

def generate_final_select(reference_hooks, event_mappings):
    """Generate the final SELECT statement"""
    select_statement = """SELECT
  peripheral::TEXT,
  _pit_hook__bridge::BLOB,
  """
  
    # Add reference hooks  
    select_statement += f"{',\n  '.join([h + '::BLOB' for h in reference_hooks])},\n"
    select_statement += "  _hook__epoch__date::BLOB,\n"
    
    # Add event fields
    for i, (_, event_name) in enumerate(event_mappings):
        select_statement += f"  {event_name}::INT"
        if i < len(event_mappings) - 1:
            select_statement += ",\n"
        else:
            select_statement += ",\n"
    
    # Add time fields
    select_statement += """  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM final
WHERE 1 = 1
AND bridge__record_updated_at BETWEEN @start_ts AND @end_ts"""
    
    return select_statement

def generate_event_model(bag, date_fields, output_dir, bridge_schema, hook_schema):
    """Generate an event model SQL file for a specific entity"""
    bag_name = bag['name']
    if not bag_name.startswith('bag__adventure_works__'):
        return False
        
    # Extract entity name from bag name
    entity_name = bag_name.replace('bag__adventure_works__', '')
    event_model_name = f"events__{entity_name}"
    bridge_name = f"bridge__{entity_name}"
    column_prefix = bag['column_prefix']
    
    # Get primary hook
    primary_hook = next((h for h in bag['hooks'] if h.get('primary')), bag['hooks'][0])
    primary_hook_name = primary_hook['name']
    
    # Create SQL file path
    sql_path = os.path.join(output_dir, f"{event_model_name}.sql")
    
    # Map date fields to event names
    event_mappings = [map_date_to_event_name(field, entity_name, column_prefix) for field in date_fields]
    
    # Get reference hooks for this entity
    reference_hooks = extract_hooks_from_bridge(entity_name, bridge_schema, bridge_name)
    if not reference_hooks:
        reference_hooks = get_hooks_from_bag(bag)
    
    # Generate SQL sections
    model_declaration = generate_model_declaration(reference_hooks, primary_hook_name)
    bridge_cte = generate_bridge_cte(reference_hooks, bridge_schema, bridge_name)
    events_cte = generate_events_cte(primary_hook_name, event_mappings, hook_schema, bag_name)
    final_cte = generate_final_cte(primary_hook_name)
    final_select = generate_final_select(reference_hooks, event_mappings)
    
    # Combine all SQL sections
    sql_content = model_declaration + "\n" + bridge_cte + events_cte + final_cte + final_select
    
    # Write to file
    with open(sql_path, 'w') as file:
        file.write(sql_content)
    
    return True