import os
import glob
import re
from parse_yaml import ensure_directory_exists

def generate_bridge_union(output_dir, bridge_schema, events_schema):
    """Generate a bridge union that contains ONLY event models with consistent column ordering"""
    ensure_directory_exists(output_dir)
    
    # Dynamically discover event models only (sorted for consistency)
    event_models = discover_models(events_schema, "events__*")
    
    # Collect PIT hooks, event fields, measure fields and their descriptions
    all_pit_hooks, all_event_fields, all_measure_fields, field_descriptions = collect_fields_from_models(events_schema, event_models)
    
    # Remove _pit_hook__bridge from the pit hooks to avoid duplication
    if "_pit_hook__bridge" in all_pit_hooks:
        all_pit_hooks.remove("_pit_hook__bridge")
    
    # Sort hooks and fields
    sorted_pit_hooks = sorted(list(all_pit_hooks))
    sorted_event_fields = sorted(list(all_event_fields))
    sorted_measure_fields = sorted(list(all_measure_fields))
    
    # Add epoch date hook
    all_hooks = sorted_pit_hooks.copy()
    all_hooks.append("_hook__epoch__date")
    
    # Generate the bridge union
    bridge_name = "_bridge__as_of"
    sql_path = os.path.join(output_dir, f"{bridge_name}.sql")
    
    # Create column descriptions, using existing descriptions when available
    column_descriptions = get_bridge_union_column_descriptions(sorted_pit_hooks, sorted_event_fields, sorted_measure_fields, field_descriptions)
    
    with open(sql_path, 'w') as sql_file:
        # Write MODEL declaration
        references = ', '.join(all_hooks)
        
        sql_file.write(f"""MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__bridge),
  references ({references}),
  description 'Unified viewpoint of all event data: Combined timeline of all business events in the Adventure Works dataset'""")
        
        # Add column descriptions
        if column_descriptions:
            sql_file.write(",\n  column_descriptions (\n")
            
            # Write descriptions in a specific order for readability
            # First static fields
            static_fields = ["peripheral", "_pit_hook__bridge"]
            for field in static_fields:
                if field in column_descriptions:
                    sql_file.write(f"    {field} = '{column_descriptions[field]}',\n")
                    
            # Then all pit hooks 
            for field in sorted_pit_hooks:
                if field in column_descriptions:
                    sql_file.write(f"    {field} = '{column_descriptions[field]}',\n")
                    
            # Then epoch hook
            if "_hook__epoch__date" in column_descriptions:
                sql_file.write(f"    _hook__epoch__date = '{column_descriptions['_hook__epoch__date']}',\n")
                
            # Then all event fields
            for field in sorted_event_fields:
                if field in column_descriptions:
                    sql_file.write(f"    {field} = '{column_descriptions[field]}',\n")
                    
            # Then all measure fields 
            for field in sorted_measure_fields:
                if field in column_descriptions:
                    sql_file.write(f"    {field} = '{column_descriptions[field]}',\n")
            
            # Finally validity fields
            validity_fields = ["bridge__record_loaded_at", "bridge__record_updated_at", 
                            "bridge__record_valid_from", "bridge__record_valid_to", 
                            "bridge__is_current_record"]
            for i, field in enumerate(validity_fields):
                if field in column_descriptions:
                    ending = ",\n" if i < len(validity_fields) - 1 else "\n"
                    sql_file.write(f"    {field} = '{column_descriptions[field]}'{ending}")
            
            sql_file.write("  )")
        
        sql_file.write("\n);\n\n")
        
        # Write UNION query with ONLY event models
        sql_file.write("WITH cte__bridge_union AS (\n")
        
        # Add all event models
        for i, event_model in enumerate(event_models):
            sql_file.write(f"  SELECT * FROM {events_schema}.{event_model}")
            if i < len(event_models) - 1:
                sql_file.write("\n  UNION ALL BY NAME\n")
            else:
                sql_file.write("\n")
        
        sql_file.write("),\n")
        
        # Add ghosting CTE to handle potentially missing columns
        sql_file.write("""cte__ghosting AS (
  SELECT
    COALESCE(COLUMNS(col -> col LIKE '%_hook__%'), 'ghost_record'),
    COLUMNS(col -> col NOT LIKE '%_hook__%')
  FROM cte__bridge_union
)\n""")
        
        # Write final SELECT with consistent column order
        sql_file.write("SELECT\n")
        sql_file.write("  peripheral::TEXT,\n")
        sql_file.write("  _pit_hook__bridge::BLOB,\n")
        
        # Include all PIT hooks in sorted order
        for pit_hook in sorted_pit_hooks:
            sql_file.write(f"  {pit_hook}::BLOB,\n")
        
        # Include epoch date hook
        sql_file.write("  _hook__epoch__date::BLOB,\n")
        
        # Include event fields in sorted order
        for event_field in sorted_event_fields:
            sql_file.write(f"  {event_field}::INT,\n")
        
        # Include measure fields in sorted order
        measure_fields_list = sorted_list = sorted(all_measure_fields)
        if measure_fields_list:
            for i, measure_field in enumerate(measure_fields_list):
                data_type = get_measure_data_type(measure_field)
                sql_file.write(f"  {measure_field}::{data_type}")
                if i < len(measure_fields_list) - 1:
                    sql_file.write(",\n")
                else:
                    sql_file.write(",\n")
        
        # Add temporal fields
        sql_file.write("""  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__ghosting""")
    
    print(f"Generated bridge union with {len(event_models)} events in {output_dir}")
    return 1  # Return count of models generated

def get_measure_data_type(measure_field):
    """Determine appropriate data type for measure fields based on naming convention"""
    if "_qty_" in measure_field:
        return "BIGINT"
    elif "_total_" in measure_field or "_amount_" in measure_field or "_value_" in measure_field:
        return "DECIMAL"
    elif measure_field.endswith("_flag") or measure_field.endswith("_indicator"):
        return "BOOL"
    else:
        return "INT"  # Default type for measures

def get_bridge_union_column_descriptions(pit_hooks, event_fields, measure_fields, field_descriptions=None):
    """Generate descriptions for bridge union columns, preferring existing descriptions when available"""
    descriptions = {}
    
    # Initialize with field_descriptions if provided
    if field_descriptions:
        descriptions.update(field_descriptions)
    
    # Describe standard columns
    descriptions["peripheral"] = "Name of the peripheral this record relates to"
    descriptions["_pit_hook__bridge"] = "Unique identifier for this bridge record"
    descriptions["_hook__epoch__date"] = "Hook to the date the event occurred"
    
    # Describe each PIT hook
    for pit_hook in pit_hooks:
        if pit_hook not in descriptions:
            # Extract concept from hook name
            parts = pit_hook.split('__')
            if len(parts) >= 2:
                concept = parts[1]
                if len(parts) >= 3:
                    qualifier = parts[2]
                    descriptions[pit_hook] = f"Point-in-time hook for {qualifier} {concept}"
                else:
                    descriptions[pit_hook] = f"Point-in-time hook for {concept}"
    
    # Describe event fields only if not already described
    for event_field in event_fields:
        if event_field not in descriptions:
            # Extract event type and entity from name
            parts = event_field.split('__')
            if len(parts) >= 3:
                event_type = parts[2]
                entity = parts[1]
                descriptions[event_field] = f"Flag indicating a {event_type} event for {entity}"
    
    # Describe measure fields only if not already described
    for measure_field in measure_fields:
        if measure_field not in descriptions:
            # Extract measure type and entity from name
            parts = measure_field.split('__')
            if len(parts) >= 3:
                measure_type = parts[2]
                entity = parts[1]
                if "_qty_" in measure_field:
                    descriptions[measure_field] = f"Quantity measure for {measure_type} in {entity}"
                elif "_total_" in measure_field:
                    descriptions[measure_field] = f"Total value measure for {measure_type} in {entity}"
                else:
                    descriptions[measure_field] = f"Measure of {measure_type} for {entity}"
    
    # Describe temporal fields
    descriptions["bridge__record_loaded_at"] = "Timestamp when this record was loaded"
    descriptions["bridge__record_updated_at"] = "Timestamp when this record was last updated"
    descriptions["bridge__record_valid_from"] = "Timestamp from which this record is valid"
    descriptions["bridge__record_valid_to"] = "Timestamp until which this record is valid"
    descriptions["bridge__is_current_record"] = "Flag indicating if this is the current valid version of the record"
    
    return descriptions

def discover_models(schema, pattern):
    """Discover models matching pattern in the given schema directory"""
    # In a real implementation, query the database catalog tables
    # For this implementation, we'll scan files in the directory
    models_dir = f"./models/{schema}/"
    sql_files = glob.glob(f"{models_dir}{pattern}.sql")
    
    # Extract model names from file paths and sort for consistency
    models = [os.path.basename(f).replace('.sql', '') for f in sql_files]
    
    return sorted(models)

def collect_fields_from_models(events_schema, event_models):
    """Parse model files to collect PIT hooks, event fields, and measure fields with descriptions"""
    all_pit_hooks = set()
    all_event_fields = set()
    all_measure_fields = set()
    all_field_descriptions = {}
    
    # Process event models to extract hooks and fields
    for event in event_models:
        pit_hooks, event_fields, measure_fields, field_descriptions = extract_fields_from_event_model(events_schema, event)
        all_pit_hooks.update(pit_hooks)
        all_event_fields.update(event_fields)
        all_measure_fields.update(measure_fields)
        all_field_descriptions.update(field_descriptions)
    
    return all_pit_hooks, all_event_fields, all_measure_fields, all_field_descriptions

def extract_fields_from_event_model(schema, model_name):
    """Extract PIT hooks, event fields, and measure fields from an event model file"""
    model_path = f"./models/{schema}/{model_name}.sql"
    pit_hooks = set()
    event_fields = set()
    measure_fields = set()
    field_descriptions = {}
    
    try:
        with open(model_path, 'r') as file:
            content = file.read()
            
            # Extract descriptions from column_descriptions section
            if "column_descriptions (" in content:
                desc_start = content.find("column_descriptions (") + len("column_descriptions (")
                desc_end = content.find(")", desc_start)
                if desc_end > desc_start:
                    desc_section = content[desc_start:desc_end]
                    # Match column name = 'description' pattern
                    desc_pattern = r'([a-zA-Z0-9_]+)\s*=\s*[\'"](.+?)[\'"]'
                    desc_matches = re.findall(desc_pattern, desc_section)
                    
                    for col_name, description in desc_matches:
                        col_name = col_name.strip()
                        description = description.strip()
                        if col_name.startswith(('event__', 'measure__')):
                            field_descriptions[col_name] = description
            
            # Find references section in model declaration
            references_section = None
            if "references (" in content:
                references_start = content.find("references (") + len("references (")
                references_end = content.find(")", references_start)
                if references_end > references_start:
                    references_section = content[references_start:references_end]
            
            # If we found references, extract PIT hooks
            if references_section:
                refs = [r.strip() for r in references_section.split(',')]
                for ref in refs:
                    # Look for any reference that starts with _pit (not just _pit_hook__)
                    if ref.startswith('_pit') and ref != "_pit_hook__bridge" and ref != "_hook__epoch__date":
                        pit_hooks.add(ref)
            
            # More robust approach to extract fields from SELECT statement
            # Find the final SELECT statement
            select_parts = content.split('SELECT')
            if len(select_parts) > 1:
                last_select = select_parts[-1]
                from_parts = last_select.split('FROM')
                if len(from_parts) > 0:
                    select_section = from_parts[0]
                    
                    # Extract all column definitions by looking for ::TYPE patterns
                    col_pattern = r'([a-zA-Z0-9_]+)::(BLOB|INT|BIGINT|DECIMAL|DOUBLE|FLOAT|TEXT|BOOL|BOOLEAN)'
                    matches = re.findall(col_pattern, select_section)
                    
                    for col_name, col_type in matches:
                        col_name = col_name.strip()
                        if col_name.startswith('_pit') and col_name != "_pit_hook__bridge":
                            pit_hooks.add(col_name)
                        elif col_name.startswith('event__'):
                            event_fields.add(col_name)
                        elif col_name.startswith('measure__'):
                            measure_fields.add(col_name)
    except Exception as e:
        print(f"Error parsing model {model_name}: {e}")
    
    return pit_hooks, event_fields, measure_fields, field_descriptions