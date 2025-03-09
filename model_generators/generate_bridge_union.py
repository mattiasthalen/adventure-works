import os
import glob
from parse_yaml import load_bags_config, ensure_directory_exists

def generate_bridge_union(output_dir, bridge_schema, events_schema):
    """Generate a bridge union that contains ONLY event models"""
    ensure_directory_exists(output_dir)
    
    # Dynamically discover event models only
    event_models = discover_models(events_schema, "events__*")
    
    # Collect PIT hooks and event fields
    all_pit_hooks, all_event_fields = collect_fields_from_models(events_schema, event_models)
    
    # Remove _pit_hook__bridge from the pit hooks to avoid duplication
    if "_pit_hook__bridge" in all_pit_hooks:
        all_pit_hooks.remove("_pit_hook__bridge")
    
    # Add epoch date hook
    all_hooks = list(sorted(all_pit_hooks))
    all_hooks.append("_hook__epoch__date")
    
    # Generate the bridge union
    bridge_name = "_bridge__as_of"
    sql_path = os.path.join(output_dir, f"{bridge_name}.sql")
    
    with open(sql_path, 'w') as sql_file:
        # Write MODEL declaration
        references = ', '.join(all_hooks)
        
        sql_file.write(f"""MODEL (
  enabled TRUE,
  kind VIEW,
  tags unified_star_schema,
  grain (_pit_hook__bridge),
  references ({references})
);

""")
        
        # Write UNION query with ONLY event models
        sql_file.write("WITH cte__bridge_union AS (\n")
        
        # Add all event models
        for i, event_model in enumerate(event_models):
            sql_file.write(f"  SELECT * FROM {events_schema}.{event_model}")
            if i < len(event_models) - 1:
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
        
        # Include epoch date hook
        sql_file.write("  _hook__epoch__date::BLOB,\n")
        
        # Include event fields
        for i, event_field in enumerate(sorted(all_event_fields)):
            sql_file.write(f"  {event_field}::INT")
            if i < len(all_event_fields) - 1:
                sql_file.write(",\n")
            else:
                sql_file.write(",\n")
        
        # Add temporal fields
        sql_file.write("""  bridge__record_loaded_at::TIMESTAMP,
  bridge__record_updated_at::TIMESTAMP,
  bridge__record_valid_from::TIMESTAMP,
  bridge__record_valid_to::TIMESTAMP,
  bridge__is_current_record::BOOL
FROM cte__bridge_union""")
    
    print(f"Generated bridge union with {len(event_models)} events in {output_dir}")
    return 1  # Return count of models generated

def discover_models(schema, pattern):
    """Discover models matching pattern in the given schema directory"""
    # In a real implementation, query the database catalog tables
    # For this implementation, we'll scan files in the directory
    models_dir = f"./models/{schema}/"
    sql_files = glob.glob(f"{models_dir}{pattern}.sql")
    
    # Extract model names from file paths
    models = [os.path.basename(f).replace('.sql', '') for f in sql_files]
    
    return sorted(models)

def collect_fields_from_models(events_schema, event_models):
    """Parse model files to collect PIT hooks and event fields"""
    all_pit_hooks = set()
    all_event_fields = set()
    
    # Process event models to extract hooks and fields
    for event in event_models:
        pit_hooks, event_fields = extract_fields_from_event_model(events_schema, event)
        all_pit_hooks.update(pit_hooks)
        all_event_fields.update(event_fields)
    
    return all_pit_hooks, all_event_fields

def extract_fields_from_event_model(schema, model_name):
    """Extract both PIT hooks and event fields from an event model file"""
    model_path = f"./models/{schema}/{model_name}.sql"
    pit_hooks = set()
    event_fields = set()
    
    try:
        with open(model_path, 'r') as file:
            content = file.read()
            
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
            
            # Find event fields in final SELECT statement
            select_section = content.split('SELECT')[-1].split('FROM')[0]
            for line in select_section.splitlines():
                line = line.strip()
                if '::BLOB' in line:
                    column = line.split('::')[0].strip()
                    # Capture any PIT hook references in the SELECT statement
                    if column.startswith('_pit') and column != "_pit_hook__bridge":
                        pit_hooks.add(column)
                elif '::INT' in line:
                    column = line.split('::')[0].strip()
                    if column.startswith('event__'):
                        event_fields.add(column)
    except Exception as e:
        print(f"Error parsing model {model_name}: {e}")
    
    return pit_hooks, event_fields