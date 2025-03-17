import time
from generate_raw_views import generate_raw_views
from generate_hook_bags import generate_hook_bags
from generate_bridges import generate_bridges
from generate_event_models import generate_event_models
from generate_bridge_union import generate_bridge_union
from generate_peripherals import generate_peripherals

def extract_schema_name(path):
    """Extract schema name from a path (e.g., './models/dar' -> 'dar')"""
    # Remove trailing slashes and split by directory separator
    normalized_path = path.rstrip('/')
    parts = normalized_path.split('/')
    # Return the last part of the path as the schema name
    if parts and parts[-1]:
        return parts[-1]
    return 'public'  # Default schema if none can be determined

def main(
    raw_dir='./models/das/', 
    hook_dir='./models/dab/',
    bridge_dir='./models/dar__staging/',
    events_dir='./models/dar__staging/',
    bridge_union_dir='./models/dar/',
    peripheral_dir='./models/dar/'
):
    """Generate all models for Adventure Works with configurable output paths"""
    print("Starting model generation...")
    total_start_time = time.time()
    n_models = 0
    
    # Extract schema names from directories
    raw_schema = extract_schema_name(raw_dir)
    hook_schema = extract_schema_name(hook_dir)
    bridge_schema = extract_schema_name(bridge_dir)
    events_schema = extract_schema_name(events_dir)
    
    # Step 1: Generate raw models
    print(f"\n=== Generating Raw Models in {raw_dir} ===")
    step_start = time.time()
    raw_count = generate_raw_views(output_dir=raw_dir)
    step_elapsed = time.time() - step_start
    print(f"Runtime: {step_elapsed:.2f} seconds")
    n_models += raw_count
    
    # Step 2: Generate hook models
    print(f"\n=== Generating Hook Models in {hook_dir} ===")
    step_start = time.time()
    hook_count = generate_hook_bags(output_dir=hook_dir, raw_schema=raw_schema)
    step_elapsed = time.time() - step_start
    print(f"Runtime: {step_elapsed:.2f} seconds")
    n_models += hook_count
    
    # Step 3: Generate bridge models
    print(f"\n=== Generating Bridge Models in {bridge_dir} ===")
    step_start = time.time()
    bridge_count = generate_bridges(
        output_dir=bridge_dir,
        hook_schema=hook_schema,
        bridge_schema=bridge_schema
    )
    step_elapsed = time.time() - step_start
    print(f"Runtime: {step_elapsed:.2f} seconds")
    n_models += bridge_count
    
    # Step 4: Generate event models
    print(f"\n=== Generating Event Models in {events_dir} ===")
    step_start = time.time()
    event_count = generate_event_models(
        output_dir=events_dir,
        bridge_schema=bridge_schema,
        hook_schema=hook_schema
    )
    step_elapsed = time.time() - step_start
    print(f"Runtime: {step_elapsed:.2f} seconds")
    n_models += event_count
    
    # Step 5: Generate bridge union
    print(f"\n=== Generating Bridge Union in {bridge_union_dir} ===")
    step_start = time.time()
    union_count = generate_bridge_union(
        output_dir=bridge_union_dir,
        bridge_schema=bridge_schema,
        events_schema=events_schema
    )
    step_elapsed = time.time() - step_start
    print(f"Runtime: {step_elapsed:.2f} seconds")
    n_models += union_count
    
    # Step 6: Generate peripheral models
    print(f"\n=== Generating Peripheral Models in {peripheral_dir} ===")
    step_start = time.time()
    peripheral_count = generate_peripherals(output_dir=peripheral_dir, hook_schema=hook_schema)
    step_elapsed = time.time() - step_start
    print(f"Runtime: {step_elapsed:.2f} seconds")
    n_models += peripheral_count
    
    total_elapsed = time.time() - total_start_time
    print(f"\nGenerated {n_models} models in {total_elapsed:.2f} seconds")
    print("✅ Model generation complete")

if __name__ == "__main__":
    main()