import time
from generate_raw_views import generate_raw_views
from generate_hook_bags import generate_hook_bags
from generate_measure_models import generate_measure_models
from generate_bridges import generate_bridges
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
    measure_dir='./models/dar__staging/',
    bridge_dir='./models/dar__staging/',
    unified_bridge_dir='./models/dar/',
    peripheral_dir='./models/dar/'
):
    """Generate all models for Adventure Works with configurable output paths"""
    print("Starting model generation...")
    start_time = time.time()
    n_models = 0
    
    # Extract schema names from directories
    raw_schema = extract_schema_name(raw_dir)
    hook_schema = extract_schema_name(hook_dir)
    measure_schema = extract_schema_name(measure_dir)
    bridge_schema = extract_schema_name(bridge_dir)
    peripheral_schema = extract_schema_name(peripheral_dir)
    
    # Step 1: Generate raw models
    print(f"\n=== Generating Raw Models in {raw_dir} (schema: {raw_schema}) ===")
    n_models += generate_raw_views(output_dir=raw_dir)
    
    # Step 2: Generate hook models
    print(f"\n=== Generating Hook Models in {hook_dir} (schema: {hook_schema}) ===")
    n_models += generate_hook_bags(output_dir=hook_dir, raw_schema=raw_schema)
    
    # Step 3: Generate measure models
    print(f"\n=== Generating Measure Models in {measure_dir} (schema: {measure_schema}) ===")
    n_models += generate_measure_models(output_dir=measure_dir, hook_schema=hook_schema)
    
    # Step 4: Generate bridge models
    print(f"\n=== Generating Bridge Models in {bridge_dir} and {unified_bridge_dir} ===")
    bridge_count, unified_count = generate_bridges(
        output_dir=bridge_dir, 
        secondary_output_dir=unified_bridge_dir,
        hook_schema=hook_schema,
        measure_schema=measure_schema,
        bridge_schema=bridge_schema
    )
    n_models += bridge_count + unified_count
    
    # Step 5: Generate peripheral models
    print(f"\n=== Generating Peripheral Models in {peripheral_dir} (schema: {peripheral_schema}) ===")
    n_models += generate_peripherals(output_dir=peripheral_dir, hook_schema=hook_schema)
    
    elapsed_time = time.time() - start_time
    print(f"\nGenerated {n_models} models in {elapsed_time:.2f} seconds")
    print("✅ Model generation complete")

if __name__ == "__main__":
    main()