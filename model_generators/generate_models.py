import time
import argparse
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
    peripheral_dir='./models/dar/',
    generate_only=None
):
    """Generate all models for Adventure Works with configurable output paths
    
    Parameters:
    -----------
    raw_dir : str
        Directory for raw views
    hook_dir : str
        Directory for hook models
    bridge_dir : str
        Directory for bridge models
    events_dir : str
        Directory for event models
    bridge_union_dir : str
        Directory for bridge union model
    peripheral_dir : str
        Directory for peripheral models
    generate_only : str or None
        If specified, only generate this type of model.
        Valid values: 'raw', 'hook', 'bridge', 'event', 'bridge_union', 'peripheral'
    """
    print("Starting model generation...")
    total_start_time = time.time()
    n_models = 0
    
    # Extract schema names from directories
    raw_schema = extract_schema_name(raw_dir)
    hook_schema = extract_schema_name(hook_dir)
    bridge_schema = extract_schema_name(bridge_dir)
    events_schema = extract_schema_name(events_dir)
    
    # Run the selected step or all steps if none specified
    if generate_only is None or generate_only == 'raw':
        # Step 1: Generate raw models
        print(f"\n=== Generating Raw Models in {raw_dir} ===")
        step_start = time.time()
        raw_count = generate_raw_views(output_dir=raw_dir)
        step_elapsed = time.time() - step_start
        print(f"Runtime: {step_elapsed:.2f} seconds")
        n_models += raw_count
        
        if generate_only == 'raw':
            total_elapsed = time.time() - total_start_time
            print(f"\nGenerated {n_models} models in {total_elapsed:.2f} seconds")
            print("✅ Raw model generation complete")
            return
    
    if generate_only is None or generate_only == 'hook':
        # Step 2: Generate hook models
        print(f"\n=== Generating Hook Models in {hook_dir} ===")
        step_start = time.time()
        hook_count = generate_hook_bags(output_dir=hook_dir, raw_schema=raw_schema)
        step_elapsed = time.time() - step_start
        print(f"Runtime: {step_elapsed:.2f} seconds")
        n_models += hook_count
        
        if generate_only == 'hook':
            total_elapsed = time.time() - total_start_time
            print(f"\nGenerated {n_models} models in {total_elapsed:.2f} seconds")
            print("✅ Hook model generation complete")
            return
    
    if generate_only is None or generate_only == 'bridge':
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
        
        if generate_only == 'bridge':
            total_elapsed = time.time() - total_start_time
            print(f"\nGenerated {n_models} models in {total_elapsed:.2f} seconds")
            print("✅ Bridge model generation complete")
            return
    
    if generate_only is None or generate_only == 'event':
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
        
        if generate_only == 'event':
            total_elapsed = time.time() - total_start_time
            print(f"\nGenerated {n_models} models in {total_elapsed:.2f} seconds")
            print("✅ Event model generation complete")
            return
    
    if generate_only is None or generate_only == 'bridge_union':
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
        
        if generate_only == 'bridge_union':
            total_elapsed = time.time() - total_start_time
            print(f"\nGenerated {n_models} models in {total_elapsed:.2f} seconds")
            print("✅ Bridge union generation complete")
            return
    
    if generate_only is None or generate_only == 'peripheral':
        # Step 6: Generate peripheral models
        print(f"\n=== Generating Peripheral Models in {peripheral_dir} ===")
        step_start = time.time()
        peripheral_count = generate_peripherals(output_dir=peripheral_dir, hook_schema=hook_schema)
        step_elapsed = time.time() - step_start
        print(f"Runtime: {step_elapsed:.2f} seconds")
        n_models += peripheral_count
        
        if generate_only == 'peripheral':
            total_elapsed = time.time() - total_start_time
            print(f"\nGenerated {n_models} models in {total_elapsed:.2f} seconds")
            print("✅ Peripheral model generation complete")
            return
    
    # Only reach here if generating everything
    total_elapsed = time.time() - total_start_time
    print(f"\nGenerated {n_models} models in {total_elapsed:.2f} seconds")
    print("✅ Model generation complete")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate data models for Adventure Works')
    parser.add_argument('--raw-dir', default='./models/das/', help='Directory for raw views')
    parser.add_argument('--hook-dir', default='./models/dab/', help='Directory for hook models')
    parser.add_argument('--bridge-dir', default='./models/dar__staging/', help='Directory for bridge models')
    parser.add_argument('--events-dir', default='./models/dar__staging/', help='Directory for event models')
    parser.add_argument('--bridge-union-dir', default='./models/dar/', help='Directory for bridge union model')
    parser.add_argument('--peripheral-dir', default='./models/dar/', help='Directory for peripheral models')
    parser.add_argument('--generate-only', choices=['raw', 'hook', 'bridge', 'event', 'bridge_union', 'peripheral'],
                        help='Only generate this type of model')
    
    args = parser.parse_args()
    
    main(
        raw_dir=args.raw_dir,
        hook_dir=args.hook_dir,
        bridge_dir=args.bridge_dir,
        events_dir=args.events_dir,
        bridge_union_dir=args.bridge_union_dir,
        peripheral_dir=args.peripheral_dir,
        generate_only=args.generate_only
    )