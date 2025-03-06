import time
from generate_raw_views import generate_raw_views
from generate_hook_bags import generate_hook_bags
from generate_bridges import generate_bridges
from generate_peripherals import generate_peripherals
from generate_bridge_views import generate_bridge_views
from generate_one_big_table import generate_one_big_table

def main():
    """Generate all models for Adventure Works"""
    print("Starting model generation...")
    start_time = time.time()
    
    # Step 1: Generate raw models
    print("\n=== Generating Raw Models ===")
    generate_raw_views()
    
    # Step 2: Generate hook models
    print("\n=== Generating Hook Models ===")
    generate_hook_bags()
    
    # Step 3: Generate bridge models
    print("\n=== Generating Bridge Models ===")
    generate_bridges()
    
    # Step 4: Generate peripheral models
    print("\n=== Generating Peripheral Models ===")
    generate_peripherals()
    
    # Step 5: Generate bridge views
    #print("\n=== Generating Bridge Views ===")
    #generate_bridge_views()
    
    # Step 6: Generate one big table views
    print("\n=== Generating One Big Table Views ===")
    generate_one_big_table()
    
    # Print summary
    elapsed_time = time.time() - start_time
    print(f"\nCompleted all model generation in {elapsed_time:.2f} seconds")
    print("✅ Model generation complete")

if __name__ == "__main__":
    main()