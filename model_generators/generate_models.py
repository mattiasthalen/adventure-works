import time
from generate_raw_views import generate_raw_views
from generate_hook_bags import generate_hook_bags
from generate_uss_bridges import generate_uss_bridges

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
    
    # Step 3: Generate USS bridges
    print("\n=== Generating USS Bridges ===")
    generate_uss_bridges()
    
    # Print summary
    elapsed_time = time.time() - start_time
    print(f"\nCompleted all model generation in {elapsed_time:.2f} seconds")
    print("✅ Model generation complete")

if __name__ == "__main__":
    main()