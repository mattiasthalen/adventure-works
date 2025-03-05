import time
from generate_raw_models import generate_raw_models
from generate_hook_models import generate_hook_models

def main():
    """Generate all models for Adventure Works"""
    print("Starting model generation...")
    start_time = time.time()
    
    # Step 1: Generate raw models
    print("\n=== Generating Raw Models ===")
    generate_raw_models()
    
    # Step 2: Generate hook models
    print("\n=== Generating Hook Models ===")
    generate_hook_models()
    
    # Print summary
    elapsed_time = time.time() - start_time
    print(f"\nCompleted all model generation in {elapsed_time:.2f} seconds")
    print("✅ Model generation complete")

if __name__ == "__main__":
    main()