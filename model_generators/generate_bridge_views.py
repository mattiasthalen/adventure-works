import os
from parse_yaml import load_bags_config, ensure_directory_exists

def generate_bridge_views():
    """Generate bridge views in the gold layer"""
    output_dir = './models/gold/'
    ensure_directory_exists(output_dir)
    
    # Load bag config to get all hooks for references
    bags_config = load_bags_config()
    all_hooks = []
    for bag in bags_config['bags']:
        for hook in bag['hooks']:
            hook_name = hook['name']
            if hook_name not in all_hooks:
                all_hooks.append(hook_name)
    
    generate_as_of_view(output_dir, all_hooks)
    #generate_as_is_view(output_dir, all_hooks)
    
    print(f"Generated 2 bridge views in {output_dir}")

def generate_as_of_view(output_dir, all_hooks):
    """Generate the as_of bridge view"""
    sql_path = os.path.join(output_dir, "_bridge__as_of.sql")
    
    # Create references list excluding bridge hook
    references = [h for h in all_hooks if h != "_pit_hook__bridge"]
    references_str = ", ".join(references)
    
    sql = f"""MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__bridge),
  references ({references_str})
);

SELECT
  *
FROM silver.uss_bridge"""
    
    with open(sql_path, 'w') as file:
        file.write(sql)
    
    return True

def generate_as_is_view(output_dir, all_hooks):
    """Generate the as_is bridge view"""
    sql_path = os.path.join(output_dir, "_bridge__as_is.sql")
    
    # Create references list excluding bridge hook
    references = [h for h in all_hooks if h != "_pit_hook__bridge"]
    references_str = ", ".join(references)
    
    sql = f"""MODEL (
  enabled TRUE,
  kind VIEW,
  tags uss,
  grain (_pit_hook__bridge),
  references ({references_str})
);

SELECT
  *
FROM silver.uss_bridge
WHERE
  bridge__is_current_record = TRUE"""
    
    with open(sql_path, 'w') as file:
        file.write(sql)
    
    return True

if __name__ == "__main__":
    generate_bridge_views()