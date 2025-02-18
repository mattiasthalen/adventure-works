import glob
import re

def generate_bridge_query():
    # Get all matching SQL files
    sql_files = glob.glob('./models/silver/uss_bridge__*.sql')
    
    # First, collect all pit hook fields from all files
    pit_hook_fields = set()
    
    # First pass - collect all fields
    for file_path in sql_files:
        with open(file_path, 'r') as f:
            content = f.read()
            select_match = re.search(r'SELECT\s+(.*?)\s+FROM', content, re.IGNORECASE | re.DOTALL)
            if select_match:
                fields = select_match.group(1)
                # Split fields and clean them
                field_matches = re.finditer(r'(?:AS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:,|$)', fields)
                for match in field_matches:
                    field_name = match.group(1).strip()
                    if field_name.startswith('_pit_hook__'):
                        pit_hook_fields.add(field_name)
    
    # Sort pit hook fields
    sorted_pit_hooks = sorted(pit_hook_fields)
    
    # Start building the query
    query_parts = [
        "MODEL (\n  kind VIEW\n);\n",
        "WITH bridge AS ("
    ]
    
    # Generate UNION ALL part
    for file_path in sql_files:
        table_name = re.search(r'uss_bridge__[^.]*', file_path).group(0)
        select_part = f"  SELECT\n    *\n  FROM silver.{table_name}"
        query_parts.append(select_part)
        if file_path != sql_files[-1]:  # If not the last file
            query_parts.append("  UNION ALL BY NAME")
    
    # Close the CTE and add the main query
    query_parts.extend([
        ")",
        "SELECT"
    ])
    
    # Add pit hook fields
    for field in sorted_pit_hooks:
        query_parts.append(f"  {field},")
    
    # Add bridge fields
    query_parts.extend([
        "  bridge__record_loaded_at,",
        "  bridge__record_updated_at,",
        "  bridge__record_valid_from,",
        "  bridge__record_valid_to,",
        "  bridge__is_current_record",
        "FROM bridge"
    ])
    
    # Join all parts with newlines
    final_query = '\n'.join(query_parts)
    
    # Save the result
    output_path = './models/silver/uss_bridge.sql'
    with open(output_path, 'w') as f:
        f.write(final_query)
    
    print(f"Query has been saved to {output_path}")
    return final_query

if __name__ == '__main__':
    result = generate_bridge_query()