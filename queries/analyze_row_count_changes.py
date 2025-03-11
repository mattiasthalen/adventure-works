import polars as pl
import duckdb
from collections import defaultdict

def classify_layer(schema, table_name):
    """Determine which layer a table belongs to."""
    if table_name.startswith('raw__adventure_works__'):
        return 'raw'
    elif table_name.startswith('bag__adventure_works__'):
        return 'bag'
    elif table_name.startswith('bridge__'):
        return 'bridge'
    elif schema == 'dar' and not table_name.startswith(('bridge__', 'events__', '_')):
        return 'peripheral'
    return 'other'

def normalize_table_name(table_name):
    """Extract the base entity name from a table name."""
    if table_name.startswith('raw__adventure_works__'):
        return table_name.replace('raw__adventure_works__', '')
    elif table_name.startswith('bag__adventure_works__'):
        return table_name.replace('bag__adventure_works__', '')
    elif table_name.startswith('bridge__'):
        return table_name.replace('bridge__', '')
    return table_name

def get_table_counts(db_path):
    """Get row counts for all tables in specified schemas."""
    conn = duckdb.connect(db_path)
    conn.execute("SET unsafe_enable_version_guessing = true;")
    
    tables_query = """
    SELECT 
        table_schema as schema,
        table_name as table
    FROM information_schema.tables
    WHERE table_schema IN ('das', 'dab', 'dar__staging', 'dar')
    AND table_name NOT LIKE '_dlt%'
    ORDER BY table_schema, table_name
    """
    
    tables_df = conn.execute(tables_query).fetchdf()
    tables_pl = pl.from_pandas(tables_df)
    
    results = []
    for row in tables_pl.iter_rows(named=True):
        schema_name = row['schema']
        table_name = row['table']
        
        layer = classify_layer(schema_name, table_name)
        if layer == 'other':
            continue
            
        base_name = normalize_table_name(table_name)
        
        count_query = f'SELECT COUNT(*) as row_count FROM "{schema_name}"."{table_name}"'
        try:
            row_count = conn.execute(count_query).fetchone()[0]
            
            results.append({
                "schema": schema_name,
                "table": table_name,
                "base_name": base_name,
                "layer": layer,
                "row_count": row_count
            })
        except Exception as e:
            print(f"Error counting rows in {schema_name}.{table_name}: {e}")
    
    conn.close()
    return results

def analyze_specific_transitions(counts):
    """Analyze raw-to-bag, bag-to-bridge, and bag-to-peripheral transitions."""
    # Group by base table name and layer
    entity_layer_counts = defaultdict(dict)
    
    for item in counts:
        base_name = item['base_name']
        layer = item['layer']
        row_count = item['row_count']
        
        entity_layer_counts[base_name][layer] = row_count
    
    # Analyze specific transitions
    raw_to_bag = []
    bag_to_bridge = []
    bag_to_peripheral = []
    
    for base_name, layer_counts in entity_layer_counts.items():
        # Raw to Bag
        if 'raw' in layer_counts and 'bag' in layer_counts:
            diff = layer_counts['bag'] - layer_counts['raw']
            if diff != 0:
                raw_to_bag.append({
                    "base_name": base_name,
                    "raw_count": layer_counts['raw'],
                    "bag_count": layer_counts['bag'],
                    "difference": diff,
                    "abs_difference": abs(diff)
                })
        
        # Bag to Bridge
        if 'bag' in layer_counts and 'bridge' in layer_counts:
            diff = layer_counts['bridge'] - layer_counts['bag']
            if diff != 0:
                bag_to_bridge.append({
                    "base_name": base_name,
                    "bag_count": layer_counts['bag'],
                    "bridge_count": layer_counts['bridge'],
                    "difference": diff,
                    "abs_difference": abs(diff)
                })
        
        # Bag to Peripheral
        if 'bag' in layer_counts and 'peripheral' in layer_counts:
            diff = layer_counts['peripheral'] - layer_counts['bag']
            if diff != 0:
                bag_to_peripheral.append({
                    "base_name": base_name,
                    "bag_count": layer_counts['bag'],
                    "peripheral_count": layer_counts['peripheral'],
                    "difference": diff,
                    "abs_difference": abs(diff)
                })
    
    return raw_to_bag, bag_to_bridge, bag_to_peripheral

def main():
    db_path = "./lakehouse/db.duckdb"
    
    try:
        # Get row counts for all tables
        counts = get_table_counts(db_path)
        
        # Analyze specified transitions
        raw_to_bag, bag_to_bridge, bag_to_peripheral = analyze_specific_transitions(counts)
        
        # Create and display DataFrames
        if raw_to_bag:
            print("\nTables with non-zero row count changes from RAW to BAG:")
            raw_to_bag_df = pl.DataFrame(raw_to_bag).sort("abs_difference", descending=True).drop("abs_difference")
            print(raw_to_bag_df)
        else:
            print("\nNo tables with non-zero row count changes from RAW to BAG.")
        
        if bag_to_bridge:
            print("\nTables with non-zero row count changes from BAG to BRIDGE:")
            bag_to_bridge_df = pl.DataFrame(bag_to_bridge).sort("abs_difference", descending=True).drop("abs_difference")
            print(bag_to_bridge_df)
        else:
            print("\nNo tables with non-zero row count changes from BAG to BRIDGE.")
        
        if bag_to_peripheral:
            print("\nTables with non-zero row count changes from BAG to PERIPHERAL:")
            bag_to_peripheral_df = pl.DataFrame(bag_to_peripheral).sort("abs_difference", descending=True).drop("abs_difference")
            print(bag_to_peripheral_df)
        else:
            print("\nNo tables with non-zero row count changes from BAG to PERIPHERAL.")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()