import yaml
import argparse

def get_primary_keys(table):
    return [col_name for col_name, col in table['columns'].items() 
            if col.get('primary_key', False)]

def find_relationships(tables):
    relationships = []
    column_to_tables = {}
    
    # Build a map of columns to tables
    for table_name, table in tables.items():
        for col_name in table['columns']:
            if table_name not in column_to_tables.get(col_name, []):
                column_to_tables.setdefault(col_name, []).append(table_name)
    
    # Find relationships based on column names
    for table_name, table in tables.items():
        pks = get_primary_keys(table)
        for col_name in table['columns']:
            if col_name in pks:
                continue
            related_tables = column_to_tables.get(col_name, [])
            for related_table in related_tables:
                if related_table != table_name:
                    related_pks = get_primary_keys(tables[related_table])
                    if col_name in related_pks:
                        # Many-to-one relationship: current table -> related table
                        relationships.append((table_name, related_table, col_name))
    
    return relationships

def generate_mermaid_flowchart(tables, relationships, include_labels=False):
    mermaid = ["flowchart LR"]
    
    # Identify roots (no outgoing) and leaves (no incoming)
    sources = set(rel[0] for rel in relationships)
    targets = set(rel[1] for rel in relationships)
    all_tables = set(tables.keys())
    
    roots = all_tables - sources  # Tables with no outgoing relationships
    leaves = all_tables - targets  # Tables with no incoming relationships
    
    # Count connectors for each table
    connector_count = {}
    for table in all_tables:
        connector_count[table] = 0
    for source, target, _ in relationships:
        connector_count[source] = connector_count.get(source, 0) + 1  # Outgoing
        connector_count[target] = connector_count.get(target, 0) + 1  # Incoming
    
    # Sort relationships by total connector count of source then target
    sorted_relationships = sorted(relationships, 
                                key=lambda x: (connector_count[x[0]] + connector_count[x[1]], x[0], x[1]))
    
    # Sort roots and leaves by total connector count
    sorted_roots = sorted(roots, key=lambda x: (connector_count[x], x))
    sorted_leaves = sorted(leaves, key=lambda x: (connector_count[x], x))
    
    # Add roots subgraph if there are any
    if sorted_roots:
        mermaid.append("    subgraph Roots")
        for root in sorted_roots:
            mermaid.append(f"        {root}")
        mermaid.append("    end")
    
    # Add leaves subgraph if there are any
    if sorted_leaves:
        mermaid.append("    subgraph Leaves")
        for leaf in sorted_leaves:
            mermaid.append(f"        {leaf}")
        mermaid.append("    end")
    
    # Add all relationships at the top level, sorted by total connectors
    for source, target, column in sorted_relationships:
        if include_labels:
            mermaid.append(f"    {source} -->|{column}| {target}")
        else:
            mermaid.append(f"    {source} --> {target}")
    
    return "\n".join(mermaid)

def generate_oriented_data_model(file_path, labels=False):
    # Read YAML from file
    try:
        with open(file_path, 'r') as file:
            schema = yaml.safe_load(file)
    except FileNotFoundError:
        return f"Error: Schema file not found at {file_path}"
    except yaml.YAMLError as e:
        return f"Error: Invalid YAML format - {str(e)}"
    
    tables = schema['tables']
    relationships = find_relationships(tables)
    flowchart = generate_mermaid_flowchart(tables, relationships, include_labels=labels)
    return flowchart

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a Mermaid flowchart from a schema YAML")
    parser.add_argument("file_path", help="Path to the schema YAML file")
    parser.add_argument("--labels", action="store_true", default=False, 
                       help="Include column name labels on relationships (default: False)")
    
    args = parser.parse_args()
    
    result = generate_oriented_data_model(args.file_path, labels=args.labels)
    print(result)