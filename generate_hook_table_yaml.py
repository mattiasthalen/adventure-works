import yaml
import os
import re

# Function to parse schema from file
def parse_schema(file_path):
    with open(file_path, 'r') as f:
        schema_text = f.read()
    tables = {}
    current_table = None
    in_columns = False

    for line in schema_text.split('\n'):
        line = line.strip()
        if line.startswith('raw__adventure_works__'):
            if 'columns:' in line:
                current_table = line.split(':')[0]
                tables[current_table] = {'columns': {}}
                in_columns = True
            elif in_columns and current_table:
                if line.startswith('- ') or line.startswith('  '):
                    continue
                elif line == 'write_disposition:' or line == 'table_format:' or line == 'resource:' or line == 'x-normalizer:':
                    in_columns = False
                elif not line:
                    in_columns = False
        elif in_columns and current_table and ':' in line and not line.startswith('#'):
            key, value = [part.strip() for part in line.split(':', 1)]
            if key in ['nullable', 'primary_key', 'data_type']:
                if key not in tables[current_table]['columns'].setdefault(value.split()[0], {}):
                    tables[current_table]['columns'][value.split()[0]] = {}
                tables[current_table]['columns'][value.split()[0]][key] = True if 'true' in value.lower() else False if 'false' in value.lower() else value

    return tables

# Function to load core concepts from concepts.yaml
def load_core_concepts(file_path):
    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)
    return [item['name'] for item in data['concepts']['core']]

# Function to generate tables (now bags) with concepts
def generate_tables(tables, core_concepts):
    result = []
    for table_name, table_data in tables.items():
        if not table_name.startswith('_dlt_'):
            logical_name = table_name.replace('raw__adventure_works__', '')
            table_entry = {
                'name': logical_name,
                'source_table': table_name,
                'concepts': []
            }
            primary_key = next((col for col, attrs in table_data['columns'].items() if attrs.get('primary_key', False)), None)
            for col, attrs in table_data['columns'].items():
                concept_name = col.split('_')[-1] if '_' in col and col.split('_')[-1].isdigit() else col
                concept_name = concept_name.replace('s$', '')  # Simplify to base concept
                if concept_name in core_concepts:
                    core = True
                else:
                    core = False
                primary = attrs.get('primary_key', False)
                identifier = f"{logical_name.replace('s$', '')}__{col}"
                keyset = f"adventure_works__{concept_name}"
                hook = f"_hook__{('reference__' if not core else '')}{concept_name}__id"
                if primary:
                    table_entry['concepts'].append({
                        'name': concept_name,
                        'core': core,
                        'primary': primary,
                        'identifier': identifier,
                        'keyset': keyset,
                        'hook': hook
                    })
                elif col != primary_key and not col.startswith('_dlt_'):
                    table_entry['concepts'].append({
                        'name': concept_name,
                        'core': core,
                        'primary': False,
                        'identifier': identifier,
                        'keyset': keyset,
                        'hook': hook
                    })
            result.append(table_entry)
    return result

# Main execution
schema_file_path = './pipelines/schema/export/adventure_works.schema.yaml'
concepts_file_path = './hook/concepts.yaml'
output_file_path = './hook/bags.yaml'

# Create ./hook directory if it doesn't exist
os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

# Load core concepts
core_concepts = load_core_concepts(concepts_file_path)

# Parse schema and generate tables
tables_data = parse_schema(schema_file_path)
tables_data = generate_tables(tables_data, core_concepts)

# Output to YAML with 'bags' section
output = {
    'bags': tables_data
}

# Save to file
with open(output_file_path, 'w') as f:
    yaml.dump(output, f, sort_keys=False)
print(f"YAML has been generated and saved to {output_file_path}")
