import yaml
import os

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

# Function to load existing concepts from hook.yml
def load_existing_concepts(file_path):
    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)
    core_concepts = {item.get('name'): item.get('taxonomy', []) for item in data['concepts']['core']}
    return core_concepts

# Function to generate reference concepts from schema
def generate_reference_concepts(tables, core_concepts):
    reference_concepts = set()
    for table_name, table_data in tables.items():
        if not table_name.startswith('_dlt_'):
            logical_name = table_name.replace('raw__adventure_works__', '')
            base_concept = logical_name.replace('_', '').replace('s$', '')  # Simplify to base concept
            if base_concept not in core_concepts:
                reference_concepts.add(base_concept)

            for column in table_data['columns']:
                if not table_data['columns'][column].get('primary_key', False):
                    foreign_concept = column.split('_')[-1] if '_' in column and column.split('_')[-1].isdigit() else column
                    foreign_concept = foreign_concept.replace('s$', '')  # Simplify to base concept
                    if foreign_concept not in core_concepts and foreign_concept != base_concept:
                        reference_concepts.add(foreign_concept)

    return sorted(list(reference_concepts))

# Function to generate bags with concepts, definition, and taxonomy
def generate_bags(tables, core_concepts):
    result = []
    for table_name, table_data in tables.items():
        if not table_name.startswith('_dlt_'):
            logical_name = table_name.replace('raw__adventure_works__', '')
            bag_entry = {
                'name': logical_name,
                'source_table': table_name,
                'definition': '',  # Empty definition field
                'concepts': []
            }
            primary_key = next((col for col, attrs in table_data['columns'].items() if attrs.get('primary_key', False)), None)
            for col, attrs in table_data['columns'].items():
                concept_name = col.split('_')[-1] if '_' in col and col.split('_')[-1].isdigit() else col
                concept_name = concept_name.replace('s$', '')  # Simplify to base concept
                if concept_name in core_concepts:
                    core = True
                    taxonomy = core_concepts.get(concept_name, [])
                    # Infer taxonomy based on table name for order
                    if concept_name == 'order' and primary_key:
                        if 'sales' in logical_name:
                            taxonomy = ['sales']
                        elif 'purchase' in logical_name:
                            taxonomy = ['purchase']
                        elif 'work' in logical_name:
                            taxonomy = ['work']
                else:
                    core = False
                    taxonomy = []
                primary = attrs.get('primary_key', False)
                identifier = f"{logical_name.replace('s$', '')}__{col}"
                keyset = f"adventure_works__{concept_name}"
                hook = f"_hook__{('reference__' if not core else '')}{concept_name}__id"
                concept = {
                    'name': concept_name,
                    'core': core,
                    'primary': primary,
                    'identifier': identifier,
                    'keyset': keyset,
                    'hook': hook
                }
                if taxonomy and primary:  # Only add taxonomy to primary concept if defined
                    concept['taxonomy'] = taxonomy
                if primary:
                    bag_entry['concepts'].append(concept)
                elif col != primary_key and not col.startswith('_dlt_'):
                    bag_entry['concepts'].append(concept)
            result.append(bag_entry)
    return result

# Function to prompt for overwrite confirmation
def prompt_overwrite(existing_data):
    overwrite_reference = 'reference' in existing_data['concepts'] and existing_data['concepts']['reference']
    overwrite_bags = 'bags' in existing_data and existing_data['bags']
    if overwrite_reference or overwrite_bags:
        message = "The following sections will be overwritten:\n"
        if overwrite_reference:
            message += "- reference concepts\n"
        if overwrite_bags:
            message += "- bags\n"
        message += "Proceed? (yes/no): "
        response = input(message).lower()
        return response == 'yes'
    return True  # No overwrite needed if sections are empty or absent

# Main execution
schema_file_path = './pipelines/schema/export/adventure_works.schema.yaml'
hook_file_path = './hook.yml'

# Create ./hook directory if it doesn't exist
os.makedirs(os.path.dirname(hook_file_path), exist_ok=True)

# Load existing concepts
core_concepts = load_existing_concepts(hook_file_path)

# Parse schema and generate reference concepts
tables_data = parse_schema(schema_file_path)
reference_concepts = generate_reference_concepts(tables_data, core_concepts)

# Generate bags
bags_data = generate_bags(tables_data, core_concepts)

# Load existing hook.yml data and check for overwrite
with open(hook_file_path, 'r') as f:
    existing_data = yaml.safe_load(f)
if not prompt_overwrite(existing_data):
    print("Operation cancelled by user.")
else:
    existing_data['concepts']['reference'] = [{'name': name} for name in reference_concepts]  # Overwrite reference
    existing_data['bags'] = bags_data  # Overwrite bags
    # Save updated content back to hook.yml
    with open(hook_file_path, 'w') as f:
        yaml.dump(existing_data, f, sort_keys=False)
    print("hook.yml has been updated with overwritten reference concepts and bags")