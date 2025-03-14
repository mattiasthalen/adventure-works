import os
import glob
from bs4 import BeautifulSoup
import yaml
import re

def extract_table_info(html_file):
    """Extract table metadata from an HTML file."""
    with open(html_file, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
        soup = BeautifulSoup(content, 'html.parser')
    
    # Extract table name and schema
    header = soup.find('h1', class_='sticky-header')
    if not header:
        print(f"Warning: No header found in {html_file}")
        return None
    
    header_text = header.get_text().strip()
    header_text = re.sub(r'^[^\w.]+', '', header_text).strip()
    
    header_parts = header_text.split('.')
    if len(header_parts) < 2:
        print(f"Warning: Invalid header format in {html_file}: {header_text}")
        return None
    
    schema_name = header_parts[0].strip()
    table_name = header_parts[1].strip()
    
    # Extract table description
    description = ""
    desc_span = soup.find('span', class_='cs82292024')
    if desc_span:
        description = desc_span.get_text().strip()
    
    # Extract column information
    columns = []
    columns_div = soup.find('div', attrs={'data-key': 'columns'})
    if not columns_div:
        print(f"Warning: No columns div found in {html_file}")
        return {
            'schema': schema_name,
            'name': table_name,
            'description': description,
            'columns': []
        }
    
    column_rows = columns_div.select('tbody > tr:not(.only-mobile)')
    
    for row in column_rows:
        tds = row.find_all('td')
        if len(tds) < 5:
            continue
            
        col_name = tds[3].get_text().strip()
        data_type = tds[4].get_text().strip()
        
        # Check if the column is nullable
        nullable_cell = tds[5].get_text().strip()
        is_nullable = nullable_cell != ""
        
        # Get description
        col_description = ""
        if len(tds) >= 9:
            desc_cell = tds[8]
            if 'user-description' in desc_cell.get('class', []):
                col_description = desc_cell.get_text().strip()
        
        # Check for primary key
        primary = False
        key_cell = tds[2]
        primary_key_icon = key_cell.find('i', attrs={'title': 'Primary key'})
        if primary_key_icon:
            primary = True
            
        # Check for unique key (only if not primary)
        unique = False
        if not primary:  # If it's primary, it's implicitly unique
            unique_key_icon = key_cell.find('i', attrs={'title': 'Unique key'})
            if unique_key_icon:
                unique = True
        
        column_info = {
            'name': col_name,
            'data_type': data_type,
            'nullable': is_nullable,
            'description': col_description
        }
        
        if primary:
            column_info['primary'] = True
        elif unique:  # Only add unique flag if it's not already primary
            column_info['unique'] = True
        
        columns.append(column_info)
    
    return {
        'schema': schema_name,
        'name': table_name,
        'description': description,
        'columns': columns
    }

def main():
    # Directory containing HTML files
    html_dir = './dataedo'
    
    # Find all HTML files
    html_pattern = os.path.join(html_dir, '*.html')
    html_files = glob.glob(html_pattern)
    
    if not html_files:
        print(f"No HTML files found in {html_dir}")
        return
        
    print(f"Found {len(html_files)} HTML files")
    
    tables = []
    processed_count = 0
    
    for html_file in html_files:
        try:
            table_info = extract_table_info(html_file)
            if table_info and table_info['schema'] and table_info['name']:
                tables.append(table_info)
                processed_count += 1
                print(f"Processed: {table_info['schema']}.{table_info['name']} - {len(table_info['columns'])} columns")
        except Exception as e:
            print(f"Error processing {html_file}: {e}")
    
    if not tables:
        print("No tables were processed successfully")
        return
        
    # Write to YAML
    output_file = 'adventure_works_schema.yaml'
    with open(output_file, 'w', encoding='utf-8') as f:
        yaml.dump({'tables': tables}, f, default_flow_style=False, sort_keys=False)
    
    print(f"Successfully processed {processed_count} tables and saved to {output_file}")

if __name__ == "__main__":
    main()