import yaml
import os
import re

# ---- Raw Model Generation Functions ----
def generate_raw_column_def(column_name, column_info):
    """Generate column definition for raw layer with proper type"""
    type_mapping = {
        'bigint': 'BIGINT',
        'text': 'VARCHAR',
        'timestamp': 'TIMESTAMP',
        'double': 'DOUBLE',
        'bool': 'BOOLEAN'
    }
    data_type = column_info.get('data_type', 'text')
    sql_type = type_mapping.get(data_type.lower(), 'VARCHAR')
    return f"  {column_name}::{sql_type}"

def generate_raw_model_sql(table_name, columns):
    """Generate the complete SQL model for a raw table with ordered columns"""
    primary_keys = []
    foreign_keys = []
    general_fields = []
    dlt_columns = []

    for col_name, col_info in columns.items():
        if col_info.get('primary_key', False):
            primary_keys.append((col_name, col_info))
        elif col_name.startswith('_dlt_'):
            dlt_columns.append((col_name, col_info))
        elif col_name.endswith('_id') and not col_name.startswith('_dlt_'):
            foreign_keys.append((col_name, col_info))
        else:
            general_fields.append((col_name, col_info))

    primary_keys.sort()
    foreign_keys.sort()
    general_fields.sort()
    dlt_columns.sort()

    ordered_columns = primary_keys + foreign_keys + general_fields + dlt_columns
    column_defs = [generate_raw_column_def(col_name, col_info)
                  for col_name, col_info in ordered_columns]

    if not any(col_name == '_dlt_load_id' for col_name, _ in ordered_columns):
        column_defs.append("  _dlt_load_id::VARCHAR")

    columns_str = ",\n".join(column_defs)

    return f"""MODEL (
  kind VIEW,
  enabled TRUE
);
SELECT
{columns_str}
FROM ICEBERG_SCAN(
  "file://" || @project_path || "/lakehouse/bronze/{table_name}"
);
"""

# ---- Hook Model Generation Functions ----
def extract_columns_from_raw(raw_sql, prefix):
    """Extract columns from raw model SQL and adapt for staging without casting"""
    select_start = raw_sql.find('SELECT') + len('SELECT')
    select_end = raw_sql.find('FROM ICEBERG_SCAN')
    columns_str = raw_sql[select_start:select_end].strip()

    type_mappings = {}
    staging_columns = []

    for line in columns_str.split(',\n'):
        if line.strip():
            column_def = line.strip()
            column_name, sql_type = column_def.split('::')
            column_name = column_name.strip()
            sql_type = sql_type.strip()

            if column_name.startswith('_dlt_') and column_name != '_dlt_load_id':
                continue

            type_mappings[f"{prefix}__{column_name}"] = sql_type
            if column_name == '_dlt_load_id':
                staging_columns.append(f"    TO_TIMESTAMP(_dlt_load_id::DOUBLE) AS {prefix}__record_loaded_at")
            else:
                staging_columns.append(f"    {column_name} AS {prefix}__{column_name}")

    return staging_columns, type_mappings

def generate_hook_output_column(column_name, type_mappings):
    """Generate final output column with proper type casting from raw model"""
    if column_name.startswith('_pit_hook__') or column_name.startswith('_hook__'):
        return f"  {column_name}::BLOB"
    elif column_name.endswith('__record_loaded_at') or column_name.endswith('__record_valid_from') or column_name.endswith('__record_valid_to') or column_name.endswith('__record_updated_at'):
        return f"  {column_name}::TIMESTAMP"
    elif column_name.endswith('__record_version'):
        return f"  {column_name}::INT"
    elif column_name.endswith('__is_current_record'):
        return f"  {column_name}::BOOLEAN"
    else:
        return f"  {column_name}::{type_mappings.get(column_name, 'VARCHAR')}"

def generate_hook_model_sql(raw_table_name, columns, raw_sql):
    """Generate the complete SQL model for a hook table"""
    table_name = raw_table_name.replace('raw__adventure_works__', '')
    
    if table_name.endswith('ies'):
        prefix = table_name[:-3] + 'y'
    elif table_name.endswith('es'):
        prefix = table_name[:-2]
    elif table_name.endswith('s'):
        prefix = table_name[:-1]
    else:
        prefix = table_name

    pk_column = next((col for col, info in columns.items() if info.get('primary_key', False)), None)
    if not pk_column:
        raise ValueError(f"No primary key found for {raw_table_name}")

    staging_columns, type_mappings = extract_columns_from_raw(raw_sql, prefix)
    staging_columns_str = ",\n".join(staging_columns)

    foreign_keys = [col for col, info in columns.items()
                   if col.endswith('_id') and not col.startswith('_dlt_') and not info.get('primary_key', False)]
    
    pk_hook_name = pk_column.replace('_id', '') if pk_column.endswith('_id') else pk_column
    pit_hook = [
        f"""    CONCAT(
      '{pk_hook_name}|adventure_works|',
      {prefix}__{pk_column},
      '~epoch|valid_from|',
      {prefix}__record_valid_from
    ) AS _pit_hook__{pk_hook_name}"""
    ]

    hook_columns = [
        f"    CONCAT('{pk_hook_name}|adventure_works|', {prefix}__{pk_column}) AS _hook__{pk_hook_name}"
    ]
    hook_name_mapping = {}

    for fk in foreign_keys:
        if fk == 'bill_to_address_id':
            hook_columns.append(
                f"    CONCAT('address|adventure_works|', {prefix}__{fk}) AS _hook__address__bill_to"
            )
            hook_name_mapping[fk] = "_hook__address__bill_to"
        elif fk == 'ship_to_address_id':
            hook_columns.append(
                f"    CONCAT('address|adventure_works|', {prefix}__{fk}) AS _hook__address__ship_to"
            )
            hook_name_mapping[fk] = "_hook__address__ship_to"
        elif fk == 'business_entity_id':
            hook_columns.append(
                f"    CONCAT('business_entity|adventure_works|', {prefix}__{fk}) AS _hook__business_entity"
            )
            hook_name_mapping[fk] = "_hook__business_entity"
        else:
            fk_hook_name = fk.replace('_id', '') if fk.endswith('_id') else fk
            hook_columns.append(
                f"    CONCAT('{fk_hook_name}|adventure_works|', {prefix if fk != 'login_id' else 'HEX(' + prefix}__{fk}{'' if fk != 'login_id' else ')'}) AS _hook__{fk_hook_name}"
            )
            hook_name_mapping[fk] = f"_hook__{fk_hook_name}"
    
    pit_hook_str = ",\n".join(pit_hook)
    hook_columns_str = ",\n".join(hook_columns)

    pk_cols = [f"{prefix}__{pk_column}"]
    fk_cols = [f"{prefix}__{fk}" for fk in foreign_keys if fk not in {'bill_to_address_id', 'ship_to_address_id'}]
    general_cols = [f"{prefix}__{col}" for col, info in columns.items()
                   if not col.startswith('_dlt_') and not info.get('primary_key', False) and not col.endswith('_id')]
    metadata_cols = [
        f"{prefix}__record_loaded_at",
        f"{prefix}__record_updated_at",
        f"{prefix}__record_valid_from",
        f"{prefix}__record_valid_to",
        f"{prefix}__record_version",
        f"{prefix}__is_current_record"
    ]

    pk_cols.sort()
    fk_cols.sort()
    general_cols.sort()

    output_columns = (
        [f"_pit_hook__{pk_hook_name}"] +
        [f"_hook__{pk_hook_name}"] +
        [hook_name_mapping[fk] for fk in sorted(foreign_keys)] +
        pk_cols + fk_cols + general_cols + metadata_cols
    )

    output_columns_str = ",\n".join([generate_hook_output_column(col, type_mappings) for col in output_columns])

    return f"""MODEL (
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column {prefix}__record_updated_at
  ),
  enabled TRUE
);

WITH staging AS (
  SELECT
{staging_columns_str}
  FROM bronze.{raw_table_name}
), validity AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY {prefix}__{pk_column} ORDER BY {prefix}__record_loaded_at) AS {prefix}__record_version,
    CASE
      WHEN {prefix}__record_version = 1
      THEN @min_ts::TIMESTAMP
      ELSE {prefix}__record_loaded_at
    END AS {prefix}__record_valid_from,
    COALESCE(
      LEAD({prefix}__record_loaded_at) OVER (PARTITION BY {prefix}__{pk_column} ORDER BY {prefix}__record_loaded_at),
      @max_ts::TIMESTAMP
    ) AS {prefix}__record_valid_to,
    {prefix}__record_valid_to = @max_ts::TIMESTAMP AS {prefix}__is_current_record,
    CASE
      WHEN {prefix}__is_current_record
      THEN {prefix}__record_loaded_at
      ELSE {prefix}__record_valid_to
    END AS {prefix}__record_updated_at
  FROM staging
), hooks AS (
  SELECT
{pit_hook_str},
{hook_columns_str},
    *
  FROM validity
)
SELECT
{output_columns_str}
FROM hooks
WHERE 1 = 1
AND {prefix}__record_updated_at BETWEEN @start_ts AND @end_ts"""

# ---- USS Bridge Generation Functions ----
def parse_hook_model(file_path):
    with open(file_path, 'r') as f:
        sql = f.read()
    
    select_start = sql.rfind('SELECT')
    select_content = sql[select_start:].split('FROM hooks')[0].strip()
    lines = select_content.split('\n')
    
    hooks = {}
    pit_hook = None
    prefix = None
    for line in lines:
        line = line.strip()
        if not line or line.startswith('FROM') or line.startswith('WHERE'):
            continue
        match = re.match(r'^\s*(_pit_hook__|_hook__)([a-z_]+(?:__[a-z_]+)?)::[A-Z]+', line)
        if match:
            hook_type, hook_name = match.groups()
            if hook_type == '_pit_hook__':
                pit_hook = f"_pit_hook__{hook_name}"
            elif hook_type == '_hook__':
                if not hooks:  # First _hook__ is PK
                    hooks[f"_hook__{hook_name}"] = 'pk'
                else:
                    hooks[f"_hook__{hook_name}"] = 'fk'
        prefix_match = re.match(r'^\s*([a-z_]+)__record_loaded_at::TIMESTAMP', line)
        if prefix_match:
            prefix = prefix_match.group(1)
    
    return {
        'prefix': prefix,
        'pit_hook': pit_hook,
        'pk_hook': next((h for h, t in hooks.items() if t == 'pk'), None),
        'fk_hooks': [h for h, t in hooks.items() if t == 'fk']
    }

def build_dependency_graph(hook_dir):
    graph = {}
    hook_files = [f for f in os.listdir(hook_dir) if f.startswith('hook__adventure_works__') and f.endswith('.sql')]
    
    for file in hook_files:
        hook_name = f"hook__adventure_works__{file.replace('hook__adventure_works__', '').replace('.sql', '')}"
        info = parse_hook_model(os.path.join(hook_dir, file))
        graph[hook_name] = {
            'prefix': info['prefix'],
            'pit_hook': info['pit_hook'],
            'pk_hook': info['pk_hook'],
            'fk_hooks': info['fk_hooks'],
            'upstream': []
        }
    
    for node, info in graph.items():
        for fk_hook in info['fk_hooks']:
            match = re.match(r'_hook__([a-z_]+)(?:__[a-z_]+)?', fk_hook)
            if not match:
                continue
            concept = match.group(1)
            target_hook = f"_hook__{concept}"
            for upstream, up_info in graph.items():
                if up_info['pk_hook'] == target_hook:
                    info['upstream'].append((fk_hook, upstream))
                    break
    
    for node, info in graph.items():
        upstream_by_table = {}
        for fk_hook, upstream in info['upstream']:
            if upstream not in upstream_by_table:
                upstream_by_table[upstream] = []
            upstream_by_table[upstream].append(fk_hook)
        
        for upstream, fk_hooks in upstream_by_table.items():
            if len(fk_hooks) > 1:
                print(f"\nMultiple subtypes to {upstream} detected for {node}:")
                for i, fk_hook in enumerate(fk_hooks):
                    print(f"{i + 1}. {fk_hook}")
                choice = int(input(f"Choose one subtype for join to {upstream} (1-{len(fk_hooks)}): ")) - 1
                info['join_fk_hook'] = fk_hooks[choice]
                info['join_upstream'] = upstream
                info['upstream'] = [(fk, up) for fk, up in info['upstream'] if up != upstream or fk == fk_hooks[choice]]
            else:
                if 'join_fk_hook' not in info:
                    info['join_fk_hook'] = fk_hooks[0]
                    info['join_upstream'] = upstream
    
    return graph

def generate_bridge_sql(hook_name, graph):
    info = graph[hook_name]
    bridge_name = f"uss_bridge__adventure_works__{info['prefix']}"
    
    select_clause = [
        f"    '{info['prefix']}' AS peripheral",
        f"    {hook_name}.{info['pit_hook']}"
    ]
    
    upstream_pit_hooks = set()
    for _, upstream in info['upstream']:
        up_info = graph[upstream]
        upstream_pit_hooks.add(f"    {upstream}.{up_info['pit_hook']}")
        for _, up_upstream in up_info['upstream']:
            up_up_info = graph[up_upstream]
            upstream_pit_hooks.add(f"    {up_upstream}.{up_up_info['pit_hook']}")
    
    select_clause.extend(sorted(upstream_pit_hooks))
    
    loaded_at = [f"{hook_name}.{info['prefix']}__record_loaded_at"]
    updated_at = [f"{hook_name}.{info['prefix']}__record_updated_at"]
    valid_from = [f"{hook_name}.{info['prefix']}__record_valid_from"]
    valid_to = [f"{hook_name}.{info['prefix']}__record_valid_to"]
    
    for _, upstream in info['upstream']:
        up_info = graph[upstream]
        up_prefix = up_info['prefix']
        loaded_at.append(f"{upstream}.{up_prefix}__record_loaded_at")
        updated_at.append(f"{upstream}.{up_prefix}__record_updated_at")
        valid_from.append(f"{upstream}.{up_prefix}__record_valid_from")
        valid_to.append(f"{upstream}.{up_prefix}__record_valid_to")
    
    select_clause.extend([
        f"    GREATEST(\n      {',\n      '.join(loaded_at)}\n    ) AS bridge__record_loaded_at",
        f"    GREATEST(\n      {',\n      '.join(updated_at)}\n    ) AS bridge__record_updated_at",
        f"    GREATEST(\n      {',\n      '.join(valid_from)}\n    ) AS bridge__record_valid_from",
        f"    LEAST(\n      {',\n      '.join(valid_to)}\n    ) AS bridge__record_valid_to"
    ])
    
    join_clauses = [f"  FROM silver.{hook_name}"]
    for fk_hook, upstream in info['upstream']:
        up_info = graph[upstream]
        up_prefix = up_info['prefix']
        validity_condition = (
            f"    AND {hook_name}.{info['prefix']}__record_valid_from <= {upstream}.{up_prefix}__record_valid_to\n"
            f"    AND {hook_name}.{info['prefix']}__record_valid_to >= {upstream}.{up_prefix}__record_valid_from"
        ) if upstream == info.get('join_upstream') and fk_hook == info.get('join_fk_hook') else (
            f"    AND {hook_name}.{info['prefix']}__record_valid_from <= {upstream}.{up_prefix}__record_valid_to"
        )
        join_clauses.append(
            f"  LEFT JOIN silver.{upstream}\n"
            f"    ON {hook_name}.{fk_hook} = {upstream}.{up_info['pk_hook']}\n"
            f"{validity_condition}"
        )
    
    sql = f"""MODEL (
  kind VIEW,
  enabled FALSE
);

WITH bridge AS (
  SELECT
{',\n'.join(select_clause)}
{'\n'.join(join_clauses)}
)
SELECT
  *,
  bridge__record_valid_to = '9999-12-31 23:59:59'::TIMESTAMP AS bridge__is_current_record
FROM bridge"""
    return sql

# ---- Main Processing Function ----
def process_schema(yaml_content):
    """Process the schema and generate raw, hook, and USS bridge models"""
    schema = yaml.safe_load(yaml_content)

    bronze_dir = "./models/bronze"
    silver_dir = "./models/silver"
    os.makedirs(bronze_dir, exist_ok=True)
    os.makedirs(silver_dir, exist_ok=True)

    raw_tables = {k: v for k, v in schema['tables'].items() if k.startswith('raw__')}

    for table_name, table_info in raw_tables.items():
        raw_model_sql = generate_raw_model_sql(table_name, table_info['columns'])
        print(f"Generated raw model for {table_name}.")

        raw_file_path = f"{bronze_dir}/{table_name}.sql"
        with open(raw_file_path, 'w') as f:
            f.write(raw_model_sql)
        
        hook_model_sql = generate_hook_model_sql(table_name, table_info['columns'], raw_model_sql)  # Fixed: raw_sql -> raw_model_sql
        hook_table_name = table_name.replace('raw__', 'hook__')
        print(f"Generated hook model for {hook_table_name}.")

        hook_file_path = f"{silver_dir}/{hook_table_name}.sql"
        with open(hook_file_path, 'w') as f:
            f.write(hook_model_sql)
    
    graph = build_dependency_graph(silver_dir)
    
    for hook_name in graph:
        sql = generate_bridge_sql(hook_name, graph)
        bridge_name = hook_name.replace('hook__', 'uss_bridge__')
        file_path = os.path.join(silver_dir, f"{bridge_name}.sql")
        with open(file_path, 'w') as f:
            f.write(sql)
        print(f"Generated USS bridge for {bridge_name}.")
    
    total_models = len(raw_tables) * 2 + len(graph)
    print(f"Generated {total_models} models (raw: {len(raw_tables)}, hook: {len(raw_tables)}, bridge: {len(graph)}).")

# Read the YAML file and process
yaml_path = "./pipelines/schemas/export/adventure_works.schema.yaml"
with open(yaml_path, 'r') as f:
    yaml_content = f.read()

process_schema(yaml_content)
