import yaml
import os

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

    if table_name.endswith('es'):
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

    # Name hooks after the key (strip '_id' if present)
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
            # Shouldn't happen if it's the PK, but handle it
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

# ---- Main Processing Function ----
def process_schema(yaml_content):
    """Process the schema and generate both raw and hook models"""
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

        hook_model_sql = generate_hook_model_sql(table_name, table_info['columns'], raw_model_sql)
        hook_table_name = table_name.replace('raw__', 'bag__')
        print(f"Generated hook model for {hook_table_name}.")

        hook_file_path = f"{silver_dir}/{hook_table_name}.sql"
        with open(hook_file_path, 'w') as f:
            f.write(hook_model_sql)

    print(f"Generated {len(raw_tables)*2} models.")

# Read the YAML file and process
yaml_path = "./pipelines/schemas/export/adventure_works.schema.yaml"
with open(yaml_path, 'r') as f:
    yaml_content = f.read()

process_schema(yaml_content)
