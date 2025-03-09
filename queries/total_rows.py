import duckdb

conn = duckdb.connect('./lakehouse/db.duckdb')
conn.execute("SET unsafe_enable_version_guessing = true;")

schemas = ["das", "dab", "dar__staging", "dar"]

for schema in schemas:
    tables_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'"
    tables = conn.execute(tables_query).fetchall()
    
    total_rows = 0
    for table in tables:
        table_name = table[0]
        row_count = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}").fetchone()[0]
        total_rows += row_count
    
    result = total_rows
    
    print(f"Total number of rows in schema '{schema}': {result}")

# Close the connection
conn.close()