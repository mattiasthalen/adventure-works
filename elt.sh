uv run ./pipelines/adventure_works.py prod
uv run sqlmesh plan prod
uv run ./pipelines/duckdb_to_iceberg.py