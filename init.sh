uv run ./pipelines/adventure_works.py prod
uv run sqlmesh run prod
uv run ./pipelines/duckdb_to_iceberg.py