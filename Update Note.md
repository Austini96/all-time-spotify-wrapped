## Update Notes

### 2025-11-05
- **Removed audio feature related data**: Deleted 2 dbt models and 1 database table, removed 13 audio feature columns from dim_tracks as they are deprecated in Spotify API and have very limited use through third party APIs
- **Normalized schema design**: Added 4 dimension foreign keys to fct_listening_history table for better star schema normalization
- **Enhanced listening pattern tracking**: Added 6 new columns for session/consecutive listening analysis and created 1 new analytics table to track detailed listening patterns
- **Documentation improvements**: Completed column descriptions for better data documentation
- **Code simplification**: To improve maintanability and reduce DuckDB lock issues by being able to keep better track of DuckDB process concurrency, reduced codebase by 90 net lines, with DuckDB loader simplified by 170 net lines and Python scripts reduced by 96 net lines

### 2025-10-21
- **DuckDB + OpenLineage**: Updated automation of emitting column-level lineage to Marquez during dbt execution by using `openlineage-dbt` package and `dbt-ol run` to run the model instead of `dbt run`
- **DuckDB + Metabase**: Metabase is now connecting directly to DuckDB using the [DuckDB driver](https://github.com/motherduckdb/metabase_duckdb_driver), eliminating the unnecessary work for PostgreSQL sync, thus simplifying the architecture.