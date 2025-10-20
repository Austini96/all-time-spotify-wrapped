"""
Sync DuckDB data to PostgreSQL for Metabase visualization
"""
import duckdb
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def sync_duckdb_to_postgres():
    """
    Sync all analytics tables from DuckDB to PostgreSQL
    """
    # Connect to DuckDB
    duckdb_path = '/opt/airflow/data/duckdb/spotify.duckdb'
    duck_conn = duckdb.connect(duckdb_path, read_only=True)
    
    # Connect to PostgreSQL (using Marquez DB for simplicity)
    # In production, you'd want a separate analytics database
    pg_conn = psycopg2.connect(
        host='marquez-db',
        port=5432,
        database='marquez',
        user='marquez',
        password='marquez'
    )
    pg_cursor = pg_conn.cursor()
    
    # Create a separate schema for analytics
    pg_cursor.execute("CREATE SCHEMA IF NOT EXISTS spotify_analytics")
    pg_conn.commit()
    
    # Tables to sync
    tables = [
        # Analytics tables (final outputs)
        'analytics.top_tracks_daily',
        'analytics.top_artists_daily',
        'analytics.listening_patterns_hourly',
        'analytics.audio_features_analysis',
        'analytics.playlist_analysis',
        # Marts tables (if you want detailed views)
        'marts.dim_tracks',
        'marts.dim_artists',
        'marts.dim_albums',
        'marts.dim_playlists',
        'marts.fct_listening_history',
    ]
    
    for table in tables:
        try:
            # Read from DuckDB
            logger.info(f"Reading {table} from DuckDB...")
            df = duck_conn.execute(f"SELECT * FROM {table}").df()
            
            if df.empty:
                logger.warning(f"Table {table} is empty, skipping...")
                continue
            
            # Create table name for PostgreSQL
            pg_table = table.replace('.', '_')
            
            # Drop and recreate table in PostgreSQL
            logger.info(f"Creating table spotify_analytics.{pg_table} in PostgreSQL...")
            
            # Drop table if exists
            pg_cursor.execute(f"DROP TABLE IF EXISTS spotify_analytics.{pg_table} CASCADE")
            
            # Create table with appropriate types
            create_stmt = f"CREATE TABLE spotify_analytics.{pg_table} ("
            
            for col, dtype in zip(df.columns, df.dtypes):
                # Map pandas/DuckDB types to PostgreSQL types
                if dtype == 'object':
                    pg_type = 'TEXT'
                elif 'int' in str(dtype):
                    pg_type = 'BIGINT'
                elif 'float' in str(dtype):
                    pg_type = 'DOUBLE PRECISION'
                elif 'datetime' in str(dtype):
                    pg_type = 'TIMESTAMP'
                elif 'bool' in str(dtype):
                    pg_type = 'BOOLEAN'
                else:
                    pg_type = 'TEXT'
                
                create_stmt += f'"{col}" {pg_type}, '
            
            create_stmt = create_stmt.rstrip(', ') + ')'
            pg_cursor.execute(create_stmt)
            pg_conn.commit()
            
            # Insert data
            logger.info(f"Inserting {len(df)} rows into spotify_analytics.{pg_table}...")
            
            # Convert DataFrame to list of tuples
            data = [tuple(row) for row in df.values]
            
            # Use execute_values for efficient bulk insert
            cols = ', '.join([f'"{col}"' for col in df.columns])
            insert_stmt = f"INSERT INTO spotify_analytics.{pg_table} ({cols}) VALUES %s"
            execute_values(pg_cursor, insert_stmt, data)
            pg_conn.commit()
            
            logger.info(f"✓ Successfully synced {table} ({len(df)} rows)")
            
        except Exception as e:
            logger.error(f"Error syncing {table}: {e}")
            pg_conn.rollback()
            continue
    
    # Close connections
    duck_conn.close()
    pg_cursor.close()
    pg_conn.close()
    
    logger.info("\n✅ Sync complete! Data is ready in PostgreSQL for Metabase")
    logger.info("   Database: marquez")
    logger.info("   Schema: spotify_analytics")
    logger.info("   Host: marquez-db")
    logger.info("   Port: 5432")


if __name__ == "__main__":
    sync_duckdb_to_postgres()

