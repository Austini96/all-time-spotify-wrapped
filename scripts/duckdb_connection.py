"""
Provides context managers for safe DuckDB connection handling
"""
import duckdb
import os
import logging
import time
from contextlib import contextmanager
from threading import Lock
from typing import Optional

logger = logging.getLogger(__name__)

# Global connection lock to prevent concurrent connections
_connection_lock = Lock()
_default_db_path = '/opt/airflow/data/duckdb/spotify.duckdb'

@contextmanager
def get_duckdb_connection(db_path: Optional[str] = None):
    if db_path is None:
        db_path = _default_db_path
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Try to connect with retry for transient lock errors
    conn = None
    max_attempts = 3
    
    for attempt in range(max_attempts):
        # Acquire lock to prevent concurrent connections within this process
        with _connection_lock:
            try:
                conn = duckdb.connect(db_path, read_only=False)
                break  # Success - exit retry loop
            except Exception as e:
                error_msg = str(e).lower()
                error_type = type(e).__name__
                
                # Check for lock-related errors (DuckDB file locks, database locks, etc.)
                is_lock_error = ("lock" in error_msg or 
                                "busy" in error_msg or 
                                "conflicting" in error_msg or
                                "ioerror" in error_type.lower())
                
                # If not a lock error, or last attempt, raise immediately
                if not is_lock_error or attempt == max_attempts - 1:
                    raise
                
                # Calculate wait time with exponential backoff (2s, 4s, 8s, 16s)
                wait_time = 2 ** (attempt + 1)
                logger.warning(f"Database locked (process conflict), retrying in {wait_time}s (attempt {attempt + 1}/{max_attempts})")
        
        # Wait outside the lock so other threads/processes can proceed
        time.sleep(wait_time)
    
    # Yield connection to caller
    yield conn
    
    # Close connection when done
    conn.close()