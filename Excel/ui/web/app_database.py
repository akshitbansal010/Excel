"""
Database connection and operations for DataEngine Pro Streamlit app.
Handles SQLite database connections and queries.
"""

import re
import sqlite3
import pandas as pd
import os
import tempfile
import streamlit as st
from typing import List, Optional, Tuple


# =============================================================================
# VALIDATION HELPERS
# =============================================================================

def _is_valid_table_name(table_name: str) -> bool:
    """
    Validate table name to prevent SQL injection attacks.
    
    Args:
        table_name: Table name to validate
        
    Returns:
        True if valid (matches pattern), False otherwise
    """
    return isinstance(table_name, str) and bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name))


def connect_to_database(db_path: str) -> bool:
    """
    Connect to a SQLite database.
    
    Args:
        db_path: Path to the SQLite database file
        
    Returns:
        True if connection successful, False otherwise
    """
    try:
        conn = sqlite3.connect(db_path)
        st.session_state.db_connection = conn
        st.session_state.db_path = db_path
        return True
    except Exception as e:
        st.error(f"Failed to connect: {e}")
        return False


def disconnect_from_database():
    """Disconnect from the current database."""
    if st.session_state.db_connection:
        st.session_state.db_connection.close()
    st.session_state.db_connection = None
    st.session_state.db_path = None


def get_database_tables() -> List[str]:
    """
    Get list of tables in the connected database.
    
    Returns:
        List of table names
    """
    if st.session_state.db_connection:
        try:
            cursor = st.session_state.db_connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            return [row[0] for row in cursor.fetchall()]
        except (sqlite3.DatabaseError, sqlite3.OperationalError):
            return []
    return []


def get_table_schema(table_name: str) -> List[dict]:
    """
    Get schema information for a table.
    
    Args:
        table_name: Name of the table
        
    Returns:
        List of column information dictionaries
    """
    # Validate table name to prevent injection
    if not _is_valid_table_name(table_name):
        return []
    
    if st.session_state.db_connection:
        try:
            cursor = st.session_state.db_connection.cursor()
            cursor.execute(f'PRAGMA table_info("{table_name}")')
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    'name': row[1],
                    'type': row[2],
                    'nullable': not row[3],
                    'default': row[4],
                    'pk': row[5]
                })
            return columns
        except (sqlite3.Error, TypeError):
            return []
    return []


def load_table_from_db(table_name: str) -> Optional[pd.DataFrame]:
    """
    Load a table from the connected database.
    
    Args:
        table_name: Name of the table to load
        
    Returns:
        DataFrame containing the table data, or None if error
    """
    # Validate table name to prevent injection
    if not _is_valid_table_name(table_name):
        st.error("Invalid table name")
        return None
    
    if st.session_state.db_connection:
        try:
            return pd.read_sql_query(f'SELECT * FROM "{table_name}"', st.session_state.db_connection)
        except Exception as e:
            st.error(f"Error loading table: {e}")
            return None
    return None


def execute_sql_query(sql: str) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Execute a SQL SELECT query on the database.
    
    Args:
        sql: SQL query to execute
        
    Returns:
        Tuple of (DataFrame or None, status message)
    """
    if not st.session_state.db_connection:
        return None, "No database connection"
    
    try:
        # Security check - only SELECT statements allowed
        sql_stripped = sql.strip().upper()
        if not sql_stripped.startswith('SELECT'):
            return None, "Only SELECT queries are allowed for security reasons"
        
        df = pd.read_sql_query(sql, st.session_state.db_connection)
        return df, "success"
    except Exception as e:
        return None, str(e)


def execute_raw_query(sql: str) -> Tuple[bool, str]:
    """
    Execute a raw SQL statement (INSERT, UPDATE, DELETE, CREATE, etc.).
    
    Args:
        sql: SQL statement to execute
        
    Returns:
        Tuple of (success boolean, status message)
    """
    if not st.session_state.db_connection:
        return False, "No database connection"
    
    try:
        cursor = st.session_state.db_connection.cursor()
        cursor.execute(sql)
        st.session_state.db_connection.commit()
        return True, f"Success: {cursor.rowcount} rows affected"
    except Exception as e:
        return False, str(e)


def save_to_database(df: pd.DataFrame, table_name: str, if_exists: str = "replace") -> bool:
    """
    Save a DataFrame to the database.
    
    Args:
        df: DataFrame to save
        table_name: Name of the table
        if_exists: How to handle existing table ('replace', 'append', 'fail')
        
    Returns:
        True if successful, False otherwise
    """
    # Validate table name to prevent injection
    if not _is_valid_table_name(table_name):
        st.warning("Invalid table name")
        return False
    
    if not st.session_state.db_connection:
        st.warning("No database connection - data not saved")
        return False
    
    try:
        df.to_sql(table_name, st.session_state.db_connection, if_exists=if_exists, index=False)
        return True
    except Exception as e:
        st.error(f"Error saving: {e}")
        return False


def is_connected() -> bool:
    """Check if database is connected."""
    return st.session_state.db_connection is not None


def get_connection_info() -> Optional[dict]:
    """Get connection information."""
    if st.session_state.db_path:
        return {
            'path': st.session_state.db_path,
            'filename': os.path.basename(st.session_state.db_path)
        }
    return None


# =============================================================================
# FILE UPLOAD HELPERS
# =============================================================================

def load_csv_file(file_path: str) -> Optional[pd.DataFrame]:
    """Load a CSV file into a DataFrame."""
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        st.error(f"Error loading CSV: {e}")
        return None


def load_excel_file(file_path: str, sheet: int = 0) -> Optional[pd.DataFrame]:
    """Load an Excel file into a DataFrame."""
    try:
        return pd.read_excel(file_path, sheet_name=sheet)
    except Exception as e:
        st.error(f"Error loading Excel: {e}")
        return None


def create_temp_file(uploaded_file) -> str:
    """
    Create a temporary file from an uploaded file.
    
    Args:
        uploaded_file: Streamlit uploaded file
        
    Returns:
        Path to temporary file
    """
    suffix = os.path.splitext(uploaded_file.name)[1]
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(uploaded_file.getvalue())
        return tmp.name


def load_uploaded_file(uploaded_file) -> Optional[pd.DataFrame]:
    """
    Load an uploaded file (CSV or Excel) into a DataFrame.
    
    Args:
        uploaded_file: Streamlit uploaded file
        
    Returns:
        DataFrame or None
    """
    file_ext = uploaded_file.name.split('.')[-1].lower()
    tmp_path = create_temp_file(uploaded_file)
    
    try:
        if file_ext == 'csv':
            return load_csv_file(tmp_path)
        elif file_ext in ['xlsx', 'xls']:
            return load_excel_file(tmp_path)
        else:
            st.error(f"Unsupported file type: {file_ext}")
            return None
    finally:
        # Clean up temp file
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
