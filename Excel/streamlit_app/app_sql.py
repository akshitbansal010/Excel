"""
SQL query module for DataEngine Pro Streamlit app.
Handles SQL query execution and results.
"""

import pandas as pd
import streamlit as st
from typing import Tuple, Optional

import streamlit_app.app_data_ops
import streamlit_app.app_database

# Try to import DuckDB engine from shared
try:
    from shared.excelpy.sql_engine import run_sql_query
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False


# =============================================================================
# QUERY EXECUTION
# =============================================================================

def execute_query(sql: str) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Execute SQL query on current data.
    Uses DuckDB for real SQL if available, fallback to pandas query.
    
    Args:
        sql: SQL query (DuckDB) or pandas query string
        
    Returns:
        Tuple of (DataFrame or None, status message)
    """
    df = streamlit_app.app_data_ops.get_current_dataframe()
    
    if df is None or df.empty:
        return None, "No data loaded"
    
    # Validate query to prevent code injection (basic check)
    dangerous_keywords = ['import', 'getattr', 'setattr', 'eval', 'exec', 'compile', 'open', 'file', 'os.', 'sys.', '__import__']
    if any(keyword in sql.lower() for keyword in dangerous_keywords):
        return None, "Query contains disallowed keywords"
    
    try:
        if DUCKDB_AVAILABLE:
            # Use real SQL engine
            result_df = run_sql_query(df, sql)
            return result_df, "success"
        else:
            # Fallback to pandas query (less powerful, but works)
            # This expects a pandas expression, not SQL
            # Examples: "age > 25", "name == 'John'"
            result_df = df.query(sql, local_dict={}, global_dict={})
            return result_df, "success"
    except Exception as e:
        return None, f"Query error: {str(e)}. Note: Without DuckDB installed, use pandas expressions (e.g., 'age > 25') instead of SQL."


def execute_database_query(sql: str) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Execute SQL query on connected database.
    
    Args:
        sql: SQL SELECT query
        
    Returns:
        Tuple of (DataFrame or None, status message)
    """
    return streamlit_app.app_database.execute_sql_query(sql)


def store_query_result(df: pd.DataFrame):
    """
    Store query result in session state.
    
    Args:
        df: DataFrame to store
    """
    st.session_state.sql_query_result = df


def clear_query_result():
    """Clear stored query result."""
    st.session_state.sql_query_result = None


def get_query_result() -> Optional[pd.DataFrame]:
    """Get stored query result."""
    return st.session_state.sql_query_result


def has_query_result() -> bool:
    """Check if there's a stored query result."""
    return st.session_state.sql_query_result is not None


# =============================================================================
# QUERY HELPERS
# =============================================================================

def get_column_names() -> list:
    """Get list of column names from current data."""
    df = streamlit_app.app_data_ops.get_current_dataframe()
    if df is not None:
        return list(df.columns)
    return []


def get_column_type(column_name: str) -> str:
    """Get the data type of a column."""
    df = streamlit_app.app_data_ops.get_current_dataframe()
    if df is not None and column_name in df.columns:
        return str(df[column_name].dtype)
    return "unknown"


def build_query_from_filters() -> str:
    """
    Build a SQL-like query string from active filters.
    
    Returns:
        Query string
    """
    filters = st.session_state.active_filters
    
    if not filters:
        return ""
    
    query_parts = []
    
    for f in filters:
        col = f['column']
        op = f['operator']
        val = f['value']
        
        if op == "==":
            if isinstance(val, str):
                query_parts.append(f'{col} == "{val}"')
            else:
                query_parts.append(f'{col} == {val}')
        elif op == "!=":
            if isinstance(val, str):
                query_parts.append(f'{col} != "{val}"')
            else:
                query_parts.append(f'{col} != {val}')
        elif op == ">":
            query_parts.append(f'{col} > {val}')
        elif op == "<":
            query_parts.append(f'{col} < {val}')
        elif op == ">=":
            query_parts.append(f'{col} >= {val}')
        elif op == "<=":
            query_parts.append(f'{col} <= {val}')
        elif op == "contains":
            query_parts.append(f'{col}.str.contains("{val}")')
        elif op == "startswith":
            query_parts.append(f'{col}.str.startswith("{val}")')
        elif op == "endswith":
            query_parts.append(f'{col}.str.endswith("{val}")')
    
    return " and ".join(query_parts)


def get_example_queries() -> dict:
    """Get example query templates."""
    if DUCKDB_AVAILABLE:
        return {
            "Select all": "SELECT * FROM df",
            "Filter rows": "SELECT * FROM df WHERE age > 25",
            "Aggregation": "SELECT department, COUNT(*) as count FROM df GROUP BY department",
            "Average salary": "SELECT AVG(salary) FROM df",
            "Top N": "SELECT * FROM df ORDER BY salary DESC LIMIT 5",
            "Distinct values": "SELECT DISTINCT category FROM df"
        }
    return {
        "Simple equality": "column_name == 'value'",
        "Numeric comparison": "age > 25",
        "Multiple conditions": "(age > 25) and (status == 'Active')",
        "Text contains": "name.str.contains('John')",
        "In list": "status.isin(['Active', 'Pending'])",
        "Numeric range": "(age >= 18) and (age <= 65)",
        "Not null": "column_name.notna()",
        "Null values": "column_name.isna()"
    }


# =============================================================================
# APPLY QUERY AS FILTER
# =============================================================================

def apply_query_as_filter():
    """
    Apply the current query result as a filter.
    This replaces the working data with the query result.
    """
    if st.session_state.sql_query_result is None:
        return False
    
    table_name = st.session_state.current_table
    if table_name and table_name in st.session_state.working_data:
        st.session_state.working_data[table_name] = st.session_state.sql_query_result.copy()
        
        # Clear query result after applying
        st.session_state.sql_query_result = None
        
        return True
    
    return False
