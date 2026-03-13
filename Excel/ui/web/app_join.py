"""
Table join operations module for DataEngine Pro Streamlit app.
Handles VLOOKUP-like operations and SQL JOINs.
"""

import pandas as pd
import streamlit as st
from typing import Optional, List, Tuple
import app_state
import app_data_ops


# =============================================================================
# JOIN TYPES
# =============================================================================

JOIN_TYPES = {
    "LEFT JOIN": "Keep all rows from left table, matching rows from right",
    "RIGHT JOIN": "Keep all rows from right table, matching rows from left",
    "INNER JOIN": "Only keep rows that match in both tables",
    "FULL OUTER JOIN": "Keep all rows from both tables",
    "VLOOKUP (Left)": "Like Excel VLOOKUP - add right table columns to left",
    "VLOOKUP (Right)": "Add left table columns to right table based on match"
}


# =============================================================================
# JOIN OPERATIONS
# =============================================================================

def join_tables(
    left_table: str,
    right_table: str,
    left_key: str,
    right_key: str,
    join_type: str = "LEFT JOIN",
    select_columns: Optional[List[str]] = None
) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Join two tables based on matching keys.
    
    Args:
        left_table: Name of left table
        right_table: Name of right table  
        left_key: Column name in left table to match on
        right_key: Column name in right table to match on
        join_type: Type of join to perform
        select_columns: Optional list of columns to include
        
    Returns:
        Tuple of (DataFrame or None, status message)
    """
    # Get tables
    tables = st.session_state.session_tables
    
    if left_table not in tables:
        return None, f"Table '{left_table}' not found"
    
    if right_table not in tables:
        return None, f"Table '{right_table}' not found"
    
    left_df = tables[left_table]
    right_df = tables[right_table]
    
    # Validate keys
    if left_key not in left_df.columns:
        return None, f"Column '{left_key}' not found in {left_table}"
    
    if right_key not in right_df.columns:
        return None, f"Column '{right_key}' not found in {right_table}"
    
    try:
        # Determine merge type
        how_map = {
            "LEFT JOIN": "left",
            "RIGHT JOIN": "right", 
            "INNER JOIN": "inner",
            "FULL OUTER JOIN": "outer",
            "VLOOKUP (Left)": "left",
            "VLOOKUP (Right)": "right"
        }
        
        how = how_map.get(join_type, "left")
        
        # For VLOOKUP style, rename right key to match left key
        if "VLOOKUP" in join_type:
            right_df = right_df.copy()
            right_df.rename(columns={right_key: left_key}, inplace=True)
            merge_key = left_key
        else:
            merge_key = [left_key, right_key]
        
        # Perform merge
        result = pd.merge(
            left_df,
            right_df,
            left_on=merge_key if isinstance(merge_key, str) else None,
            right_on=merge_key if isinstance(merge_key, str) else None,
            how=how,
            suffixes=('_left', '_right')
        )
        
        # Handle column selection
        if select_columns:
            available_cols = [c for c in select_columns if c in result.columns]
            if available_cols:
                result = result[available_cols]
        
        return result, "success"
        
    except Exception as e:
        return None, f"Join error: {str(e)}"


def create_lookup_column(
    source_table: str,
    lookup_table: str,
    source_key: str,
    lookup_key: str,
    lookup_column: str,
    new_column_name: str,
    aggregation: str = "first"
) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Create a new column by looking up values from another table (VLOOKUP-like).
    
    Args:
        source_table: Name of table to add column to
        lookup_table: Name of table to lookup from
        source_key: Column in source table to match
        lookup_key: Column in lookup table to match
        lookup_column: Column to retrieve from lookup table
        new_column_name: Name for new column
        aggregation: How to handle multiple matches ('first', 'last', 'mean', etc.)
        
    Returns:
        Tuple of (DataFrame or None, status message)
    """
    tables = st.session_state.session_tables
    
    if source_table not in tables:
        return None, f"Table '{source_table}' not found"
    
    if lookup_table not in tables:
        return None, f"Table '{lookup_table}' not found"
    
    source_df = tables[source_table]
    lookup_df = tables[lookup_table]
    
    # Validate columns
    if source_key not in source_df.columns:
        return None, f"Column '{source_key}' not found in {source_table}"
    
    if lookup_key not in lookup_df.columns:
        return None, f"Column '{lookup_key}' not found in {lookup_table}"
    
    if lookup_column not in lookup_df.columns:
        return None, f"Column '{lookup_column}' not found in {lookup_table}"
    
    try:
        result = source_df.copy()
        
        # Create lookup series
        lookup_series = lookup_df.drop_duplicates(subset=[lookup_key]).set_index(lookup_key)[lookup_column]
        
        # Map values
        result[new_column_name] = result[source_key].map(lookup_series)
        
        return result, "success"
        
    except Exception as e:
        return None, f"Lookup error: {str(e)}"


def concatenate_tables(
    table1: str,
    table2: str,
    join_type: str = "vertical"
) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Concatenate two tables (stack rows or merge columns).
    
    Args:
        table1: First table name
        table2: Second table name
        join_type: 'vertical' (stack rows) or 'horizontal' (merge columns)
        
    Returns:
        Tuple of (DataFrame or None, status message)
    """
    tables = st.session_state.session_tables
    
    if table1 not in tables:
        return None, f"Table '{table1}' not found"
    
    if table2 not in tables:
        return None, f"Table '{table2}' not found"
    
    df1 = tables[table1]
    df2 = tables[table2]
    
    try:
        if join_type == "vertical":
            # Stack rows (like SQL UNION)
            result = pd.concat([df1, df2], ignore_index=True)
        else:
            # Merge columns (like SQL UNION ALL with different columns)
            result = pd.concat([df1, df2], axis=1)
        
        return result, "success"
        
    except Exception as e:
        return None, f"Concatenation error: {str(e)}"


# =============================================================================
# TABLE MANAGEMENT
# =============================================================================

def get_available_tables() -> List[str]:
    """Get list of available table names."""
    return list(st.session_state.session_tables.keys())


def get_table_columns(table_name: str) -> List[str]:
    """Get list of column names for a table."""
    if table_name in st.session_state.session_tables:
        return list(st.session_state.session_tables[table_name].columns)
    return []


def store_as_new_table(table_name: str, df: pd.DataFrame) -> bool:
    """
    Store result as a new table.
    
    Args:
        table_name: Name for the new table
        df: DataFrame to store
        
    Returns:
        True if successful
    """
    app_data_ops.store_dataframe(table_name, df)
    return True


def update_existing_table(table_name: str, df: pd.DataFrame) -> bool:
    """
    Update an existing table with new data.
    
    Args:
        table_name: Name of table to update
        df: New DataFrame
        
    Returns:
        True if successful
    """
    if table_name not in st.session_state.session_tables:
        return False
    
    # Store in session tables
    st.session_state.session_tables[table_name] = df
    
    # Update working data
    st.session_state.working_data[table_name] = df.copy()
    
    return True


# =============================================================================
# JOIN UI HELPERS
# =============================================================================

def get_join_type_description(join_type: str) -> str:
    """Get description of join type."""
    return JOIN_TYPES.get(join_type, "")


def validate_join(
    left_table: str,
    right_table: str,
    left_key: str,
    right_key: str
) -> Tuple[bool, str]:
    """
    Validate join parameters.
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    if left_table == right_table:
        return False, "Cannot join a table to itself"
    
    tables = st.session_state.session_tables
    
    if left_table not in tables:
        return False, f"Table '{left_table}' not found"
    
    if right_table not in tables:
        return False, f"Table '{right_table}' not found"
    
    if left_key not in tables[left_table].columns:
        return False, f"Column '{left_key}' not found in {left_table}"
    
    if right_key not in tables[right_table].columns:
        return False, f"Column '{right_key}' not found in {right_table}"
    
    return True, ""
