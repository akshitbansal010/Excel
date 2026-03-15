"""
Data operations module for DataEngine Pro Streamlit app.
Handles filtering, sorting, pagination, and column operations.
"""

import pandas as pd
import numpy as np
import streamlit as st
from typing import Optional, List, Tuple, Any, Dict
from datetime import datetime

import streamlit_app.app_state
import streamlit_app.app_config


# =============================================================================
# DATA RETRIEVAL
# =============================================================================

def get_current_dataframe() -> Optional[pd.DataFrame]:
    """Get the current working DataFrame."""
    return streamlit_app.app_state.get_current_dataframe()


def get_filtered_dataframe() -> pd.DataFrame:
    """
    Get DataFrame with filters and sorting applied.
    
    Returns:
        Filtered and sorted DataFrame
    """
    df = get_current_dataframe()
    
    if df is None:
        return pd.DataFrame()
    
    # Apply filters
    df = _apply_filters(df)
    
    # Apply sorting
    df = _apply_sorting(df)
    
    return df


def _apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Apply active filters to DataFrame."""
    if not st.session_state.active_filters:
        return df
    
    for filter_def in st.session_state.active_filters:
        col = filter_def['column']
        op = filter_def['operator']
        val = filter_def['value']
        
        if col not in df.columns:
            continue
        
        try:
            df = _apply_single_filter(df, col, op, val)
        except Exception as e:
            st.error(f"Filter error on {col}: {e}")
    
    return df


def _apply_single_filter(df: pd.DataFrame, column: str, operator: str, value: Any) -> pd.DataFrame:
    """Apply a single filter condition."""
    
    if operator == "==":
        return df[df[column] == value]
    elif operator == "!=":
        return df[df[column] != value]
    elif operator == ">":
        return df[df[column] > value]
    elif operator == "<":
        return df[df[column] < value]
    elif operator == ">=":
        return df[df[column] >= value]
    elif operator == "<=":
        return df[df[column] <= value]
    elif operator == "contains":
        return df[df[column].astype(str).str.contains(str(value), case=False, na=False)]
    elif operator == "startswith":
        return df[df[column].astype(str).str.startswith(str(value), na=False)]
    elif operator == "endswith":
        return df[df[column].astype(str).str.endswith(str(value), na=False)]
    elif operator == "is_null":
        return df[df[column].isna()]
    elif operator == "is_not_null":
        return df[df[column].notna()]
    elif operator == "is_blank":
        return df[df[column].isna() | (df[column].astype(str).str.strip() == "")]
    elif operator == "in":
        return df[df[column].isin(value)]
    
    return df


def _apply_sorting(df: pd.DataFrame) -> pd.DataFrame:
    """Apply sorting to DataFrame."""
    sort_col = st.session_state.sort_column
    
    if sort_col and sort_col in df.columns:
        return df.sort_values(
            by=sort_col, 
            ascending=st.session_state.sort_ascending
        )
    
    return df


# =============================================================================
# PAGINATION
# =============================================================================

def get_paginated_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, int, int]:
    """
    Get paginated data for large datasets.
    
    Returns:
        Tuple of (paginated DataFrame, start index, end index)
    """
    total_rows = len(df)
    page_size = st.session_state.page_size
    total_pages = max(1, (total_rows - 1) // page_size + 1)
    
    # Ensure page is valid
    if st.session_state.page > total_pages:
        st.session_state.page = 1
    
    # Calculate slice
    start_idx = (st.session_state.page - 1) * page_size
    end_idx = min(start_idx + page_size, total_rows)
    
    paginated_df = df.iloc[start_idx:end_idx].copy()
    
    return paginated_df, start_idx + 1, end_idx


def change_page(new_page: int):
    """Change the current page."""
    total_rows = len(get_filtered_dataframe())
    total_pages = max(1, (total_rows - 1) // st.session_state.page_size + 1)
    
    if 1 <= new_page <= total_pages:
        st.session_state.page = new_page


def change_page_size(new_size: int):
    """Change the page size."""
    st.session_state.page_size = new_size
    st.session_state.page = 1


# =============================================================================
# FILTER OPERATIONS
# =============================================================================

def add_filter(column: str, operator: str, value: Any):
    """
    Add a filter to the active filters.
    
    Args:
        column: Column name to filter
        operator: Filter operator
        value: Filter value
    """
    # Remove existing filter on same column
    st.session_state.active_filters = [
        f for f in st.session_state.active_filters 
        if f['column'] != column
    ]
    
    # Add new filter
    st.session_state.active_filters.append({
        'column': column,
        'operator': operator,
        'value': value
    })
    
    # Reset page
    st.session_state.page = 1
    
    # Add to undo stack
    streamlit_app.app_state.add_to_undo_stack('add_filter', {
        'column': column,
        'operator': operator,
        'value': value
    })


def remove_filter(index: int):
    """Remove a filter by index."""
    if 0 <= index < len(st.session_state.active_filters):
        removed = st.session_state.active_filters.pop(index)
        
        streamlit_app.app_state.add_to_undo_stack('remove_filter', removed)
        st.session_state.page = 1


def clear_all_filters():
    """Clear all active filters."""
    st.session_state.active_filters = []
    st.session_state.page = 1


def get_filter_operators(column_type) -> List[str]:
    """Get available filter operators based on column type."""
    if pd.api.types.is_numeric_dtype(column_type):
        return streamlit_app.app_config.NUMERIC_OPERATORS
    else:
        return streamlit_app.app_config.TEXT_OPERATORS


# =============================================================================
# SORT OPERATIONS
# =============================================================================

def apply_sort(column: str, ascending: bool = True):
    """
    Apply sorting to the data.
    
    Args:
        column: Column name to sort by
        ascending: Sort direction
    """
    st.session_state.sort_column = column
    st.session_state.sort_ascending = ascending
    st.session_state.page = 1
    
    streamlit_app.app_state.add_to_undo_stack('sort', {
        'column': column,
        'ascending': ascending
    })


def clear_sort():
    """Clear sorting."""
    st.session_state.sort_column = None
    st.session_state.sort_ascending = True
    st.session_state.page = 1


# =============================================================================
# COLUMN OPERATIONS
# =============================================================================

def add_column(column_name: str, default_value: Any = None) -> bool:
    """
    Add a new column to the current table.
    
    Args:
        column_name: Name of the new column
        default_value: Default value for the column
        
    Returns:
        True if successful, False otherwise
    """
    table_name = st.session_state.current_table
    
    if not table_name or table_name not in st.session_state.session_tables:
        return False
    
    df = st.session_state.session_tables[table_name]
    
    if column_name in df.columns:
        st.error(f"Column '{column_name}' already exists")
        return False
    
    # Add new column
    df[column_name] = default_value
    
    # Update working data
    if table_name in st.session_state.working_data:
        st.session_state.working_data[table_name][column_name] = default_value
    
    # Add to undo stack
    streamlit_app.app_state.add_to_undo_stack('add_column', {
        'column': column_name,
        'default_value': default_value
    })
    
    return True


def delete_column(column_name: str) -> bool:
    """
    Delete a column from the current table.
    
    Args:
        column_name: Name of the column to delete
        
    Returns:
        True if successful, False otherwise
    """
    table_name = st.session_state.current_table
    
    if not table_name or table_name not in st.session_state.session_tables:
        return False
    
    df = st.session_state.session_tables[table_name]
    
    if column_name not in df.columns:
        st.error(f"Column '{column_name}' not found")
        return False
    
    # Store column data for undo
    column_data = df[column_name].copy()
    
    # Drop column
    df.drop(columns=[column_name], inplace=True)
    
    # Update working data
    if table_name in st.session_state.working_data:
        st.session_state.working_data[table_name].drop(columns=[column_name], inplace=True)
    
    # Add to undo stack
    streamlit_app.app_state.add_to_undo_stack('delete_column', {
        'column': column_name,
        'data': column_data.to_dict()
    })
    
    return True


def rename_column(old_name: str, new_name: str) -> bool:
    """
    Rename a column.
    
    Args:
        old_name: Current column name
        new_name: New column name
        
    Returns:
        True if successful, False otherwise
    """
    table_name = st.session_state.current_table
    
    if not table_name or table_name not in st.session_state.session_tables:
        return False
    
    df = st.session_state.session_tables[table_name]
    
    if old_name not in df.columns:
        st.error(f"Column '{old_name}' not found")
        return False
    
    if new_name in df.columns:
        st.error(f"Column '{new_name}' already exists")
        return False
    
    # Rename
    df.rename(columns={old_name: new_name}, inplace=True)
    
    # Update working data
    if table_name in st.session_state.working_data:
        st.session_state.working_data[table_name].rename(columns={old_name: new_name}, inplace=True)
    
    # Add to undo stack
    streamlit_app.app_state.add_to_undo_stack('rename_column', {
        'old_name': old_name,
        'new_name': new_name
    })
    
    return True


def add_column_from_expression(expression: str, new_column_name: str) -> bool:
    """
    Add a new column using a pandas expression.
    
    Args:
        expression: Pandas eval expression
        new_column_name: Name for the new column
        
    Returns:
        True if successful, False otherwise
    """
    df = get_current_dataframe()
    
    if df is None or df.empty:
        st.error("No data loaded")
        return False
    
    try:
        # Use pandas eval for calculated columns
        df[new_column_name] = df.eval(expression)
        
        # Update working data
        table_name = st.session_state.current_table
        if table_name in st.session_state.working_data:
            st.session_state.working_data[table_name][new_column_name] = df[new_column_name]
        
        # Add to undo stack
        streamlit_app.app_state.add_to_undo_stack('add_column_sql', {
            'column': new_column_name,
            'expression': expression
        })
        
        return True
        
    except Exception as e:
        st.error(f"Error creating column: {e}")
        return False


# =============================================================================
# DATA STORAGE
# =============================================================================

def store_dataframe(table_name: str, df: pd.DataFrame):
    """
    Store a DataFrame in session state with backup.
    
    Args:
        table_name: Name for the table
        df: DataFrame to store
    """
    # Store original data if new table
    if table_name not in st.session_state.original_data:
        st.session_state.original_data[table_name] = df.copy()
    
    # Store in session tables
    st.session_state.session_tables[table_name] = df
    
    # Reset working data to match
    st.session_state.working_data[table_name] = df.copy()
    
    # Set as current table
    st.session_state.current_table = table_name
    
    # Reset filters and sorts
    st.session_state.active_filters = []
    st.session_state.sort_column = None
    st.session_state.page = 1
    st.session_state.selected_columns = None
    st.session_state.sql_query_result = None
    st.session_state.current_view = None


# =============================================================================
# UNDO OPERATIONS
# =============================================================================

def undo_operation():
    """Undo the last operation."""
    if not st.session_state.undo_stack:
        return
    
    # Pop last operation
    op = st.session_state.undo_stack.pop()
    
    # Add to redo stack
    st.session_state.redo_stack.append(op)
    
    # Handle different operation types
    action = op.get('action')
    data = op.get('data', {})
    table_name = st.session_state.current_table
    
    if action == 'add_filter':
        # Remove the filter that was added
        filters = st.session_state.active_filters
        for i, f in enumerate(filters):
            if (f['column'] == data.get('column') and 
                f['operator'] == data.get('operator') and
                f['value'] == data.get('value')):
                filters.pop(i)
                break
    
    elif action == 'remove_filter':
        # Re-add the removed filter
        st.session_state.active_filters.append({
            'column': data.get('column'),
            'operator': data.get('operator'),
            'value': data.get('value')
        })
    
    elif action == 'sort':
        # Clearing sort is often safer than trying to restore previous state if not tracked
        st.session_state.sort_column = None
        st.session_state.sort_ascending = True
    
    elif action == 'add_column' or action == 'add_column_sql':
        if table_name in st.session_state.session_tables:
            col_name = data.get('column')
            st.session_state.session_tables[table_name].drop(columns=[col_name], inplace=True)
            if table_name in st.session_state.working_data:
                st.session_state.working_data[table_name].drop(columns=[col_name], inplace=True)
    
    elif action == 'delete_column':
        if table_name in st.session_state.session_tables:
            col_name = data.get('column')
            col_data = pd.Series(data.get('data'))
            st.session_state.session_tables[table_name][col_name] = col_data
            if table_name in st.session_state.working_data:
                st.session_state.working_data[table_name][col_name] = col_data
    
    elif action == 'rename_column':
        if table_name in st.session_state.session_tables:
            old_name = data.get('old_name')
            new_name = data.get('new_name')
            st.session_state.session_tables[table_name].rename(columns={old_name: new_name}, inplace=True)
            if table_name in st.session_state.working_data:
                st.session_state.working_data[table_name].rename(columns={old_name: new_name}, inplace=True)
    
    # Reset page
    st.session_state.page = 1


def redo_operation():
    """Redo the last undone operation."""
    if not st.session_state.redo_stack:
        return
    
    # Pop from redo stack
    op = st.session_state.redo_stack.pop()
    
    # Add back to undo stack
    st.session_state.undo_stack.append(op)
    
    # Handle different operation types
    action = op.get('action')
    data = op.get('data', {})
    table_name = st.session_state.current_table
    
    if action == 'add_filter':
        # Re-add the filter
        st.session_state.active_filters.append({
            'column': data.get('column'),
            'operator': data.get('operator'),
            'value': data.get('value')
        })
    
    elif action == 'remove_filter':
        # Re-remove the filter
        filters = st.session_state.active_filters
        for i, f in enumerate(filters):
            if (f['column'] == data.get('column') and 
                f['operator'] == data.get('operator') and
                f['value'] == data.get('value')):
                filters.pop(i)
                break
    
    elif action == 'sort':
        st.session_state.sort_column = data.get('column')
        st.session_state.sort_ascending = data.get('ascending', True)
    
    elif action == 'add_column':
        add_column(data.get('column'), data.get('default_value'))
        # Pop from undo stack because add_column adds it back
        st.session_state.undo_stack.pop()
    
    elif action == 'delete_column':
        delete_column(data.get('column'))
        # Pop from undo stack because delete_column adds it back
        st.session_state.undo_stack.pop()
    
    elif action == 'rename_column':
        rename_column(data.get('old_name'), data.get('new_name'))
        # Pop from undo stack because rename_column adds it back
        st.session_state.undo_stack.pop()
    
    elif action == 'add_column_sql':
        add_column_from_expression(data.get('expression'), data.get('column'))
        # Pop from undo stack because add_column_from_expression adds it back
        st.session_state.undo_stack.pop()
    
    st.session_state.page = 1
