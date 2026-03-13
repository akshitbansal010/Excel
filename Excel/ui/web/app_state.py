"""
Session state management for DataEngine Pro Streamlit app.
Provides centralized state management for the application.
"""

import streamlit as st
from typing import Optional, Dict, Any, List

# Try to import adapter
try:
    from integration.adapter import Adapter, get_adapter
    ADAPTER_AVAILABLE = True
except ImportError:
    ADAPTER_AVAILABLE = False


def init_session_state():
    """
    Initialize all session state variables.
    Call this once at the start of the app.
    """
    
    # === Core Session ===
    _init_core_session()
    
    # === Data Management ===
    _init_data_management()
    
    # === Pagination ===
    _init_pagination()
    
    # === Sorting & Filtering ===
    _init_sort_filter()
    
    # === Custom Views ===
    _init_views()
    
    # === Column Selection ===
    _init_columns()
    
    # === SQL Query State ===
    _init_sql_state()
    
    # === Undo/Redo ===
    _init_undo_redo()
    
    # === Database Connection ===
    _init_database()


def _init_core_session():
    """Initialize core session state."""
    if 'adapter' not in st.session_state:
        if ADAPTER_AVAILABLE:
            try:
                st.session_state.adapter = get_adapter()
            except (ImportError, RuntimeError, AttributeError):
                st.session_state.adapter = None
        else:
            st.session_state.adapter = None
    
    if 'session_id' not in st.session_state:
        st.session_state.session_id = None


def _init_data_management():
    """Initialize data management state."""
    if 'current_table' not in st.session_state:
        st.session_state.current_table = None
    
    if 'session_tables' not in st.session_state:
        st.session_state.session_tables = {}
    
    # Original data backup (for "go back" functionality)
    if 'original_data' not in st.session_state:
        st.session_state.original_data = {}
    
    # Current working data (after filters/sorts)
    if 'working_data' not in st.session_state:
        st.session_state.working_data = {}


def _init_pagination():
    """Initialize pagination state."""
    if 'page' not in st.session_state:
        st.session_state.page = 1
    
    if 'page_size' not in st.session_state:
        from app_config import DEFAULT_PAGE_SIZE
        st.session_state.page_size = DEFAULT_PAGE_SIZE


def _init_sort_filter():
    """Initialize sorting and filtering state."""
    if 'sort_column' not in st.session_state:
        st.session_state.sort_column = None
    
    if 'sort_ascending' not in st.session_state:
        st.session_state.sort_ascending = True
    
    if 'active_filters' not in st.session_state:
        st.session_state.active_filters = []


def _init_views():
    """Initialize custom views state."""
    if 'saved_views' not in st.session_state:
        st.session_state.saved_views = {}
    
    if 'current_view' not in st.session_state:
        st.session_state.current_view = None


def _init_columns():
    """Initialize column selection state."""
    if 'selected_columns' not in st.session_state:
        st.session_state.selected_columns = None


def _init_sql_state():
    """Initialize SQL query state."""
    if 'sql_query_result' not in st.session_state:
        st.session_state.sql_query_result = None


def _init_undo_redo():
    """Initialize undo/redo state."""
    if 'undo_stack' not in st.session_state:
        st.session_state.undo_stack = []
    
    if 'redo_stack' not in st.session_state:
        st.session_state.redo_stack = []


def _init_database():
    """Initialize database connection state."""
    if 'db_connection' not in st.session_state:
        st.session_state.db_connection = None
    
    if 'db_path' not in st.session_state:
        st.session_state.db_path = None


def ensure_session() -> str:
    """Ensure a session exists."""
    if st.session_state.session_id is None:
        if st.session_state.adapter:
            try:
                session = st.session_state.adapter.create_session()
                st.session_state.session_id = session.id
            except (RuntimeError, AttributeError, ConnectionError):
                st.session_state.session_id = "local_session"
        else:
            st.session_state.session_id = "local_session"
    return st.session_state.session_id


def reset_to_original():
    """Reset current table to original data."""
    table_name = st.session_state.current_table
    
    if table_name and table_name in st.session_state.original_data:
        # Restore original
        st.session_state.working_data[table_name] = st.session_state.original_data[table_name].copy()
        
        # Clear all state
        st.session_state.active_filters = []
        st.session_state.sort_column = None
        st.session_state.page = 1
        st.session_state.selected_columns = None
        st.session_state.sql_query_result = None
        st.session_state.current_view = None


def add_to_undo_stack(action: str, data: Dict[str, Any]):
    """Add an operation to the undo stack."""
    from datetime import datetime
    from app_config import MAX_UNDO_HISTORY
    
    st.session_state.undo_stack.append({
        'action': action,
        'data': data,
        'timestamp': datetime.now().isoformat()
    })
    
    # Limit stack size
    if len(st.session_state.undo_stack) > MAX_UNDO_HISTORY:
        st.session_state.undo_stack.pop(0)


# =============================================================================
# GETTERS
# =============================================================================

def get_current_dataframe():
    """Get the current working DataFrame."""
    table_name = st.session_state.current_table
    
    if not table_name:
        return None
    
    # Check if we have filtered/sorted data
    if table_name in st.session_state.working_data:
        return st.session_state.working_data[table_name]
    
    # Fallback to session tables
    return st.session_state.session_tables.get(table_name)


def get_table_list() -> List[str]:
    """Get list of available tables."""
    return list(st.session_state.session_tables.keys())


def is_large_dataset(df) -> bool:
    """Check if dataframe is considered large."""
    from app_config import LARGE_DATASET_THRESHOLD
    return df is not None and len(df) > LARGE_DATASET_THRESHOLD
