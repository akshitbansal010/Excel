"""
Custom views module for DataEngine Pro Streamlit app.
Handles saving and loading custom view configurations.
"""

import json
import os
import streamlit as st
from typing import Dict, Any, Optional, List
from datetime import datetime


VIEWS_FILE_PATH = os.path.expanduser("~/.dataengine_pro/saved_views.json")


# =============================================================================
# VIEW MANAGEMENT
# =============================================================================

def save_current_view(view_name: str) -> bool:
    """
    Save current view configuration.
    
    Args:
        view_name: Name for the view
        
    Returns:
        True if successful, False otherwise
    """
    table_name = st.session_state.current_table
    
    if not table_name:
        return False
    
    view_config = {
        'table': table_name,
        'filters': st.session_state.active_filters.copy(),
        'sort_column': st.session_state.sort_column,
        'sort_ascending': st.session_state.sort_ascending,
        'selected_columns': st.session_state.selected_columns,
        'page_size': st.session_state.page_size,
        'created_at': datetime.now().isoformat()
    }
    
    st.session_state.saved_views[view_name] = view_config
    st.session_state.current_view = view_name
    
    # Save to file for persistence
    _save_views_to_file()
    
    return True


def load_view(view_name: str) -> bool:
    """
    Load a saved view.
    
    Args:
        view_name: Name of the view to load
        
    Returns:
        True if successful, False otherwise
    """
    if view_name not in st.session_state.saved_views:
        return False
    
    view_config = st.session_state.saved_views[view_name]
    
    # Apply view settings
    st.session_state.active_filters = view_config.get('filters', []).copy()
    st.session_state.sort_column = view_config.get('sort_column')
    st.session_state.sort_ascending = view_config.get('sort_ascending', True)
    st.session_state.selected_columns = view_config.get('selected_columns')
    st.session_state.page_size = view_config.get('page_size', 100)
    st.session_state.page = 1
    st.session_state.current_view = view_name
    
    return True


def delete_view(view_name: str) -> bool:
    """
    Delete a saved view.
    
    Args:
        view_name: Name of the view to delete
        
    Returns:
        True if successful, False otherwise
    """
    if view_name not in st.session_state.saved_views:
        return False
    
    del st.session_state.saved_views[view_name]
    
    if st.session_state.current_view == view_name:
        st.session_state.current_view = None
    
    _save_views_to_file()
    
    return True


def get_saved_views() -> Dict[str, Any]:
    """Get all saved views."""
    return st.session_state.saved_views


def get_view_names() -> List[str]:
    """Get list of saved view names."""
    return list(st.session_state.saved_views.keys())


# =============================================================================
# FILE PERSISTENCE
# =============================================================================

def _save_views_to_file():
    """Save views to JSON file."""
    try:
        views_dir = os.path.dirname(VIEWS_FILE_PATH)
        os.makedirs(views_dir, exist_ok=True)
        
        with open(VIEWS_FILE_PATH, 'w') as f:
            json.dump(st.session_state.saved_views, f, indent=2, default=str)
    except Exception as e:
        st.warning(f"Could not save views to file: {e}")


def load_views_from_file():
    """Load views from JSON file."""
    try:
        if os.path.exists(VIEWS_FILE_PATH):
            with open(VIEWS_FILE_PATH, 'r') as f:
                loaded_views = json.load(f)
                # Merge with existing views
                for name, config in loaded_views.items():
                    if name not in st.session_state.saved_views:
                        st.session_state.saved_views[name] = config
    except Exception as e:
        pass  # Ignore if can't load


def export_view(view_name: str) -> Optional[str]:
    """
    Export a view to JSON string.
    
    Args:
        view_name: Name of the view to export
        
    Returns:
        JSON string or None if view not found
    """
    if view_name in st.session_state.saved_views:
        return json.dumps(st.session_state.saved_views[view_name], indent=2)
    return None


def validate_view_config(view_config: Any) -> tuple[bool, str]:
    """
    Validate view configuration structure and types.
    
    Args:
        view_config: The view configuration to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check if it's a dictionary
    if not isinstance(view_config, dict):
        return False, "View configuration must be a JSON object (dictionary)"
    
    # Check for required 'table' key
    if 'table' not in view_config:
        return False, "Missing required key: 'table'"
    
    if not isinstance(view_config['table'], str):
        return False, "Key 'table' must be a string"
    
    # Validate optional but expected keys with specific types
    if 'filters' in view_config:
        if not isinstance(view_config['filters'], list):
            return False, "Key 'filters' must be a list"
    
    if 'sort_column' in view_config:
        if view_config['sort_column'] is not None and not isinstance(view_config['sort_column'], str):
            return False, "Key 'sort_column' must be a string or null"
    
    if 'sort_ascending' in view_config:
        if not isinstance(view_config['sort_ascending'], bool):
            return False, "Key 'sort_ascending' must be a boolean (true/false)"
    
    if 'selected_columns' in view_config:
        if view_config['selected_columns'] is not None and not isinstance(view_config['selected_columns'], list):
            return False, "Key 'selected_columns' must be a list or null"
    
    if 'page_size' in view_config:
        if isinstance(view_config['page_size'], bool) or not isinstance(view_config['page_size'], int) or view_config['page_size'] <= 0:
            return False, "Key 'page_size' must be a positive integer"
    
    if 'created_at' in view_config:
        if not isinstance(view_config['created_at'], str):
            return False, "Key 'created_at' must be a string (ISO datetime)"
    
    return True, ""


def import_view(view_name: str, view_json: str) -> bool:
    """
    Import a view from JSON string.
    
    Args:
        view_name: Name for the imported view
        view_json: JSON string containing view configuration
        
    Returns:
        True if successful, False otherwise
    """
    try:
        view_config = json.loads(view_json)
    except json.JSONDecodeError as e:
        st.error(f"Invalid JSON format: {e}")
        return False
    except Exception as e:
        st.error(f"Error parsing JSON: {e}")
        return False
    
    # Validate view configuration structure
    is_valid, error_message = validate_view_config(view_config)
    if not is_valid:
        st.error(f"Invalid view configuration: {error_message}")
        return False
    
    # Store and save validated configuration
    st.session_state.saved_views[view_name] = view_config
    _save_views_to_file()
    return True


# =============================================================================
# VIEW UI HELPERS
# =============================================================================

def get_view_info(view_name: str) -> Optional[Dict[str, Any]]:
    """
    Get information about a view.
    
    Args:
        view_name: Name of the view
        
    Returns:
        Dictionary with view information or None
    """
    if view_name not in st.session_state.saved_views:
        return None
    
    config = st.session_state.saved_views[view_name]
    
    return {
        'name': view_name,
        'table': config.get('table'),
        'filters_count': len(config.get('filters', [])),
        'has_sort': config.get('sort_column') is not None,
        'selected_columns': config.get('selected_columns'),
        'created_at': config.get('created_at')
    }


def clear_current_view():
    """Clear the current view without deleting it."""
    st.session_state.current_view = None
    st.session_state.selected_columns = None
