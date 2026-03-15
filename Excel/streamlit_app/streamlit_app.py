"""
DataEngine Pro - Enhanced Streamlit Web UI (Excel-like Experience)
===================================================================

A comprehensive visual interface for DataEngine Pro with:
- Database connection (SQLite, CSV, Excel)
- Excel-like data viewing with large dataset support (no crashes)
- Sorting and filtering (real-time updates)
- Custom views with save/load functionality
- Column management (add/delete/rename)
- Full SQL query execution
- SQL-based column creation
- Go back to original data functionality

Usage:
    cd Excel
    streamlit run streamlit_app/streamlit_app.py
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st

# Page configuration
st.set_page_config(
    page_title="DataEngine Pro - Excel Power, Python Speed",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Import all modules (from same package)
import streamlit_app.app_config as app_config
import streamlit_app.app_state as app_state
import streamlit_app.app_ui as app_ui


# =============================================================================
# MAIN APPLICATION
# =============================================================================

def main():
    """
    Main application entry point.
    """
    # Render custom CSS
    app_ui.render_custom_css()
    
    # Initialize session state
    app_state.init_session_state()
    
    # Ensure session exists
    app_state.ensure_session()
    
    # Render UI
    app_ui.render_left_panel()
    app_ui.render_center_panel()


if __name__ == "__main__":
    main()
