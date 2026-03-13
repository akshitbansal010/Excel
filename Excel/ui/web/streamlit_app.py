"""
DataEngine Pro - Enhanced Streamlit Web UI (Excel-like Experience)
=====================================================================

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
    streamlit run ui/web/streamlit_app.py
"""

import streamlit as st

# Page configuration
st.set_page_config(
    page_title="DataEngine Pro - Excel Power, Python Speed",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Import all modules
import app_config
import app_state
import app_ui


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
