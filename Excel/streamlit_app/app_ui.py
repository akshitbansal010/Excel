"""
UI rendering module for DataEngine Pro Streamlit app.
Handles all UI components and rendering.
"""

import streamlit as st
import pandas as pd
import numpy as np
from typing import List

import streamlit_app.app_config
import streamlit_app.app_state
import streamlit_app.app_data_ops
import streamlit_app.app_database
import streamlit_app.app_views
import streamlit_app.app_sql
import streamlit_app.app_join


# =============================================================================
# CSS STYLING
# =============================================================================

def render_custom_css():
    """Render custom CSS for the application."""
    st.markdown("""
    <style>
        /* Main layout */
        .block-container {
            padding-top: 0.5rem;
            padding-bottom: 0.5rem;
        }
        
        /* Panel styling */
        .panel-header {
            background-color: #107C41;
            color: white;
            padding: 8px 12px;
            border-radius: 4px 4px 0 0;
            font-weight: bold;
            font-size: 14px;
        }
        
        .panel-content {
            border: 1px solid #e0e0e0;
            border-top: none;
            border-radius: 0 0 4px 4px;
            padding: 12px;
            background-color: #fafafa;
        }
        
        /* Tab styling */
        .stTabs [data-baseweb="tab-list"] {
            gap: 2px;
        }
        .stTabs [data-baseweb="tab"] {
            height: 45px;
            white-space: pre-wrap;
            background-color: #f0f0f0;
            border-radius: 4px 4px 0px 0px;
            padding: 8px 16px;
            font-weight: 600;
            font-size: 13px;
        }
        .stTabs [aria-selected="true"] {
            background-color: #107C41;
            color: white;
        }
    </style>
    """, unsafe_allow_html=True)


# =============================================================================
# LEFT PANEL
# =============================================================================

def render_left_panel():
    """Render the left sidebar panel."""
    with st.sidebar:
        st.title("📊 DataEngine Pro")
        st.caption("Excel Power • Python Speed")
        
        st.markdown("---")
        
        # Database Connection Section
        render_database_connection()
        
        st.markdown("---")
        
        # File Upload Section
        render_file_upload()
        
        st.markdown("---")
        
        # Table Selection
        render_table_selection()
        
        st.markdown("---")
        
        # Quick Actions
        render_quick_actions()
        
        st.markdown("---")
        
        # Saved Views
        render_saved_views_section()


def render_database_connection():
    """Render database connection section."""
    st.markdown("### 🗄️ Database")
    
    # Connection status
    if streamlit_app.app_database.is_connected():
        conn_info = streamlit_app.app_database.get_connection_info()
        st.success(f"✅ Connected: {conn_info['filename']}")
        
        # Show tables
        tables = streamlit_app.app_database.get_database_tables()
        if tables:
            st.caption(f"Tables: {len(tables)}")
            
            db_table = st.selectbox(
                "Load Table",
                tables,
                key="db_table_select"
            )
            
            if st.button("Load to Workspace", key="load_db_table"):
                df = streamlit_app.app_database.load_table_from_db(db_table)
                if df is not None:
                    streamlit_app.app_data_ops.store_dataframe(db_table, df)
                    st.success(f"Loaded: {db_table} ({len(df):,} rows)")
                    st.rerun()
        
        if st.button("Disconnect", key="disconnect_db"):
            streamlit_app.app_database.disconnect_from_database()
            st.rerun()
            
    else:
        # SQLite file selection
        db_file = st.file_uploader(
            "Connect SQLite DB",
            type=["db", "sqlite", "sqlite3"],
            key="db_file_uploader"
        )
        
        if db_file:
            tmp_path = streamlit_app.app_database.create_temp_file(db_file)
            if streamlit_app.app_database.connect_to_database(tmp_path):
                st.success("Connected!")
                st.rerun()


def render_file_upload():
    """Render file upload section."""
    st.markdown("### 📥 Data Source")
    
    uploaded_file = st.file_uploader(
        "Upload CSV or Excel",
        type=["csv", "xlsx", "xls"],
        key="main_file_uploader"
    )
    
    if uploaded_file and st.button("Load File", type="primary", use_container_width=True):
        with st.spinner("Loading data..."):
            df = streamlit_app.app_database.load_uploaded_file(uploaded_file)
            if df is not None:
                # Create table name from filename
                import os
                table_name = os.path.splitext(uploaded_file.name)[0]
                table_name = "".join(c for c in table_name if c.isalnum() or c == '_')
                
                streamlit_app.app_data_ops.store_dataframe(table_name, df)
                st.success(f"Loaded: {table_name} ({len(df):,} rows)")
                st.rerun()


def render_table_selection():
    """Render table selection section."""
    st.markdown("### 📋 Tables")
    
    tables = streamlit_app.app_state.get_table_list()
    
    if not tables:
        st.caption("No tables loaded")
        return
    
    # Current table indicator
    if st.session_state.current_table:
        st.caption(f"Active: **{st.session_state.current_table}**")
    
    selected_table = st.radio(
        "Select Table",
        tables,
        index=tables.index(st.session_state.current_table) if st.session_state.current_table in tables else 0,
        key="table_radio"
    )
    
    if selected_table != st.session_state.current_table:
        st.session_state.current_table = selected_table
        st.session_state.page = 1
        st.session_state.active_filters = []
        st.session_state.sort_column = None
        st.rerun()
    
    # Table info
    if selected_table:
        df = st.session_state.session_tables.get(selected_table)
        if df is not None:
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Rows", f"{len(df):,}")
            with col2:
                st.metric("Columns", len(df.columns))


def render_quick_actions():
    """Render quick action buttons."""
    st.markdown("### ⚡ Quick Actions")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("↩️ Undo", use_container_width=True, disabled=len(st.session_state.undo_stack) == 0):
            streamlit_app.app_data_ops.undo_operation()
            st.rerun()
    
    with col2:
        if st.button("🔄 Reset", use_container_width=True):
            streamlit_app.app_state.reset_to_original()
            st.rerun()
    
    # Show active filters
    if st.session_state.active_filters:
        st.markdown("**Active Filters:**")
        for i, f in enumerate(st.session_state.active_filters):
            col1, col2 = st.columns([4, 1])
            with col1:
                st.caption(f"{f['column']} {f['operator']} {f['value']}")
            with col2:
                if st.button("❌", key=f"remove_filter_{i}"):
                    streamlit_app.app_data_ops.remove_filter(i)
                    st.rerun()
        
        if st.button("Clear All Filters"):
            streamlit_app.app_data_ops.clear_all_filters()
            st.rerun()


def render_saved_views_section():
    """Render saved views section."""
    st.markdown("### 👁️ Custom Views")
    
    # Load views from file
    streamlit_app.app_views.load_views_from_file()
    
    # Current view indicator
    if st.session_state.current_view:
        st.caption(f"View: **{st.session_state.current_view}**")
    
    # Save new view
    new_view_name = st.text_input("View Name", placeholder="My View")
    
    if st.button("💾 Save View", use_container_width=True):
        if new_view_name:
            if streamlit_app.app_views.save_current_view(new_view_name):
                st.success(f"Saved: {new_view_name}")
                st.rerun()
        else:
            st.warning("Enter a view name")
    
    # List saved views
    if st.session_state.saved_views:
        st.markdown("**Saved Views:**")
        
        for view_name in st.session_state.saved_views.keys():
            col1, col2 = st.columns([3, 1])
            
            with col1:
                is_active = st.session_state.current_view == view_name
                if st.button(
                    f"{'✅ ' if is_active else '📄 '} {view_name}",
                    key=f"load_view_{view_name}"
                ):
                    streamlit_app.app_views.load_view(view_name)
                    st.rerun()
            
            with col2:
                if st.button("🗑️", key=f"delete_view_{view_name}"):
                    streamlit_app.app_views.delete_view(view_name)
                    st.rerun()


# =============================================================================
# CENTER PANEL
# =============================================================================

def render_center_panel():
    """Render the center panel with tabs."""
    if not st.session_state.current_table:
        render_welcome()
        return
    
    # Tab selection
    tab_names = [
        "📊 Data", 
        "🔍 Filter & Sort", 
        "📝 Columns", 
        "🔢 SQL Query",
        "🔗 Join Tables",
        "📈 Analysis"
    ]
    tabs = st.tabs(tab_names)
    
    with tabs[0]:
        render_data_tab()
    
    with tabs[1]:
        render_filter_sort_tab()
    
    with tabs[2]:
        render_columns_tab()
    
    with tabs[3]:
        render_sql_tab()
    
    with tabs[4]:
        render_join_tab()
    
    with tabs[5]:
        render_analysis_tab()


def render_welcome():
    """Render welcome screen."""
    st.markdown("""
    <div style='text-align: center; padding: 60px 20px;'>
        <h2>Welcome to DataEngine Pro</h2>
        <p style='font-size: 16px; color: #666;'>
            Excel-like experience with Python power
        </p>
        <div style='margin: 30px 0;'>
            <h4>Getting Started:</h4>
            <p>1. Upload a CSV or Excel file in the left panel</p>
            <p>2. Or connect to a SQLite database</p>
            <p>3. Then explore, filter, sort, and analyze your data</p>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Show quick stats if data loaded
    if st.session_state.session_tables:
        st.markdown("### Loaded Tables:")
        for table_name, df in st.session_state.session_tables.items():
            st.markdown(f"- **{table_name}**: {len(df):,} rows × {len(df.columns)} columns")


def render_data_tab():
    """Render the main data view tab."""
    table_name = st.session_state.current_table
    if not table_name:
        return
    
    # Get filtered data
    df = streamlit_app.app_data_ops.get_filtered_dataframe()
    
    if df.empty:
        st.info("No data to display (filters may be too restrictive)")
        return
    
    # Status bar
    render_status_bar(df)
    
    # Column selection
    st.markdown("#### 🎯 Column Display")
    
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        all_cols = list(df.columns)
        selected = st.multiselect(
            "Select Columns to Display",
            all_cols,
            default=st.session_state.selected_columns or all_cols,
            key="column_selector"
        )
        st.session_state.selected_columns = selected
        
        if selected:
            df = df[selected]
    
    with col2:
        if st.button("Select All Columns"):
            st.session_state.selected_columns = None
            st.rerun()
    
    with col3:
        total_rows = len(streamlit_app.app_data_ops.get_filtered_dataframe())
        showing_rows = len(df)
        st.caption(f"Showing {showing_rows:,} of {total_rows:,} rows")
    
    st.markdown("---")
    
    # Check if large dataset
    is_large_dataset = streamlit_app.app_state.is_large_dataset(df)
    
    if is_large_dataset:
        # Pagination for large datasets
        paginated_df, start, end = streamlit_app.app_data_ops.get_paginated_dataframe(df)
        
        st.dataframe(
            paginated_df,
            use_container_width=True,
            height=500,
            hide_index=False
        )
        
        render_pagination(len(df))
        
    else:
        # Regular display
        display_rows = min(len(df), streamlit_app.app_config.MAX_PREVIEW_ROWS)
        
        st.dataframe(
            df.head(display_rows),
            use_container_width=True,
            height=500,
            hide_index=False
        )
        
        if len(df) > display_rows:
            st.caption(f"Showing first {display_rows} rows. Use Filter & Sort tab for more control.")


def render_status_bar(df: pd.DataFrame):
    """Render status bar with data info."""
    # Get original row count
    original_df = st.session_state.original_data.get(st.session_state.current_table)
    original_rows = len(original_df) if original_df is not None else len(df)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Rows", f"{len(df):,}")
    
    with col2:
        st.metric("Columns", len(df.columns))
    
    with col3:
        if original_rows != len(df):
            st.metric("Filtered", f"-{original_rows - len(df):,}", delta_color="inverse")
        else:
            st.metric("Filtered", "0")
    
    with col4:
        if st.session_state.sort_column:
            st.caption(f"⬆️ Sorted by {st.session_state.sort_column}")
        else:
            st.caption("No sort")
    
    with col5:
        if st.session_state.current_view:
            st.caption(f"👁️ View: {st.session_state.current_view}")
        else:
            st.caption("Default view")


def render_pagination(total_rows: int):
    """Render pagination controls."""
    page_size = st.session_state.page_size
    total_pages = max(1, (total_rows - 1) // page_size + 1)
    
    col1, col2, col3, col4, col5 = st.columns([1, 2, 1, 2, 1])
    
    with col1:
        if st.button("⏮️", disabled=st.session_state.page == 1, key="page_first"):
            st.session_state.page = 1
            st.rerun()
    
    with col2:
        new_page = st.number_input(
            "Page", 
            min_value=1, 
            max_value=total_pages, 
            value=st.session_state.page,
            key="page_input"
        )
        if new_page != st.session_state.page:
            st.session_state.page = new_page
            st.rerun()
    
    with col3:
        st.write(f"of {total_pages}")
    
    with col4:
        new_size = st.selectbox(
            "Rows per page",
            [50, 100, 200, 500],
            index=[50, 100, 200, 500].index(page_size) if page_size in [50, 100, 200, 500] else 1,
            key="page_size_select"
        )
        if new_size != page_size:
            streamlit_app.app_data_ops.change_page_size(new_size)
            st.rerun()
    
    with col5:
        if st.button("⏭️", disabled=st.session_state.page == total_pages, key="page_last"):
            st.session_state.page = total_pages
            st.rerun()


def render_filter_sort_tab():
    """Render filter and sort tab."""
    table_name = st.session_state.current_table
    if not table_name:
        return
    
    df = st.session_state.session_tables.get(table_name)
    if df is None:
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🔍 Filter")
        
        # Show active filters
        if st.session_state.active_filters:
            st.markdown("**Active Filters:**")
            for i, f in enumerate(st.session_state.active_filters):
                st.caption(f"{i+1}. {f['column']} {f['operator']} {f['value']}")
            
            if st.button("Clear All Filters"):
                streamlit_app.app_data_ops.clear_all_filters()
                st.rerun()
        
        # Add new filter
        st.markdown("**Add Filter:**")
        
        filter_col = st.selectbox(
            "Column",
            [""] + list(df.columns),
            key="filter_column"
        )
        
        if filter_col:
            col_type = df[filter_col].dtype
            operators = streamlit_app.app_data_ops.get_filter_operators(col_type)
            filter_op = st.selectbox("Operator", operators, key="filter_operator")
            
            filter_val = None
            if filter_op not in ["is_null", "is_not_null", "is_blank"]:
                if pd.api.types.is_numeric_dtype(col_type):
                    filter_val = st.text_input("Value", key="filter_value")
                    if filter_val:
                        try:
                            filter_val = float(filter_val)
                        except ValueError:
                            pass
                else:
                    filter_val = st.text_input("Value", key="filter_value")
            
            if st.button("Apply Filter", key="apply_filter"):
                streamlit_app.app_data_ops.add_filter(filter_col, filter_op, filter_val)
                st.rerun()
    
    with col2:
        st.markdown("#### ⬆️ Sort")
        
        if st.session_state.sort_column:
            st.caption(f"Currently sorted by: **{st.session_state.sort_column}**")
            st.caption(f"Direction: {'Ascending' if st.session_state.sort_ascending else 'Descending'}")
            
            if st.button("Clear Sort"):
                streamlit_app.app_data_ops.clear_sort()
                st.rerun()
        
        st.markdown("**Add Sort:**")
        
        sort_col = st.selectbox(
            "Sort by",
            [""] + list(df.columns),
            key="sort_column"
        )
        
        if sort_col:
            ascending = st.radio("Direction", ["Ascending", "Descending"], index=0 if st.session_state.sort_ascending else 1)
            
            if st.button("Apply Sort", key="apply_sort"):
                streamlit_app.app_data_ops.apply_sort(sort_col, ascending == "Ascending")
                st.rerun()


def render_columns_tab():
    """Render columns management tab."""
    table_name = st.session_state.current_table
    if not table_name:
        return
    
    df = st.session_state.session_tables.get(table_name)
    if df is None:
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### ➕ Add Column")
        
        new_col_name = st.text_input("New Column Name", key="new_col_name")
        
        default_val = st.text_input("Default Value (optional)", key="new_col_default")
        
        if st.button("Add Column", key="add_col_btn"):
            if new_col_name:
                if streamlit_app.app_data_ops.add_column(new_col_name, default_val if default_val else None):
                    st.success(f"Added column: {new_col_name}")
                    st.rerun()
            else:
                st.warning("Enter a column name")
        
        st.markdown("---")
        
        st.markdown("#### 🧮 Add Calculated Column")
        
        formula_col = st.text_input("Expression (pandas)", placeholder="col1 + col2", key="formula_col")
        formula_name = st.text_input("New Column Name", key="formula_name")
        
        if st.button("Create Column", key="create_formula_btn"):
            if formula_col and formula_name:
                if streamlit_app.app_data_ops.add_column_from_expression(formula_col, formula_name):
                    st.success(f"Created column: {formula_name}")
                    st.rerun()
            else:
                st.warning("Enter both expression and column name")
    
    with col2:
        st.markdown("#### 📝 Column Info")
        
        for col in df.columns:
            with st.expander(f"**{col}**"):
                st.caption(f"Type: {df[col].dtype}")
                st.caption(f"Nulls: {df[col].isna().sum()} / {len(df)}")
                st.caption(f"Unique: {df[col].nunique()}")
                
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Rename", key=f"rename_{col}"):
                        st.session_state.rename_column = col
                        st.rerun()
                with col2:
                    if st.button("Delete", key=f"delete_{col}"):
                        if streamlit_app.app_data_ops.delete_column(col):
                            st.success(f"Deleted: {col}")
                            st.rerun()
        
        # Handle rename
        if 'rename_column' in st.session_state and st.session_state.rename_column:
            old_name = st.session_state.rename_column
            new_name = st.text_input("New Name", value=old_name, key="rename_input")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Confirm Rename"):
                    if new_name and new_name != old_name:
                        if streamlit_app.app_data_ops.rename_column(old_name, new_name):
                            st.success(f"Renamed to: {new_name}")
                            del st.session_state.rename_column
                            st.rerun()
            with col2:
                if st.button("Cancel Rename"):
                    del st.session_state.rename_column
                    st.rerun()


def render_sql_tab():
    """Render SQL query tab."""
    table_name = st.session_state.current_table
    if not table_name:
        return
    
    df = st.session_state.session_tables.get(table_name)
    if df is None:
        return
    
    st.markdown("#### 🔢 Pandas Query")
    
    query = st.text_area(
        "Query Expression",
        placeholder="age > 25 and status == 'Active'",
        height=100,
        key="sql_query"
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Execute Query", type="primary"):
            if query:
                result, msg = streamlit_app.app_sql.execute_query(query)
                if result is not None:
                    streamlit_app.app_sql.store_query_result(result)
                    st.success(f"Query returned {len(result):,} rows")
                else:
                    st.error(msg)
    
    with col2:
        if st.button("Clear Query"):
            streamlit_app.app_sql.clear_query_result()
    
    # Show query result
    if streamlit_app.app_sql.has_query_result():
        st.markdown("---")
        st.markdown("#### Query Result")
        
        result_df = streamlit_app.app_sql.get_query_result()
        st.dataframe(result_df, use_container_width=True, height=300)
        
        if st.button("Apply as Filter"):
            if streamlit_app.app_sql.apply_query_as_filter():
                st.success("Query result applied as filter")
                st.rerun()


def render_join_tab():
    """Render join tables tab."""
    tables = streamlit_app.app_state.get_table_list()
    
    if len(tables) < 2:
        st.info("Need at least 2 tables to perform joins")
        return
    
    st.markdown("#### 🔗 Join Tables")
    
    col1, col2 = st.columns(2)
    
    with col1:
        left_table = st.selectbox("Left Table", tables, key="join_left")
    
    with col2:
        right_table = st.selectbox("Right Table", [t for t in tables if t != left_table], key="join_right")
    
    if left_table and right_table:
        left_df = st.session_state.session_tables[left_table]
        right_df = st.session_state.session_tables[right_table]
        
        col1, col2 = st.columns(2)
        
        with col1:
            left_key = st.selectbox("Left Key", left_df.columns, key="join_left_key")
        
        with col2:
            right_key = st.selectbox("Right Key", right_df.columns, key="join_right_key")
        
        join_type = st.selectbox(
            "Join Type",
            list(streamlit_app.app_join.JOIN_TYPES.keys()),
            key="join_type"
        )
        
        st.caption(streamlit_app.app_join.JOIN_TYPES[join_type])
        
        if st.button("Execute Join", type="primary"):
            result, msg = streamlit_app.app_join.join_tables(
                left_table, right_table, left_key, right_key, join_type
            )
            
            if result is not None:
                # Ask for new table name
                new_table_name = st.text_input("Save as Table Name", value=f"{left_table}_joined", key="join_save_name")
                
                if st.button("Save Joined Table"):
                    streamlit_app.app_data_ops.store_dataframe(new_table_name, result)
                    st.success(f"Saved: {new_table_name} ({len(result):,} rows)")
                    st.rerun()
            else:
                st.error(msg)


def render_analysis_tab():
    """Render analysis tab."""
    table_name = st.session_state.current_table
    if not table_name:
        return
    
    df = st.session_state.session_tables.get(table_name)
    if df is None:
        return
    
    st.markdown("#### 📈 Statistics")
    
    # Show numeric columns stats
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    if len(numeric_cols) > 0:
        st.dataframe(df[numeric_cols].describe(), use_container_width=True)
    else:
        st.info("No numeric columns for statistics")
    
    st.markdown("---")
    
    st.markdown("#### 🔢 Value Counts")
    
    count_col = st.selectbox("Column", df.columns, key="value_count_col")
    
    if count_col:
        counts = df[count_col].value_counts().head(20)
        st.dataframe(counts, use_container_width=True)
