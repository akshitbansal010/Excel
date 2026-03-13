"""
UI rendering module for DataEngine Pro Streamlit app.
Handles all UI components and rendering.
"""

import streamlit as st
import pandas as pd
import numpy as np
from typing import List

import app_config
import app_state
import app_data_ops
import app_database
import app_views
import app_sql
import app_join


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
    if app_database.is_connected():
        conn_info = app_database.get_connection_info()
        st.success(f"✅ Connected: {conn_info['filename']}")
        
        # Show tables
        tables = app_database.get_database_tables()
        if tables:
            st.caption(f"Tables: {len(tables)}")
            
            db_table = st.selectbox(
                "Load Table",
                tables,
                key="db_table_select"
            )
            
            if st.button("Load to Workspace", key="load_db_table"):
                df = app_database.load_table_from_db(db_table)
                if df is not None:
                    app_data_ops.store_dataframe(db_table, df)
                    st.success(f"Loaded: {db_table} ({len(df):,} rows)")
                    st.rerun()
        
        if st.button("Disconnect", key="disconnect_db"):
            app_database.disconnect_from_database()
            st.rerun()
            
    else:
        # SQLite file selection
        db_file = st.file_uploader(
            "Connect SQLite DB",
            type=["db", "sqlite", "sqlite3"],
            key="db_file_uploader"
        )
        
        if db_file:
            tmp_path = app_database.create_temp_file(db_file)
            if app_database.connect_to_database(tmp_path):
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
            df = app_database.load_uploaded_file(uploaded_file)
            if df is not None:
                # Create table name from filename
                import os
                table_name = os.path.splitext(uploaded_file.name)[0]
                table_name = "".join(c for c in table_name if c.isalnum() or c == '_')
                
                app_data_ops.store_dataframe(table_name, df)
                st.success(f"Loaded: {table_name} ({len(df):,} rows)")
                st.rerun()


def render_table_selection():
    """Render table selection section."""
    st.markdown("### 📋 Tables")
    
    tables = app_state.get_table_list()
    
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
            app_data_ops.undo_operation()
            st.rerun()
    
    with col2:
        if st.button("🔄 Reset", use_container_width=True):
            app_state.reset_to_original()
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
                    app_data_ops.remove_filter(i)
                    st.rerun()
        
        if st.button("Clear All Filters"):
            app_data_ops.clear_all_filters()
            st.rerun()


def render_saved_views_section():
    """Render saved views section."""
    st.markdown("### 👁️ Custom Views")
    
    # Load views from file
    app_views.load_views_from_file()
    
    # Current view indicator
    if st.session_state.current_view:
        st.caption(f"View: **{st.session_state.current_view}**")
    
    # Save new view
    new_view_name = st.text_input("View Name", placeholder="My View")
    
    if st.button("💾 Save View", use_container_width=True):
        if new_view_name:
            if app_views.save_current_view(new_view_name):
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
                    app_views.load_view(view_name)
                    st.rerun()
            
            with col2:
                if st.button("🗑️", key=f"delete_view_{view_name}"):
                    app_views.delete_view(view_name)
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
    df = app_data_ops.get_filtered_dataframe()
    
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
        total_rows = len(app_data_ops.get_filtered_dataframe())
        showing_rows = len(df)
        st.caption(f"Showing {showing_rows:,} of {total_rows:,} rows")
    
    st.markdown("---")
    
    # Check if large dataset
    is_large_dataset = app_state.is_large_dataset(df)
    
    if is_large_dataset:
        # Pagination for large datasets
        paginated_df, start, end = app_data_ops.get_paginated_dataframe(df)
        
        st.dataframe(
            paginated_df,
            use_container_width=True,
            height=500,
            hide_index=False
        )
        
        render_pagination(len(df))
        
    else:
        # Regular display
        display_rows = min(len(df), app_config.MAX_PREVIEW_ROWS)
        
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
            app_data_ops.change_page_size(new_size)
            st.rerun()
    
    with col5:
        if st.button("⏭️", disabled=st.session_state.page == total_pages, key="page_last"):
            st.session_state.page = total_pages
            st.rerun()


def render_filter_sort_tab():
    """Render filtering and sorting tab."""
    table_name = st.session_state.current_table
    if not table_name:
        return
    
    df = app_data_ops.get_current_dataframe()
    
    if df is None or df.empty:
        st.info("No data loaded")
        return
    
    # Create two columns for Filter and Sort
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🔍 Filter")
        
        # Filter builder
        filter_col = st.selectbox(
            "Column",
            [""] + list(df.columns),
            key="filter_column"
        )
        
        if filter_col:
            # Get column type
            col_dtype = df[filter_col].dtype
            
            # Get operators based on type
            operators = app_data_ops.get_filter_operators(col_dtype)
            
            filter_op = st.selectbox(
                "Operator",
                operators,
                key="filter_operator"
            )
            
            # Value input
            filter_value = None
            
            if filter_op not in ["is_null", "is_not_null", "is_blank"]:
                if pd.api.types.is_numeric_dtype(col_dtype):
                    filter_value = st.text_input("Value", key="filter_value")
                    if filter_value:
                        try:
                            filter_value = float(filter_value)
                        except ValueError:
                            pass
                else:
                    unique_vals = df[filter_col].dropna().unique()
                    if len(unique_vals) <= 20:
                        filter_value = st.selectbox(
                            "Value",
                            unique_vals,
                            key="filter_value_select"
                        )
                    else:
                        filter_value = st.text_input("Value", key="filter_value_text")
            
            # Add filter button
            if st.button("➕ Add Filter", type="primary", key="add_filter_btn"):
                if filter_col and filter_op:
                    app_data_ops.add_filter(filter_col, filter_op, filter_value)
                    st.rerun()
        
        # Show active filters
        if st.session_state.active_filters:
            st.markdown("#### Active Filters:")
            for i, f in enumerate(st.session_state.active_filters):
                with st.expander(f"Filter {i+1}: {f['column']} {f['operator']} {f['value']}"):
                    st.write(f"**Column:** {f['column']}")
                    st.write(f"**Operator:** {f['operator']}")
                    st.write(f"**Value:** {f['value']}")
                    if st.button("Remove", key=f"remove_filter_exp_{i}"):
                        app_data_ops.remove_filter(i)
                        st.rerun()
            
            if st.button("Clear All Filters"):
                app_data_ops.clear_all_filters()
                st.rerun()
    
    with col2:
        st.markdown("### ⬆️⬇️ Sort")
        
        # Sort column selection
        sort_col = st.selectbox(
            "Sort by Column",
            [""] + list(df.columns),
            key="sort_column"
        )
        
        if sort_col:
            sort_asc = st.radio(
                "Direction",
                ["Ascending ↑", "Descending ↓"],
                index=0 if st.session_state.sort_ascending else 1,
                key="sort_direction"
            )
            
            ascending = sort_asc == "Ascending ↑"
            
            col_apply, col_clear = st.columns(2)
            
            with col_apply:
                if st.button("Apply Sort", type="primary"):
                    app_data_ops.apply_sort(sort_col, ascending)
                    st.rerun()
            
            with col_clear:
                if st.button("Clear Sort"):
                    app_data_ops.clear_sort()
                    st.rerun()
        
        # Current sort indicator
        if st.session_state.sort_column:
            st.success(f"⬆️ Sorted by: {st.session_state.sort_column} ({'ASC' if st.session_state.sort_ascending else 'DESC'})")
        
        # Quick sort options
        st.markdown("#### Quick Sort")
        
        for col in list(df.columns)[:5]:
            col_sort_asc, col_sort_desc = st.columns(2)
            
            with col_sort_asc:
                if st.button(f"↑ {col[:12]}", key=f"qs_asc_{col}"):
                    app_data_ops.apply_sort(col, True)
                    st.rerun()
            
            with col_sort_desc:
                if st.button(f"↓ {col[:12]}", key=f"qs_desc_{col}"):
                    app_data_ops.apply_sort(col, False)
                    st.rerun()


def render_columns_tab():
    """Render column management tab."""
    table_name = st.session_state.current_table
    if not table_name:
        return
    
    df = app_data_ops.get_current_dataframe()
    
    if df is None or df.empty:
        st.info("No data loaded")
        return
    
    # Create two columns for Add/Delete and Rename
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### ➕ Add Column")
        
        new_col_name = st.text_input("New Column Name", key="new_col_name")
        default_val = st.text_input("Default Value (optional)", key="new_col_default")
        
        if st.button("Add Column", type="primary"):
            if new_col_name:
                value = None if default_val == "" else default_val
                if app_data_ops.add_column(new_col_name, value):
                    st.success(f"Added column: {new_col_name}")
                    st.rerun()
            else:
                st.warning("Enter a column name")
        
        st.markdown("---")
        
        # Add computed column from SQL
        st.markdown("### 🧮 Add Computed Column")
        
        sql_col_name = st.text_input("Result Column Name", key="sql_col_name")
        sql_expression = st.text_area(
            "SQL Expression (pandas eval)",
            placeholder="e.g., column1 + column2 or column1 * 0.1",
            key="sql_col_expr"
        )
        
        if st.button("Create from Expression", key="create_sql_col"):
            if sql_col_name and sql_expression:
                if app_data_ops.add_column_from_expression(sql_expression, sql_col_name):
                    st.success(f"Created column: {sql_col_name}")
                    st.rerun()
            else:
                st.warning("Enter both column name and expression")
    
    with col2:
        st.markdown("### 🗑️ Delete Column")
        
        delete_col = st.selectbox(
            "Select Column to Delete",
            df.columns,
            key="delete_col_select"
        )
        
        if delete_col:
            col_data = df[delete_col]
            st.caption(f"Type: {col_data.dtype}, Non-null: {col_data.notna().sum():,}")
        
        if st.button("Delete Column", type="primary"):
            if delete_col and app_data_ops.delete_column(delete_col):
                st.success(f"Deleted column: {delete_col}")
                st.rerun()
        
        st.markdown("---")
        
        # Rename column
        st.markdown("### ✏️ Rename Column")
        
        rename_col = st.selectbox(
            "Select Column to Rename",
            df.columns,
            key="rename_col_select"
        )
        
        new_name = st.text_input("New Name", key="rename_col_new")
        
        if st.button("Rename Column"):
            if rename_col and new_name:
                if app_data_ops.rename_column(rename_col, new_name):
                    st.success(f"Renamed {rename_col} to {new_name}")
                    st.rerun()
            else:
                st.warning("Enter new name")
    
    # Show all columns
    st.markdown("---")
    st.markdown("### 📋 All Columns")
    
    with st.expander("View Column Details", expanded=False):
        for col in df.columns:
            col_data = df[col]
            with st.expander(f"**{col}** ({col_data.dtype})"):
                st.write(f"**Type:** {col_data.dtype}")
                st.write(f"**Non-null:** {col_data.notna().sum():,} / {len(col_data):,}")
                st.write(f"**Unique values:** {col_data.nunique():,}")
                if pd.api.types.is_numeric_dtype(col_data):
                    st.write(f"**Min:** {col_data.min()}")
                    st.write(f"**Max:** {col_data.max()}")
                    st.write(f"**Mean:** {col_data.mean():.2f}")


def render_sql_tab():
    """Render SQL query tab."""
    table_name = st.session_state.current_table
    if not table_name:
        return
    
    df = app_data_ops.get_current_dataframe()
    
    if df is None or df.empty:
        st.info("No data loaded")
        return
    
    st.markdown("### 🔢 SQL Query")
    
    # Query type selection
    query_type = st.radio(
        "Query Type",
        ["Filter/SELECT", "Add Column"],
        horizontal=True,
        key="query_type"
    )
    
    if query_type == "Filter/SELECT":
        st.markdown("#### Filter Data (WHERE clause)")
        
        sql_query = st.text_area(
            "Enter WHERE clause (pandas query syntax)",
            placeholder="e.g., age > 25 and status == 'Active'",
            height=80,
            key="sql_filter_query"
        )
        
        col1, col2 = st.columns([1, 4])
        
        with col1:
            limit = st.number_input("Limit Results", min_value=1, max_value=100000, value=1000, key="sql_limit")
        
        with col_execute := col2:
            pass
        
        col_execute, col_clear = st.columns(2)
        
        with col_execute:
            if st.button("🔍 Execute Query", type="primary", key="execute_sql_btn"):
                if sql_query:
                    with st.spinner("Executing query..."):
                        result_df, msg = app_sql.execute_query(sql_query)
                        
                        if result_df is not None:
                            app_sql.store_query_result(result_df)
                            st.success(f"Found {len(result_df):,} rows")
                        else:
                            st.error(msg)
                else:
                    st.warning("Enter a query")
        
        with col_clear:
            if st.button("Clear Results"):
                app_sql.clear_query_result()
                st.rerun()
        
        # Show results
        if app_sql.has_query_result():
            result_df = app_sql.get_query_result()
            
            st.markdown("#### Query Results")
            st.dataframe(result_df.head(min(limit, len(result_df))), height=300)
            
            st.caption(f"Showing {min(limit, len(result_df)):,} of {len(result_df):,} results")
            
            # Option to apply as filter
            if st.button("Apply as Filter"):
                if app_sql.apply_query_as_filter():
                    st.success("Query results applied!")
                    st.rerun()
    
    elif query_type == "Add Column":
        st.markdown("#### Add Column from Expression")
        
        new_col_name = st.text_input("New Column Name", key="sql_add_col_name")
        
        expression = st.text_area(
            "Expression (uses pandas eval)",
            placeholder="e.g., column1 + column2 or column1 * column2 / 100",
            height=80,
            key="sql_add_col_expr"
        )
        
        st.caption("""
        **Available operators:** +, -, *, /, %, ** (power)
        **Examples:**
        - `price * quantity` - multiply columns
        - `column1 + column2` - add columns
        - `(a + b) / 2` - average
        - `column ** 2` - square
        """)
        
        if st.button("➕ Create Column", type="primary", key="sql_create_col_btn"):
            if new_col_name and expression:
                if app_data_ops.add_column_from_expression(expression, new_col_name):
                    st.success(f"Created column: {new_col_name}")
                    st.rerun()
            else:
                st.warning("Enter column name and expression")


def render_analysis_tab():
    """Render analysis tab with basic statistics."""
    table_name = st.session_state.current_table
    if not table_name:
        return
    
    df = app_data_ops.get_filtered_dataframe()
    
    if df is None or df.empty:
        st.info("No data loaded")
        return
    
    st.markdown("### 📈 Quick Analysis")
    
    # Summary statistics
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Data Summary")
        
        st.write(f"**Rows:** {len(df):,}")
        st.write(f"**Columns:** {len(df.columns)}")
        
        memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
        st.write(f"**Memory:** {memory_mb:.2f} MB")
    
    with col2:
        st.markdown("#### Column Types")
        
        type_counts = df.dtypes.value_counts()
        for dtype, count in type_counts.items():
            st.write(f"**{dtype}:** {count}")
    
    # Numeric columns analysis
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    if len(numeric_cols) > 0:
        st.markdown("---")
        st.markdown("#### Numeric Columns Statistics")
        
        selected_num_col = st.selectbox(
            "Select Numeric Column",
            numeric_cols,
            key="analysis_num_col"
        )
        
        if selected_num_col:
            col_data = df[selected_num_col]
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Sum", f"{col_data.sum():,.2f}")
            with col2:
                st.metric("Mean", f"{col_data.mean():.2f}")
            with col3:
                st.metric("Min", f"{col_data.min():.2f}")
            with col4:
                st.metric("Max", f"{col_data.max():.2f}")
            
            st.write("Full Statistics:")
            st.dataframe(col_data.describe())
    
    # Categorical columns
    cat_cols = df.select_dtypes(include=['object', 'category']).columns
    
    if len(cat_cols) > 0:
        st.markdown("---")
        st.markdown("#### Categorical Columns")
        
        selected_cat_col = st.selectbox(
            "Select Categorical Column",
            cat_cols,
            key="analysis_cat_col"
        )
        
        if selected_cat_col:
            value_counts = df[selected_cat_col].value_counts().head(20)
            
            st.write(f"**Unique values:** {df[selected_cat_col].nunique():,}")
            
            st.write("Top 20 values:")
            st.dataframe(value_counts)


def render_join_tab():
    """Render join/lookup tab for table operations."""
    tables = app_join.get_available_tables()
    
    if len(tables) < 2:
        st.info("Need at least 2 tables to perform join. Load more tables first.")
        return
    
    st.markdown("### 🔗 Join Tables (VLOOKUP-style)")
    
    # Join type selection
    join_operation = st.radio(
        "Join Type",
        ["Join Tables (SQL-style)", "VLOOKUP (Add Column)", "Stack Tables"],
        horizontal=True,
        key="join_operation"
    )
    
    if join_operation == "Join Tables (SQL-style)":
        _render_sql_join()
    elif join_operation == "VLOOKUP (Add Column)":
        _render_vlookup()
    elif join_operation == "Stack Tables":
        _render_stack_tables()


def _render_sql_join():
    """Render SQL-style join UI."""
    tables = app_join.get_available_tables()
    current_table = st.session_state.current_table
    
    st.markdown("#### SQL JOIN")
    
    col1, col2 = st.columns(2)
    
    with col1:
        left_table = st.selectbox(
            "Left Table",
            tables,
            index=tables.index(current_table) if current_table in tables else 0,
            key="join_left_table",
            on_change=lambda: st.session_state.pop("join_result_df", None)
        )
    
    with col2:
        right_table = st.selectbox(
            "Right Table",
            [t for t in tables if t != left_table],
            key="join_right_table",
            on_change=lambda: st.session_state.pop("join_result_df", None)
        )
    
    if left_table and right_table:
        left_cols = app_join.get_table_columns(left_table)
        right_cols = app_join.get_table_columns(right_table)
        
        col1, col2 = st.columns(2)
        
        with col1:
            left_key = st.selectbox(
                f"{left_table} Key Column",
                left_cols,
                key="join_left_key"
            )
        
        with col2:
            right_key = st.selectbox(
                f"{right_table} Key Column",
                right_cols,
                key="join_right_key"
            )
        
        # Join type
        join_type = st.selectbox(
            "Join Type",
            list(app_join.JOIN_TYPES.keys()),
            key="join_type"
        )
        
        st.caption(app_join.JOIN_TYPES[join_type])
        
        # Execute join
        if st.button("🔗 Perform Join", type="primary", key="btn_join"):
            # Validate
            is_valid, msg = app_join.validate_join(left_table, right_table, left_key, right_key)
            if not is_valid:
                st.error(msg)
                return
            
            with st.spinner("Performing join..."):
                result_df, msg = app_join.join_tables(
                    left_table, right_table, left_key, right_key, join_type
                )
                
                if result_df is not None:
                    # Persist result to session state
                    st.session_state["join_result_df"] = result_df
                    st.success(f"Join complete! Result: {len(result_df):,} rows × {len(result_df.columns)} columns")
                else:
                    st.error(f"Join failed: {msg}")
                    st.session_state["join_result_df"] = None
        
        # Display persisted result if available
        if "join_result_df" in st.session_state and st.session_state["join_result_df"] is not None:
            result_df = st.session_state["join_result_df"]
            
            # Show preview
            st.dataframe(result_df.head(50), height=300)
            
            # Options to save
            st.markdown("#### Save Result")
            
            col1, col2 = st.columns(2)
            
            with col1:
                new_table_name = st.text_input(
                    "New Table Name",
                    value=f"{left_table}_joined_{right_table}",
                    key="join_result_name"
                )
            
            with col2:
                st.write("")
                st.write("")
                
                if st.button("💾 Save as New Table", key="btn_save_join"):
                    if new_table_name:
                        app_join.store_as_new_table(new_table_name, result_df)
                        st.session_state.pop("join_result_df", None)
                        st.success(f"Saved as: {new_table_name}")
                        st.rerun()


def _render_vlookup():
    """Render VLOOKUP-style column addition UI."""
    tables = app_join.get_available_tables()
    current_table = st.session_state.current_table
    
    st.markdown("#### VLOOKUP - Add Column from Another Table")
    
    col1, col2 = st.columns(2)
    
    with col1:
        source_table = st.selectbox(
            "Source Table (to add column)",
            tables,
            index=tables.index(current_table) if current_table in tables else 0,
            key="vlookup_source"
        )
    
    with col2:
        lookup_table = st.selectbox(
            "Lookup Table (get value from)",
            [t for t in tables if t != source_table],
            key="vlookup_lookup"
        )
    
    if source_table and lookup_table:
        source_cols = app_join.get_table_columns(source_table)
        lookup_cols = app_join.get_table_columns(lookup_table)
        
        col1, col2 = st.columns(2)
        
        with col1:
            source_key = st.selectbox(
                "Match Column (Source)",
                source_cols,
                key="vlookup_source_key"
            )
        
        with col2:
            lookup_key = st.selectbox(
                "Match Column (Lookup)",
                lookup_cols,
                key="vlookup_lookup_key"
            )
        
        # Column to retrieve
        lookup_column = st.selectbox(
            "Column to Retrieve",
            lookup_cols,
            key="vlookup_column"
        )
        
        # New column name
        new_column = st.text_input(
            "New Column Name",
            value=f"{lookup_column}_from_{lookup_table}",
            key="vlookup_new_col"
        )
        
        # Execute VLOOKUP
        if st.button("🔍 Add Column (VLOOKUP)", type="primary", key="btn_vlookup"):
            if source_table and lookup_table and source_key and lookup_key and lookup_column and new_column:
                with st.spinner("Performing VLOOKUP..."):
                    result_df, msg = app_join.create_lookup_column(
                        source_table, lookup_table,
                        source_key, lookup_key,
                        lookup_column, new_column
                    )
                    
                    if result_df is not None:
                        # Show diff
                        original_df = st.session_state.session_tables[source_table]
                        matched = result_df[new_column].notna().sum()
                        
                        st.success(f"VLOOKUP complete! Matched: {matched:,} / {len(result_df):,}")
                        
                        # Show preview with new column
                        st.dataframe(result_df[[source_key, new_column]].head(30), height=250)
                        
                        # Save
                        if st.button("💾 Apply to Table", key="btn_apply_vlookup"):
                            app_join.update_existing_table(source_table, result_df)
                            st.success("Column added!")
                            st.rerun()
                    else:
                        st.error(f"VLOOKUP failed: {msg}")


def _render_stack_tables():
    """Render table stacking UI."""
    tables = app_join.get_available_tables()
    
    st.markdown("#### Stack Tables (Union)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        table1 = st.selectbox(
            "First Table",
            tables,
            key="stack_table1",
            on_change=lambda: st.session_state.pop("stack_result_df", None) or st.session_state.pop("stack_result_name", None)
        )
    
    with col2:
        table2 = st.selectbox(
            "Second Table",
            [t for t in tables if t != table1],
            key="stack_table2",
            on_change=lambda: st.session_state.pop("stack_result_df", None) or st.session_state.pop("stack_result_name", None)
        )
    
    stack_type = st.radio(
        "Stack Type",
        ["Vertical (Stack Rows)", "Horizontal (Merge Columns)"],
        horizontal=True,
        key="stack_type"
    )
    
    join_type = "vertical" if "Vertical" in stack_type else "horizontal"
    
    if st.button("➕ Stack Tables", type="primary", key="btn_stack"):
        with st.spinner("Stacking tables..."):
            result_df, msg = app_join.concatenate_tables(table1, table2, join_type)
            
            if result_df is not None:
                # Persist result to session state
                st.session_state["stack_result_df"] = result_df
                st.success(f"Stack complete! Result: {len(result_df):,} rows × {len(result_df.columns)} columns")
            else:
                st.error(f"Stack failed: {msg}")
                st.session_state["stack_result_df"] = None
    
    # Display persisted result if available
    if "stack_result_df" in st.session_state and st.session_state["stack_result_df"] is not None:
        result_df = st.session_state["stack_result_df"]
        
        st.dataframe(result_df.head(50), height=300)
        
        # Save
        new_name = st.text_input(
            "New Table Name",
            value=f"{table1}_and_{table2}",
            key="stack_result_name"
        )
        
        if st.button("💾 Save as New Table", key="btn_save_stack"):
            if new_name:
                app_join.store_as_new_table(new_name, result_df)
                st.session_state.pop("stack_result_df", None)
                st.session_state.pop("stack_result_name", None)
                st.success(f"Saved as: {new_name}")
                st.rerun()
