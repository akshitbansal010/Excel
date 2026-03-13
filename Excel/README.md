# DataEngine Pro - Integration Layer

This document describes the integration layer that connects the existing DataEngine Pro core modules to a Power BI-like Streamlit UI and optional FastAPI backend.

## Table of Contents

1. [Overview](#overview)
2. [Directory Structure](#directory-structure)
3. [Quick Start](#quick-start)
4. [Integration Layer](#integration-layer)
5. [Running the Applications](#running-the-applications)
6. [Adapter API](#adapter-api)
7. [Column Resolver](#column-resolver)
8. [Session Management](#session-management)
9. [Customizing the Adapter](#customizing-the-adapter)
10. [Testing](#testing)
11. [Troubleshooting](#troubleshooting)

## Overview

The integration layer provides:

- **Adapter** (`integration/adapter.py`): Canonical API that bridges existing core modules to the UI/backend
- **Session Manager** (`integration/session.py`): Session state with lightweight undo snapshots
- **Column Resolver** (`integration/column_resolver.py`): Fuzzy column name resolution
- **Diagnostic Script** (`integration/inspect_core.py`): Discovers available core functions

## Directory Structure

```
Excel/
├── integration/              # Integration layer
│   ├── __init__.py
│   ├── adapter.py          # Main adapter with canonical API
│   ├── column_resolver.py  # Fuzzy column matching
│   ├── session.py          # Session management with undo
│   └── inspect_core.py     # Core module discovery script
├── ui/web/
│   ├── streamlit_app.py    # Power BI-like Streamlit UI
│   └── engine.py           # Engine wrapper for UI
├── backend/
│   └── app.py              # FastAPI backend (optional)
├── tests/
│   ├── fixtures/
│   │   └── sample_data.csv
│   └── test_integration.py # Unit tests
└── data_engine/             # Existing core modules
```

## Quick Start

### 1. Run the Inspector

First, run the diagnostic script to see what core functions were discovered:

```bash
python integration/inspect_core.py
```

This will print a report showing:
- Which modules are available
- Which functions are mapped to the adapter
- Which operations need implementation

### 2. Run Streamlit UI

```bash
streamlit run ui/web/streamlit_app.py
```

The UI features:
- **Left Panel**: Sessions, tables, schema, column search
- **Center Panel**: Tabbed view (Preview, Diff, Charts, SQL)
- **Right Panel**: Operation Wizard (Filter, Sort, Aggregate, Rank, Pivot)

### 3. Run FastAPI Backend (Optional)

```bash
uvicorn backend.app:app --reload --port 8000
```

Access API docs at http://localhost:8000/docs

## Integration Layer

### Adapter (`integration/adapter.py`)

The adapter provides a canonical API that automatically detects and maps to available core functions.

#### Key Features:

1. **Automatic Function Discovery**: Scans core modules on initialization
2. **Bounded Results**: All previews are limited (default 200 rows)
3. **Session Management**: Creates and manages data sessions
4. **Undo Support**: Maintains operation history for undo
5. **Diff Tracking**: Tracks before/after state for comparisons

#### Canonical Operations:

```python
from integration.adapter import get_adapter

adapter = get_adapter()

# Create session
session = adapter.create_session()

# Load data
adapter.load_table(session.id, {"type": "csv", "path": "data.csv"})

# Preview
preview = adapter.preview(session.id, limit=100)

# Filter
result = adapter.op_filter(session.id, column="age", operator=">", value=25)

# Sort
result = adapter.op_sort(session.id, columns=["salary"], ascending=False)

# Aggregate
result = adapter.op_aggregate(
    session.id,
    group_by=["department"],
    aggs={"salary": "mean"}
)

# Rank
result = adapter.op_rank(session.id, by="salary", method="dense", new_col="rank")

# Save
adapter.save(session.id, dest={"type": "csv", "path": "output.csv"})

# Undo
adapter.undo(session.id)
```

### Column Resolver (`integration/column_resolver.py`)

Fuzzy column name resolution with support for:
- Exact match
- Case-insensitive match
- Partial/substring match
- Excel letter notation (A, B, AA, AB)
- Numeric indices

```python
from integration.column_resolver import ColumnResolver

resolver = ColumnResolver(df)

# Resolve column names
resolver.resolve("name")     # Exact: "name"
resolver.resolve("NAME")     # Case-insensitive: "name"
resolver.resolve("0")       # Numeric index: first column
resolver.resolve("A")       # Excel letter: first column
resolver.resolve("nam")     # Partial: "name"

# Get suggestions
suggestions = resolver.suggest("na", limit=5)
```

### Session Manager (`integration/session.py`)

Manages session state with lightweight undo snapshots.

```python
from integration.session import SessionManager

manager = SessionManager(max_undo=10)

# Create session
session_id = manager.create_session()

# Add table
manager.add_table(session_id, "my_table", df)

# Save snapshot for undo
manager.save_snapshot(session_id, "my_table")

# Undo
manager.undo(session_id, "my_table")

# Redo
manager.redo(session_id, "my_table")
```

## Running the Applications

### Streamlit UI (Recommended)

```bash
# Install dependencies
pip install streamlit pandas

# Run
streamlit run ui/web/streamlit_app.py
```

The UI provides:
- 3-column Power BI-like layout
- Tabbed interface (Preview, Diff, Charts, SQL)
- Operation wizard with preview before apply
- Undo/redo support
- File upload and download

### FastAPI Backend

```bash
# Install dependencies
pip install fastapi uvicorn pandas

# Run
uvicorn backend.app:app --reload --port 8000
```

API Endpoints:
- `POST /sessions` - Create session
- `GET /sessions/{id}` - Get session info
- `POST /sessions/{id}/load` - Load table
- `GET /sessions/{id}/preview` - Preview data
- `POST /sessions/{id}/filter` - Filter data
- `POST /sessions/{id}/sort` - Sort data
- `POST /sessions/{id}/save` - Save to file
- `POST /sessions/{id}/undo` - Undo operation
- `GET /db/tables` - List database tables

### Connect Streamlit to Backend

To use the backend with Streamlit, replace the local adapter with REST API calls. Here's a complete example:

```python
# In ui/web/streamlit_app.py
import streamlit as st
import requests

BASE_URL = "http://localhost:8000"

# Step 1: Create a session
def create_session():
    """Create a new session via REST API."""
    try:
        response = requests.post(f"{BASE_URL}/sessions")
        response.raise_for_status()
        session_data = response.json()
        return session_data["id"]  # Capture session_id
    except requests.RequestException as e:
        st.error(f"Failed to create session: {e}")
        return None

# Step 2: Load a table
def load_table(session_id, file_path):
    """Load a CSV file into the session via REST API."""
    try:
        payload = {
            "type": "csv",
            "path": file_path
        }
        response = requests.post(
            f"{BASE_URL}/sessions/{session_id}/load",
            json=payload
        )
        response.raise_for_status()
        return response.json()  # Returns SessionMeta with tables, row_count, etc.
    except requests.RequestException as e:
        st.error(f"Failed to load table: {e}")
        return None

# Step 3: Preview the data
def preview_data(session_id, limit=100):
    """Get a preview of the current table."""
    try:
        response = requests.get(
            f"{BASE_URL}/sessions/{session_id}/preview",
            params={"limit": limit}
        )
        response.raise_for_status()
        return response.json()  # Returns PreviewResult with columns and rows
    except requests.RequestException as e:
        st.error(f"Failed to preview data: {e}")
        return None

# Step 4: Perform operations (example: filter)
def filter_data(session_id, column, operator, value):
    """Apply a filter via REST API."""
    try:
        payload = {
            "column": column,
            "operator": operator,
            "value": value,
            "limit": 200
        }
        response = requests.post(
            f"{BASE_URL}/sessions/{session_id}/filter",
            json=payload
        )
        response.raise_for_status()
        return response.json()  # Returns filtered preview and summary
    except requests.RequestException as e:
        st.error(f"Failed to filter data: {e}")
        return None

# Main app using REST API
def main():
    st.title("DataEngine Pro - Streamlit + REST Backend")
    
    import os
    from pathlib import Path
    
    # Initialize session
    if "session_id" not in st.session_state:
        session_id = create_session()
        if session_id:
            st.session_state.session_id = session_id
            st.success(f"Session created: {session_id}")
        else:
            st.stop()
    
    session_id = st.session_state.session_id
    
    # Load data
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    if uploaded_file:
        # Save uploaded file and load it (with path traversal protection)
        safe_filename = os.path.basename(uploaded_file.name)  # Remove path components
        file_path = str(Path("/tmp") / safe_filename)  # Safe path joining
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        session_info = load_table(session_id, file_path)
        if session_info:
            st.write(f"Loaded {session_info['row_count']} rows")
    
    # Preview data
    if st.button("Preview Data"):
        preview = preview_data(session_id)
        if preview:
            st.dataframe(preview["rows"])
    
    # Filter interface
    col1, col2, col3 = st.columns(3)
    with col1:
        column = st.text_input("Column to filter")
    with col2:
        operator = st.selectbox("Operator", ["==", "!=", ">", "<", "CONTAINS"])
    with col3:
        value = st.text_input("Filter value")
    
    if st.button("Apply Filter"):
        result = filter_data(session_id, column, operator, value)
        if result:
            st.dataframe(result["preview"]["rows"])
            st.metric("Rows after filter", len(result["preview"]["rows"]))

if __name__ == "__main__":
    main()
```

### Key Points for REST API Integration:

1. **Session Management**: Always capture the `session_id` from the initial POST request and pass it to all subsequent calls
2. **Error Handling**: Wrap each request in try-except and show user-friendly error messages
3. **Stateless Design**: The session state is managed on the backend; Streamlit only maintains the `session_id`
4. **Endpoint Pattern**: All session-scoped operations follow the pattern `/sessions/{session_id}/endpoint`
5. **Request/Response**: Use `requests.post()` for mutations and `requests.get()` for reads; always call `.json()` to parse responses

## Adapter API Reference

### Session Management

| Function | Description |
|----------|-------------|
| `create_session(session_id?)` | Create new session |
| `list_tables(session_id)` | List session tables |
| `get_active_table(session_id)` | Get active table |
| `get_dataframe(session_id, table?)` | Get DataFrame |

### Data Operations

| Function | Description |
|----------|-------------|
| `load_table(session_id, source)` | Load from CSV/Excel/SQLite |
| `preview(session_id, columns?, limit?, table?)` | Preview data |
| `sample(session_id, n?, table?)` | Random sample |
| `column_window(session_id, start, width?, table?)` | Column slice |
| `get_schema(session_id, table?)` | Get schema info |
| `get_diff(session_id, table?, limit?)` | Before/after diff |

### Transformations

| Function | Description |
|----------|-------------|
| `op_filter(session_id, column, operator, value, ...)` | Filter rows |
| `op_sort(session_id, columns, ascending, ...)` | Sort data |
| `op_aggregate(session_id, group_by, aggs, ...)` | Group & aggregate |
| `op_rank(session_id, by, method, new_col, ...)` | Add ranking |
| `op_pivot(session_id, rows, cols, values, agg, ...)` | Pivot table |
| `op_sql(session_id, sql, limit?)` | SQL query |

### Persistence

| Function | Description |
|----------|-------------|
| `save(session_id, dest, mode?, confirm?, table?)` | Save to file |
| `undo(session_id, table?)` | Undo last operation |

### Status

| Function | Description |
|----------|-------------|
| `op_status(op_id)` | Get operation status |
| `get_discovery_report()` | Get adapter mapping report |

## Customizing the Adapter

### Adding Custom Core Functions

To add a new core function to the adapter:

1. Implement the function in `data_engine/operations/`

2. The adapter automatically detects it via the `FunctionDiscovery` class

3. Or manually add to the mapping in `adapter.py`:

```python
# In FunctionDiscovery._discover_functions()
self.discovered["my_operation"] = "my_core_function"
```

### Example: Adding a Custom Filter

```python
# In data_engine/operations/custom.py
def op_custom_filter(df, column, value):
    """Custom filter implementation."""
    return df[df[column] == value]

# The adapter will automatically detect and use it
```

## Testing

### Run Unit Tests

```bash
# Install test dependencies
pip install pytest

# Run tests
pytest tests/test_integration.py -v
```

### Test Coverage

- Adapter session management
- Filter, sort, aggregate operations
- Column resolver fuzzy matching
- Session undo/redo
- File save/load

### Create Custom Tests

```python
# tests/test_custom.py
import pytest
from integration.adapter import Adapter

def test_custom_operation():
    adapter = Adapter()
    session = adapter.create_session()
    
    # Your test code here
    assert True
```

## Troubleshooting

### Core Module Not Found

If you see "Core modules not available":

1. Check that `data_engine` is installed:
   ```bash
   pip install -e .
   ```

2. Run the inspector:
   ```bash
   python integration/inspect_core.py
   ```

### Missing Operations

If an operation returns "not found":

1. Check the discovery report: `adapter.get_discovery_report()`
2. Implement the missing function in core modules
3. Or use the fallback implementations in the adapter

### Streamlit Issues

If Streamlit has issues:

1. Clear cache: `streamlit cache clear`
2. Check Python version: requires Python 3.8+
3. Install dependencies: `pip install streamlit pandas numpy`

### Backend Connection Issues

If backend can't connect:

1. Check port availability: `lsof -i :8000`
2. Verify CORS settings in `backend/app.py`
3. Check firewall settings

## Performance Tips

1. **Large Files**: Use the adapter's `preview()` instead of loading full data
2. **Memory**: The session manager stores snapshots as references for large datasets
3. **SQL**: Use parameterized queries in SQL operations for security

## Security

- SQL operations are restricted to read-only queries
- File paths are validated before save operations
- No eval/exec of user strings
- Session-bound data access only
