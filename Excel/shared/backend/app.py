#!/usr/bin/env python3
"""
DataEngine Pro - FastAPI Backend
=================================

Optional backend for multi-user/multi-process safety.
Provides REST API endpoints that mirror the adapter functions.

Usage:
    # Run the server
    uvicorn backend.app:app --reload --port 8000
    
    # Access API docs at http://localhost:8000/docs
"""

import os
import re
import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime

from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
import io
import uuid

# Add parent directory to path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

# Try to import adapter
try:
    from shared.integration.adapter import Adapter, get_adapter
    ADAPTER_AVAILABLE = True
except ImportError:
    ADAPTER_AVAILABLE = False


# =============================================================================
# APP SETUP
# =============================================================================

app = FastAPI(
    title="DataEngine Pro API",
    description="REST API for DataEngine Pro data operations",
    version="1.0.0"
)

# CORS middleware - configurable via environment variable
CORS_ORIGINS = os.environ.get("CORS_ORIGINS", "").split(",") if os.environ.get("CORS_ORIGINS") else ["http://localhost:3000", "http://localhost:8501"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Content-Type", "Authorization"],
)

# Initialize adapter
adapter = get_adapter() if ADAPTER_AVAILABLE else None


# =============================================================================
# MODELS
# =============================================================================

class SessionResponse(BaseModel):
    id: str
    tables: List[str]
    active_table: str
    row_count: int
    created_at: str


class SourceSpec(BaseModel):
    type: str  # sqlite, csv, excel
    path: str
    table: Optional[str] = None
    sheet: Optional[int] = 0


class DestinationSpec(BaseModel):
    type: str  # sqlite, csv, excel
    path: str
    table: Optional[str] = None


class FilterSpec(BaseModel):
    column: str
    operator: str
    value: Any
    preview_columns: Optional[List[str]] = None
    limit: int = 200


class SortSpec(BaseModel):
    columns: List[str]
    ascending: bool = True
    preview_columns: Optional[List[str]] = None
    limit: int = 200


class AggregateSpec(BaseModel):
    group_by: List[str]
    aggs: Dict[str, str]
    preview_columns: Optional[List[str]] = None
    limit: int = 200


class RankSpec(BaseModel):
    by: str
    method: str = "dense"
    new_col: str = "rank"
    top_n: Optional[int] = None
    preview_columns: Optional[List[str]] = None


class PivotSpec(BaseModel):
    rows: List[str]
    cols: Optional[List[str]]
    values: str
    agg: str = "sum"
    preview_columns: Optional[List[str]] = None
    limit: int = 200


# =============================================================================
# DATABASE HELPERS
# =============================================================================

def get_db_connection(db_path: str = "data.db"):
    """Get SQLite connection."""
    return sqlite3.connect(db_path)


def get_tables_from_db(db_path: str = "data.db") -> List[str]:
    """Get list of tables in database."""
    try:
        conn = get_db_connection(db_path)
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tables
    except Exception:
        return []


def get_table_schema(db_path: str, table: str) -> List[Dict[str, str]]:
    """Get table schema."""
    # Validate table name to prevent injection
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table):
        return []
    
    try:
        conn = get_db_connection(db_path)
        cursor = conn.execute(f'PRAGMA table_info("{table}")')
        schema = [{"name": row[1], "type": row[2]} for row in cursor.fetchall()]
        conn.close()
        return schema
    except Exception:
        return []


# =============================================================================
# SESSION ENDPOINTS
# =============================================================================

@app.get("/")
def root():
    """Root endpoint."""
    return {
        "name": "DataEngine Pro API",
        "version": "1.0.0",
        "adapter_available": ADAPTER_AVAILABLE
    }


@app.post("/sessions", response_model=SessionResponse)
def create_session():
    """Create a new session."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not available")
    
    session = adapter.create_session()
    return SessionResponse(
        id=session.id,
        tables=session.tables,
        active_table=session.active_table,
        row_count=session.row_count,
        created_at=session.created_at
    )


@app.get("/sessions/{session_id}", response_model=SessionResponse)
def get_session(session_id: str):
    """Get session info."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not available")
    
    tables = adapter.list_tables(session_id)
    active_table = adapter.get_active_table(session_id)
    
    row_count = 0
    if active_table:
        df = adapter.get_dataframe(session_id, active_table)
        row_count = len(df) if df is not None else 0
    
    return SessionResponse(
        id=session_id,
        tables=tables,
        active_table=active_table,
        row_count=row_count,
        created_at=datetime.now().isoformat()
    )


@app.delete("/sessions/{session_id}")
def delete_session(session_id: str):
    """Delete a session."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not available")
    
    if session_id in adapter._sessions:
        del adapter._sessions[session_id]
        return {"status": "deleted"}
    
    raise HTTPException(status_code=404, detail="Session not found")


# =============================================================================
# TABLE ENDPOINTS
# =============================================================================

@app.get("/db/tables")
def list_db_tables(db: str = Query("data.db")):
    """List tables in database."""
    tables = get_tables_from_db(db)
    return {"tables": tables, "db": db}


@app.get("/db/tables/{table}/schema")
def get_db_table_schema(table: str, db: str = Query("data.db")):
    """Get table schema."""
    schema = get_table_schema(db, table)
    return {"table": table, "schema": schema}


@app.get("/db/tables/{table}/preview")
def preview_db_table(
    table: str,
    db: str = Query("data.db"),
    limit: int = Query(200, le=1000),
    offset: int = Query(0)
):
    """Preview table data."""
    # Validate table name to prevent injection
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table):
        raise HTTPException(status_code=400, detail="Invalid table name")
    
    try:
        conn = get_db_connection(db)
        # Verify table exists before querying
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,)
        )
        if not cursor.fetchone():
            conn.close()
            raise HTTPException(status_code=404, detail="Table not found")
        
        df = pd.read_sql_query(
            f'SELECT * FROM "{table}" LIMIT {limit} OFFSET {offset}',
            conn
        )
        conn.close()
        
        return {
            "columns": list(df.columns),
            "rows": df.to_dict("records"),
            "summary": {
                "limit": limit,
                "offset": offset,
                "rows_returned": len(df)
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# LOAD/SAVE ENDPOINTS
# =============================================================================

@app.post("/sessions/{session_id}/load")
def load_table(session_id: str, source: SourceSpec):
    """Load table into session."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not available")
    
    try:
        session = adapter.load_table(
            session_id,
            source.dict()
        )
        return {
            "status": "loaded",
            "session": session.dict()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sessions/{session_id}/save")
def save_table(
    session_id: str,
    dest: DestinationSpec,
    mode: str = Form("replace"),
    table_name: Optional[str] = Form(None)
):
    """Save table from session to file."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not available")
    
    try:
        result = adapter.save(
            session_id,
            dest.dict(),
            mode=mode,
            table_name=table_name
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sessions/{session_id}/download")
def download_table(
    session_id: str,
    table_name: Optional[str] = None,
    format: str = Query("csv"),
    limit: int = Query(10000, le=100000)
):
    """Download table as CSV or Excel."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not available")
    
    try:
        df = adapter.get_dataframe(session_id, table_name)
        if df is None:
            raise HTTPException(status_code=404, detail="Table not found")
        
        df = df.head(limit)
        
        if format == "csv":
            csv = df.to_csv(index=False)
            return StreamingResponse(
                io.StringIO(csv),
                media_type="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename={table_name or 'data'}.csv"
                }
            )
        elif format == "excel":
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name=table_name or "Sheet1")
            
            return StreamingResponse(
                io.BytesIO(buffer.getvalue()),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={
                    "Content-Disposition": f"attachment; filename={table_name or 'data'}.xlsx"
                }
            )
        else:
            raise HTTPException(status_code=400, detail="Unsupported format")
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# OPERATION ENDPOINTS
# =============================================================================

@app.get("/sessions/{session_id}/schema")
def get_schema(session_id: str, table_name: Optional[str] = None):
    """Get table schema."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not available")
    
    try:
        schema = adapter.get_schema(session_id, table_name)
        return schema
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sessions/{session_id}/preview")
def preview_table(
    session_id: str,
    table_name: Optional[str] = None,
    columns: Optional[str] = None,
    limit: int = Query(200, le=1000)
):
    """Preview table data."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not available")
    
    try:
        cols = columns.split(",") if columns else None
        preview = adapter.preview(
            session_id,
            columns=cols,
            limit=limit,
            table_name=table_name
        )
        return {
            "columns": preview.columns,
            "rows": preview.rows,
            "summary": preview.summary
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sessions/{session_id}/sample")
def sample_table(
    session_id: str,
    table_name: Optional[str] = None,
    n: int = Query(200, le=1000)
):
    """Get random sample."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not available")
    
    try:
        sample = adapter.sample(session_id, n=n, table_name=table_name)
        return {
            "columns": sample.columns,
            "rows": sample.rows,
            "summary": sample.summary
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sessions/{session_id}/column_window")
def column_window(
    session_id: str,
    start_idx: int = Query(0),
    width: int = Query(10, le=50),
    table_name: Optional[str] = None
):
    """Get column window."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not available")
    
    try:
        result = adapter.column_window(
            session_id,
            start_idx=start_idx,
            width=width,
            table_name=table_name
        )
        return {
            "columns": result.columns,
            "rows": result.rows,
            "summary": result.summary
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sessions/{session_id}/filter")
def filter_table(session_id: str, spec: FilterSpec, table_name: Optional[str] = None):
    """Filter table."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not available")
    
    try:
        result = adapter.op_filter(
            session_id,
            column=spec.column,
            operator=spec.operator,
            value=spec.value,
            preview_columns=spec.preview_columns,
            limit=spec.limit,
            table_name=table_name
        )
        return {
            "op_id": result.op_id,
            "status": result.status,
            "preview": {
                "columns": result.preview.columns,
                "rows": result.preview.rows
            },
            "summary": result.summary
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sessions/{session_id}/sort")
def sort_table(session_id: str, spec: SortSpec, table_name: Optional[str] = None):
    """Sort table."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not available")
    
    try:
        result = adapter.op_sort(
            session_id,
            columns=spec.columns,
            ascending=spec.ascending,
            preview_columns=spec.preview_columns,
            limit=spec.limit,
            table_name=table_name
        )
        return {
            "op_id": result.op_id,
            "status": result.status,
            "preview": {
                "columns": result.preview.columns,
                "rows": result.preview.rows
            },
            "summary": result.summary
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sessions/{session_id}/aggregate")
def aggregate_table(session_id: str, spec: AggregateSpec, table_name: Optional[str] = None):
    """Aggregate table."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not available")
    
    try:
        result = adapter.op_aggregate(
            session_id,
            group_by=spec.group_by,
            aggs=spec.aggs,
            preview_columns=spec.preview_columns,
            limit=spec.limit,
            table_name=table_name
        )
        return {
            "op_id": result.op_id,
            "status": result.status,
            "preview": {
                "columns": result.preview.columns,
                "rows": result.preview.rows
            },
            "summary": result.summary
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sessions/{session_id}/rank")
def rank_table(session_id: str, spec: RankSpec, table_name: Optional[str] = None):
    """Rank table."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not available")
    
    try:
        result = adapter.op_rank(
            session_id,
            by=spec.by,
            method=spec.method,
            new_col=spec.new_col,
            top_n=spec.top_n,
            preview_columns=spec.preview_columns,
            table_name=table_name
        )
        return {
            "op_id": result.op_id,
            "status": result.status,
            "preview": {
                "columns": result.preview.columns,
                "rows": result.preview.rows
            },
            "summary": result.summary
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sessions/{session_id}/pivot")
def pivot_table(session_id: str, spec: PivotSpec, table_name: Optional[str] = None):
    """Create pivot table."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not available")
    
    try:
        result = adapter.op_pivot(
            session_id,
            rows=spec.rows,
            cols=spec.cols,
            values=spec.values,
            agg=spec.agg,
            preview_columns=spec.preview_columns,
            limit=spec.limit,
            table_name=table_name
        )
        return {
            "op_id": result.op_id,
            "status": result.status,
            "preview": {
                "columns": result.preview.columns,
                "rows": result.preview.rows
            },
            "summary": result.summary
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sessions/{session_id}/sql")
def sql_query(
    session_id: str,
    sql: str = Form(...),
    limit: int = Form(200),
    table_name: Optional[str] = None
):
    """Execute SQL query."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not available")
    
    try:
        result = adapter.op_sql(session_id, sql, limit)
        return {
            "op_id": result.op_id,
            "status": result.status,
            "preview": {
                "columns": result.preview.columns,
                "rows": result.preview.rows
            },
            "summary": result.summary
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# UNDO/REDO
# =============================================================================

@app.post("/sessions/{session_id}/undo")
def undo(session_id: str, table_name: Optional[str] = None):
    """Undo last operation."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not available")
    
    try:
        success = adapter.undo(session_id, table_name)
        return {"status": "success" if success else "no_op"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sessions/{session_id}/diff")
def get_diff(session_id: str, table_name: Optional[str] = None, limit: int = Query(50)):
    """Get before/after diff."""
    if not adapter:
        raise HTTPException(status_code=500, detail="Adapter not available")
    
    try:
        diff = adapter.get_diff(session_id, table_name, limit)
        return diff
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# DISCOVERY
# =============================================================================

@app.get("/discovery")
def get_discovery():
    """Get adapter discovery report."""
    if not adapter:
        return {"error": "Adapter not available"}
    
    return adapter.get_discovery_report()


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
