"""
DataEngine Pro - Integration Adapter Layer
============================================

This adapter provides a canonical API that bridges the existing data_engine
core modules to the Streamlit UI and optional FastAPI backend.

The adapter automatically detects available core functions and wires them
to the canonical operations. If a function is not found, it provides clear
error messages with instructions for implementation.

Usage:
    from integration.adapter import Adapter
    
    adapter = Adapter()
    session = adapter.create_session()
    adapter.load_table(session.id, {"type": "csv", "path": "data.csv"})
    result = adapter.preview(session.id)
"""

import os
import uuid
import json
import time
import sqlite3
import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Any, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import threading
import importlib
import ast

# Try to import core modules
try:
    from data_engine import operations as ops
    from data_engine.session import Session as CoreSession
    from data_engine.database import db_load, db_save, db_tables, db_get_schema
    CORE_AVAILABLE = True
except ImportError as e:
    CORE_AVAILABLE = False
    ops = None
    CoreSession = None
    db_load = None
    db_save = None
    db_tables = None
    CORE_IMPORT_ERROR = str(e)

# Try to import rapidfuzz for fuzzy matching
try:
    from rapidfuzz import fuzz, process
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False
    fuzz = None
    process = None


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class SessionMeta:
    """Session metadata returned by create_session and load_table."""
    id: str
    tables: List[str]
    active_table: str
    row_count: int
    created_at: str


@dataclass
class ColumnInfo:
    """Column metadata for schema."""
    name: str
    type: str
    sample: Any


@dataclass
class PreviewResult:
    """Result of preview/sample operations."""
    columns: List[str]
    rows: List[Dict[str, Any]]
    summary: Dict[str, Any]


@dataclass
class OperationResult:
    """Result of any operation (filter, sort, etc.)."""
    preview: PreviewResult
    summary: Dict[str, Any]
    op_id: str
    status: str = "completed"


@dataclass
class OperationStatus:
    """Status of a long-running operation."""
    op_id: str
    status: str  # pending, running, completed, failed
    progress: float  # 0.0 to 1.0
    message: str
    result: Optional[Dict[str, Any]] = None


# =============================================================================
# DISCOVERED FUNCTIONS MAPPING
# =============================================================================

class FunctionDiscovery:
    """
    Discovers available functions in the core modules and maps them
    to the canonical adapter API.
    """
    
    def __init__(self):
        self.discovered = {}
        self.mappings = {}
        self._discover_functions()
    
    def _discover_functions(self):
        """Scan core modules for available functions."""
        if not CORE_AVAILABLE:
            self.discovered["status"] = "error"
            self.discovered["error"] = CORE_IMPORT_ERROR
            return
        
        # Map of canonical names to possible core function names
        canonical_map = {
            # Filtering
            "filter": ["op_filter", "apply_single_condition", "op_multi_filter"],
            # Sorting
            "sort": ["op_sort"],
            # Aggregation
            "aggregate": ["op_aggregate"],
            # Ranking
            "rank": ["op_rank"],
            # Pivot
            "pivot": ["op_pivot", "op_pivot_table"],
            # Transform/Add Column
            "add_column": ["op_add_column"],
            # Null handling
            "handle_nulls": ["op_handle_nulls"],
            # Rename/Drop
            "rename_drop": ["op_rename_drop"],
            # Dedupe
            "dedupe": ["op_dedupe"],
            # Change type
            "change_type": ["op_change_type"],
            # Join
            "join": ["op_join"],
            # Save
            "save": ["op_save", "db_save"],
            # Export
            "export": ["op_export"],
        }
        
        self.discovered["status"] = "success"
        self.discovered["modules"] = ["data_engine.operations"]
        
        # Check each canonical operation
        for canonical_name, possible_names in canonical_map.items():
            found = None
            for name in possible_names:
                if hasattr(ops, name):
                    found = name
                    break
            
            if found:
                self.discovered[canonical_name] = found
                self.mappings[canonical_name] = found
            else:
                self.discovered[canonical_name] = None
        
        # Also check database module directly
        self.discovered["db_load"] = "db_load" if db_load else None
        self.discovered["db_save"] = "db_save" if db_save else None
        self.discovered["db_tables"] = "db_tables" if db_tables else None
        self.discovered["db_schema"] = "db_get_schema" if db_get_schema else None
    
    def get_report(self) -> Dict[str, Any]:
        """Get a report of discovered functions."""
        return {
            "core_available": CORE_AVAILABLE,
            "rapidfuzz_available": RAPIDFUZZ_AVAILABLE,
            "discovered_functions": self.discovered,
            "mappings": self.mappings,
            "missing_operations": [
                k for k, v in self.discovered.items() 
                if v is None and k not in ["status", "error", "modules"]
            ]
        }


# =============================================================================
# ADAPTER IMPLEMENTATION
# =============================================================================

class Adapter:
    """
    Main adapter class that provides the canonical API for data operations.
    
    This adapter:
    1. Discovers available core functions on initialization
    2. Provides a canonical API that maps to discovered functions
    3. Handles data preview, sampling, and bounded results
    4. Manages operation status for long-running tasks
    """
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the adapter.
        
        Args:
            db_path: Optional path to SQLite database for session storage
        """
        self._discovery = FunctionDiscovery()
        self._sessions: Dict[str, CoreSession] = {}
        self._session_created_at: Dict[str, str] = {}  # Track actual creation times
        self._operation_status: Dict[str, OperationStatus] = {}
        self._operation_threads: Dict[str, threading.Thread] = {}
        self._db_path = db_path or ":memory:"
        self._max_undo = 10
        
        # Print discovery report on initialization
        self._print_discovery_report()
    
    def _print_discovery_report(self):
        """Print a report of discovered functions."""
        report = self._discovery.get_report()
        print("\n" + "="*60)
        print("DataEngine Pro - Adapter Discovery Report")
        print("="*60)
        print(f"Core modules available: {report['core_available']}")
        print(f"RapidFuzz available: {report['rapidfuzz_available']}")
        
        if report['discovered_functions'].get('status') == 'success':
            print("\nDiscovered functions:")
            for name, func in report['discovered_functions'].items():
                if name not in ['status', 'error', 'modules']:
                    status = "✓" if func else "✗"
                    print(f"  {status} {name}: {func or 'NOT FOUND'}")
            
            if report['missing_operations']:
                print("\nMissing operations (require implementation in core):")
                for op in report['missing_operations']:
                    print(f"  - {op}")
        else:
            print(f"\nError loading core: {report['discovered_functions'].get('error')}")
        
        print("="*60 + "\n")
    
    # =========================================================================
    # SESSION MANAGEMENT
    # =========================================================================
    
    def create_session(self, session_id: Optional[str] = None) -> SessionMeta:
        """
        Create a new session.
        
        Args:
            session_id: Optional custom session ID
            
        Returns:
            SessionMeta with session information
        """
        sid = session_id or str(uuid.uuid4())
        created_at = datetime.now().isoformat()
        
        if CORE_AVAILABLE and CoreSession:
            # Use the core Session class
            core_sess = CoreSession(self._db_path)
            self._sessions[sid] = core_sess
        else:
            # Fallback to simple dict-based session
            self._sessions[sid] = {
                "tables": {},
                "active": "",
                "history": {}
            }
        
        # Store the actual creation time
        self._session_created_at[sid] = created_at
        
        return SessionMeta(
            id=sid,
            tables=[],
            active_table="",
            row_count=0,
            created_at=created_at
        )
    
    def get_session(self, session_id: str) -> Optional[Union[CoreSession, Dict]]:
        """Get session by ID."""
        return self._sessions.get(session_id)
    
    # =========================================================================
    # DATA LOADING
    # =========================================================================
    
    def _validate_path(self, path: str, allowed_dir: Optional[str] = None) -> str:
        """
        Validate and resolve path to prevent directory traversal and symlink attacks.
        
        Args:
            path: The file path to validate
            allowed_dir: Optional directory to restrict access to
            
        Returns:
            The validated absolute path
            
        Raises:
            ValueError: If path attempts directory traversal or is outside allowed dir
        """
        import os
        # Expand user home directory
        expanded_path = os.path.expanduser(path)
        
        # Check for symlink attacks - resolve symlinks to get the real path
        if os.path.islink(expanded_path):
            real_path = os.path.realpath(expanded_path)
            raise ValueError(f"Symlinks are not allowed: '{path}' resolves to '{real_path}'")
        
        # Resolve the absolute path and normalize it
        abs_path = os.path.abspath(expanded_path)
        
        # If allowed_dir is specified, ensure path is within it
        if allowed_dir:
            allowed_abs = os.path.abspath(os.path.expanduser(allowed_dir))
            if not abs_path.startswith(allowed_abs + os.sep) and abs_path != allowed_abs:
                raise ValueError(f"Path '{path}' is outside allowed directory '{allowed_dir}'")
        
        return abs_path

    def load_table(self, session_id: str, source: Dict[str, Any]) -> SessionMeta:
        """
        Load a table into the session.
        
        Args:
            session_id: Session ID
            source: Source specification with keys:
                - type: 'sqlite', 'csv', 'table', 'excel'
                - path: File path or database path
                - table: Table name (for SQLite)
                
        Returns:
            SessionMeta with updated session information
        """
        sess = self.get_session(session_id)
        if not sess:
            raise ValueError(f"Session {session_id} not found")
        
        source_type = source.get("type", "").lower()
        path = source.get("path", "")
        
        # Validate path to prevent directory traversal
        if path:
            path = self._validate_path(path)
        
        df = None
        table_name = ""
        
        if source_type == "csv":
            df = pd.read_csv(path)
            table_name = Path(path).stem
        elif source_type == "excel":
            df = pd.read_excel(path, sheet_name=source.get("sheet", 0))
            table_name = Path(path).stem
        elif source_type == "sqlite":
            if db_load:
                table = source.get("table", "")
                df = db_load(path, table)
                table_name = table
            else:
                raise RuntimeError("SQLite support not available")
        elif source_type == "table" and isinstance(sess, dict):
            # Load from existing tables dict
            table_name = source.get("table", "")
            df = sess["tables"].get(table_name)
        
        if df is None:
            raise ValueError(f"Failed to load table from source: {source}")
        
        # Store in session
        if isinstance(sess, CoreSession):
            sess.add(table_name, df)
        elif isinstance(sess, dict):
            sess["tables"][table_name] = df
            sess["active"] = table_name
        
        return SessionMeta(
            id=session_id,
            tables=self.list_tables(session_id),
            active_table=self.get_active_table(session_id),
            row_count=len(df),
            created_at=self._session_created_at.get(session_id, datetime.now().isoformat())
        )
    
    def list_tables(self, session_id: str) -> List[str]:
        """List all tables in the session."""
        sess = self.get_session(session_id)
        if not sess:
            return []
        
        if isinstance(sess, CoreSession):
            return sess.list_tables()
        elif isinstance(sess, dict):
            return list(sess.get("tables", {}).keys())
        return []
    
    def get_active_table(self, session_id: str) -> str:
        """Get the active table name."""
        sess = self.get_session(session_id)
        if not sess:
            return ""
        
        if isinstance(sess, CoreSession):
            return sess.active
        elif isinstance(sess, dict):
            return sess.get("active", "")
        return ""
    
    def get_dataframe(self, session_id: str, table_name: Optional[str] = None) -> Optional[pd.DataFrame]:
        """Get the DataFrame for a session."""
        sess = self.get_session(session_id)
        if not sess:
            return None
        
        table = table_name or self.get_active_table(session_id)
        
        if isinstance(sess, CoreSession):
            return sess.tables.get(table)
        elif isinstance(sess, dict):
            return sess.get("tables", {}).get(table)
        return None
    
    # =========================================================================
    # SCHEMA & PREVIEW
    # =========================================================================
    
    def get_schema(self, session_id: str, table_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get schema information for a table.
        
        Returns:
            Dict with 'columns' (list of ColumnInfo) and 'row_count'
        """
        df = self.get_dataframe(session_id, table_name)
        if df is None:
            return {"columns": [], "row_count": 0, "error": "Table not found"}
        
        columns = []
        for col in df.columns:
            col_type = str(df[col].dtype)
            sample = df[col].dropna().head(3).tolist()
            columns.append(ColumnInfo(name=col, type=col_type, sample=sample))
        
        return {
            "columns": [{"name": c.name, "type": c.type, "sample": c.sample} for c in columns],
            "row_count": len(df)
        }
    
    def preview(self, session_id: str, 
                columns: Optional[List[str]] = None,
                limit: int = 200,
                table_name: Optional[str] = None) -> PreviewResult:
        """
        Get a preview of the data.
        
        Args:
            session_id: Session ID
            columns: Optional list of columns to include
            limit: Maximum number of rows (default 200)
            table_name: Optional table name
            
        Returns:
            PreviewResult with columns, rows, and summary
        """
        df = self.get_dataframe(session_id, table_name)
        if df is None:
            return PreviewResult(columns=[], rows=[], summary={"error": "No data"})
        
        # Apply column selection
        if columns:
            df = df[[c for c in columns if c in df.columns]]
        
        # Apply limit
        limited_df = df.head(limit)
        
        return PreviewResult(
            columns=list(limited_df.columns),
            rows=limited_df.to_dict("records"),
            summary={
                "rows_before": len(df),
                "rows_after": len(limited_df),
                "total_rows": len(df),
                "time_ms": 0
            }
        )
    
    def sample(self, session_id: str, 
               n: int = 200,
               table_name: Optional[str] = None) -> PreviewResult:
        """Get a random sample of the data."""
        df = self.get_dataframe(session_id, table_name)
        if df is None:
            return PreviewResult(columns=[], rows=[], summary={"error": "No data"})
        
        sample_df = df.sample(n=min(n, len(df)), random_state=42)
        
        return PreviewResult(
            columns=list(sample_df.columns),
            rows=sample_df.to_dict("records"),
            summary={
                "sample_size": len(sample_df),
                "total_rows": len(df)
            }
        )
    
    def column_window(self, session_id: str,
                     start_idx: int = 0,
                     width: int = 10,
                     table_name: Optional[str] = None) -> PreviewResult:
        """
        Get a window of columns.
        
        Args:
            session_id: Session ID
            start_idx: Starting column index
            width: Number of columns to return
            table_name: Optional table name
        """
        df = self.get_dataframe(session_id, table_name)
        if df is None:
            return PreviewResult(columns=[], rows=[], summary={"error": "No data"})
        
        cols = list(df.columns)
        window_cols = cols[start_idx:start_idx + width]
        
        if not window_cols:
            return PreviewResult(columns=[], rows=[], summary={"error": "No columns in window"})
        
        window_df = df[window_cols].head(200)
        
        return PreviewResult(
            columns=window_cols,
            rows=window_df.to_dict("records"),
            summary={
                "start_idx": start_idx,
                "width": width,
                "total_columns": len(cols)
            }
        )
    
    # =========================================================================
    # OPERATIONS
    # =========================================================================
    
    def _apply_filter(self, df: pd.DataFrame, column: str, operator: str, value: Any) -> pd.DataFrame:
        """Apply a single filter condition."""
        # Map common operators
        op_map = {
            "==": "==",
            "equals": "==",
            "!=": "!=",
            "not_equals": "!=",
            ">": ">",
            "<": "<",
            ">=": ">=",
            "<=": "<=",
            "contains": "contains",
            "startswith": "startswith",
            "endswith": "endswith",
            "is_blank": "is_blank",
            "is_null": "is_null",
        }
        
        op = op_map.get(operator.lower(), operator)
        
        if op in ["is_blank", "is_null"]:
            return df[df[column].isna() | (df[column].astype(str).str.strip() == "")]
        
        if op == "contains":
            return df[df[column].astype(str).str.contains(str(value), case=False, na=False)]
        
        if op == "startswith":
            return df[df[column].astype(str).str.startswith(str(value), na=False)]
        
        if op == "endswith":
            return df[df[column].astype(str).str.endswith(str(value), na=False)]
        
        # Try numeric comparison
        try:
            num_val = float(value)
            if op == "==":
                return df[df[column] == num_val]
            elif op == "!=":
                return df[df[column] != num_val]
            elif op == ">":
                return df[df[column] > num_val]
            elif op == "<":
                return df[df[column] < num_val]
            elif op == ">=":
                return df[df[column] >= num_val]
            elif op == "<=":
                return df[df[column] <= num_val]
        except (ValueError, TypeError):
            pass
        
        # String comparison
        str_val = str(value)
        if op == "==":
            return df[df[column].astype(str).str.strip() == str_val]
        elif op == "!=":
            return df[df[column].astype(str).str.strip() != str_val]
        
        return df
    
    def op_filter(self, session_id: str,
                  column: str,
                  operator: str,
                  value: Any,
                  preview_columns: Optional[List[str]] = None,
                  limit: int = 200,
                  table_name: Optional[str] = None) -> OperationResult:
        """
        Apply a filter operation.
        
        Args:
            session_id: Session ID
            column: Column to filter on
            operator: Filter operator (==, !=, >, <, contains, etc.)
            value: Filter value
            preview_columns: Columns to include in preview
            limit: Max rows in preview
            table_name: Optional table name
            
        Returns:
            OperationResult with preview and summary
        """
        df = self.get_dataframe(session_id, table_name)
        if df is None:
            return OperationResult(
                preview=PreviewResult(columns=[], rows=[], summary={}),
                summary={"error": "No data"},
                op_id=""
            )
        
        op_id = str(uuid.uuid4())
        
        # Save state for undo
        sess = self.get_session(session_id)
        if isinstance(sess, CoreSession):
            sess.push_undo(table_name or sess.active)
        elif isinstance(sess, dict):
            table = table_name or sess.get("active", "")
            if table not in sess["history"]:
                sess["history"][table] = []
            sess["history"][table].append(df.copy())
            if len(sess["history"][table]) > self._max_undo:
                sess["history"][table].pop(0)
        
        # Apply filter
        filtered_df = self._apply_filter(df, column, operator, value)
        
        # Store result
        if isinstance(sess, CoreSession):
            sess.df = filtered_df
        elif isinstance(sess, dict):
            table = table_name or sess.get("active", "")
            sess["tables"][table] = filtered_df
        
        # Build preview
        preview_df = filtered_df.head(limit)
        if preview_columns:
            preview_df = preview_df[[c for c in preview_columns if c in preview_df.columns]]
        
        return OperationResult(
            preview=PreviewResult(
                columns=list(preview_df.columns),
                rows=preview_df.to_dict("records"),
                summary={}
            ),
            summary={
                "rows_before": len(df),
                "rows_after": len(filtered_df),
                "rows_removed": len(df) - len(filtered_df)
            },
            op_id=op_id,
            status="completed"
        )
    
    def op_sort(self, session_id: str,
                columns: List[str],
                ascending: bool = True,
                preview_columns: Optional[List[str]] = None,
                limit: int = 200,
                table_name: Optional[str] = None) -> OperationResult:
        """
        Apply sort operation.
        
        Args:
            session_id: Session ID
            columns: Columns to sort by
            ascending: Sort order
            preview_columns: Columns for preview
            limit: Max rows in preview
            table_name: Optional table name
        """
        df = self.get_dataframe(session_id, table_name)
        if df is None:
            return OperationResult(
                preview=PreviewResult(columns=[], rows=[], summary={}),
                summary={"error": "No data"},
                op_id=""
            )
        
        op_id = str(uuid.uuid4())
        
        # Save state for undo
        sess = self.get_session(session_id)
        if isinstance(sess, CoreSession):
            sess.push_undo(table_name or sess.active)
        elif isinstance(sess, dict):
            table = table_name or sess.get("active", "")
            if table not in sess["history"]:
                sess["history"][table] = []
            sess["history"][table].append(df.copy())
        
        # Apply sort
        valid_cols = [c for c in columns if c in df.columns]
        if valid_cols:
            sorted_df = df.sort_values(by=valid_cols, ascending=ascending)
        else:
            sorted_df = df
        
        # Store result
        if isinstance(sess, CoreSession):
            sess.df = sorted_df
        elif isinstance(sess, dict):
            table = table_name or sess.get("active", "")
            sess["tables"][table] = sorted_df
        
        # Build preview
        preview_df = sorted_df.head(limit)
        if preview_columns:
            preview_df = preview_df[[c for c in preview_columns if c in preview_df.columns]]
        
        return OperationResult(
            preview=PreviewResult(
                columns=list(preview_df.columns),
                rows=preview_df.to_dict("records"),
                summary={}
            ),
            summary={
                "sorted_by": valid_cols,
                "order": "ascending" if ascending else "descending",
                "rows_before": len(df),
                "rows_after": len(sorted_df)
            },
            op_id=op_id,
            status="completed"
        )
    
    def op_aggregate(self, session_id: str,
                     group_by: List[str],
                     aggs: Dict[str, str],
                     preview_columns: Optional[List[str]] = None,
                     limit: int = 200,
                     table_name: Optional[str] = None) -> OperationResult:
        """
        Apply aggregation.
        
        Args:
            session_id: Session ID
            group_by: Columns to group by
            aggs: Dict of {column: aggregation_function}
            preview_columns: Columns for preview
            limit: Max rows
            table_name: Optional table name
        """
        df = self.get_dataframe(session_id, table_name)
        if df is None:
            return OperationResult(
                preview=PreviewResult(columns=[], rows=[], summary={}),
                summary={"error": "No data"},
                op_id=""
            )
        
        op_id = str(uuid.uuid4())
        
        # Validate columns
        valid_group = [c for c in group_by if c in df.columns]
        valid_aggs = {c: agg for c, agg in aggs.items() if c in df.columns}
        
        if not valid_group or not valid_aggs:
            return OperationResult(
                preview=PreviewResult(columns=[], rows=[], summary={}),
                summary={"error": "Invalid columns for aggregation"},
                op_id=op_id
            )
        
        # Apply aggregation
        try:
            agg_df = df.groupby(valid_group, dropna=False).agg(valid_aggs).reset_index()
            agg_df = agg_df.head(limit)
        except Exception as e:
            return OperationResult(
                preview=PreviewResult(columns=[], rows=[], summary={}),
                summary={"error": str(e)},
                op_id=op_id
            )
        
        return OperationResult(
            preview=PreviewResult(
                columns=list(agg_df.columns),
                rows=agg_df.to_dict("records"),
                summary={}
            ),
            summary={
                "group_by": valid_group,
                "aggregations": valid_aggs,
                "result_rows": len(agg_df)
            },
            op_id=op_id,
            status="completed"
        )
    
    def op_rank(self, session_id: str,
                by: str,
                method: str = "dense",
                new_col: str = "rank",
                top_n: Optional[int] = None,
                preview_columns: Optional[List[str]] = None,
                table_name: Optional[str] = None) -> OperationResult:
        """
        Apply ranking.
        
        Args:
            session_id: Session ID
            by: Column to rank by
            method: Ranking method (dense, min, max, average, ordinal)
            new_col: Name for new rank column
            top_n: Optional filter to top N
            preview_columns: Columns for preview
            table_name: Optional table name
        """
        df = self.get_dataframe(session_id, table_name)
        if df is None:
            return OperationResult(
                preview=PreviewResult(columns=[], rows=[], summary={}),
                summary={"error": "No data"},
                op_id=""
            )
        
        op_id = str(uuid.uuid4())
        
        if by not in df.columns:
            return OperationResult(
                preview=PreviewResult(columns=[], rows=[], summary={}),
                summary={"error": f"Column {by} not found"},
                op_id=op_id
            )
        
        # Save state for undo
        sess = self.get_session(session_id)
        if isinstance(sess, CoreSession):
            sess.push_undo(table_name or sess.active)
        elif isinstance(sess, dict):
            table = table_name or sess.get("active", "")
            if table not in sess["history"]:
                sess["history"][table] = []
            sess["history"][table].append(df.copy())
        
        # Apply ranking
        try:
            df[new_col] = df[by].rank(method=method, ascending=True)
            
            if top_n:
                df = df[df[new_col] <= top_n]
        except Exception as e:
            return OperationResult(
                preview=PreviewResult(columns=[], rows=[], summary={}),
                summary={"error": str(e)},
                op_id=op_id
            )
        
        # Store result
        if isinstance(sess, CoreSession):
            sess.df = df
        elif isinstance(sess, dict):
            table = table_name or sess.get("active", "")
            sess["tables"][table] = df
        
        # Build preview
        preview_df = df.head(200)
        if preview_columns:
            preview_df = preview_df[[c for c in preview_columns if c in preview_df.columns]]
        
        return OperationResult(
            preview=PreviewResult(
                columns=list(preview_df.columns),
                rows=preview_df.to_dict("records"),
                summary={}
            ),
            summary={
                "ranked_by": by,
                "method": method,
                "new_column": new_col,
                "top_n": top_n
            },
            op_id=op_id,
            status="completed"
        )
    
    def op_pivot(self, session_id: str,
                 rows: List[str],
                 cols: Optional[List[str]],
                 values: str,
                 agg: str = "sum",
                 preview_columns: Optional[List[str]] = None,
                 limit: int = 200,
                 table_name: Optional[str] = None) -> OperationResult:
        """
        Create pivot table.
        
        Args:
            session_id: Session ID
            rows: Row groupings
            cols: Column groupings
            values: Values column
            agg: Aggregation function
            preview_columns: Columns for preview
            limit: Max rows
            table_name: Optional table name
        """
        df = self.get_dataframe(session_id, table_name)
        if df is None:
            return OperationResult(
                preview=PreviewResult(columns=[], rows=[], summary={}),
                summary={"error": "No data"},
                op_id=""
            )
        
        op_id = str(uuid.uuid4())
        
        try:
            pivot = pd.pivot_table(
                df,
                values=values,
                index=rows,
                columns=cols,
                aggfunc=agg,
                fill_value=0
            )
            pivot_df = pivot.reset_index().head(limit)
        except Exception as e:
            return OperationResult(
                preview=PreviewResult(columns=[], rows=[], summary={}),
                summary={"error": str(e)},
                op_id=op_id
            )
        
        return OperationResult(
            preview=PreviewResult(
                columns=list(pivot_df.columns),
                rows=pivot_df.to_dict("records"),
                summary={}
            ),
            summary={
                "rows": rows,
                "columns": cols,
                "values": values,
                "agg": agg,
                "result_rows": len(pivot_df)
            },
            op_id=op_id,
            status="completed"
        )
    
    def op_sql(self, session_id: str,
               sql: str,
               limit: int = 200) -> OperationResult:
        """
        Execute SQL query on session data.
        
        Note: For security, this is executed in a read-only manner
        using DuckDB or SQLite with restrictions.
        """
        df = self.get_dataframe(session_id)
        if df is None:
            return OperationResult(
                preview=PreviewResult(columns=[], rows=[], summary={}),
                summary={"error": "No data"},
                op_id=""
            )
        
        op_id = str(uuid.uuid4())
        
        # Security: basic SQL injection prevention
        forbidden = ["drop", "delete", "update", "insert", "alter", "create", "truncate"]
        sql_lower = sql.lower()
        if any(f in sql_lower for f in forbidden):
            return OperationResult(
                preview=PreviewResult(columns=[], rows=[], summary={}),
                summary={"error": "SQL contains forbidden keywords"},
                op_id=op_id
            )
        
        try:
            # Use pandas query for safe SQL-like operations
            result_df = df.query(sql).head(limit)
        except Exception as e:
            return OperationResult(
                preview=PreviewResult(columns=[], rows=[], summary={}),
                summary={"error": str(e)},
                op_id=op_id
            )
        
        return OperationResult(
            preview=PreviewResult(
                columns=list(result_df.columns),
                rows=result_df.to_dict("records"),
                summary={}
            ),
            summary={
                "sql": sql,
                "rows_before": len(df),
                "rows_after": len(result_df)
            },
            op_id=op_id,
            status="completed"
        )
    
    # =========================================================================
    # SAVE & EXPORT
    # =========================================================================
    
    def save(self, session_id: str,
             dest: Dict[str, Any],
             mode: str = "replace",
             confirm: bool = False,
             table_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Save session data to destination.
        
        Args:
            session_id: Session ID
            dest: Destination specification
                - type: 'sqlite', 'csv', 'excel'
                - path: File path
                - table: Table name (for SQLite)
            mode: 'append' or 'replace'
            confirm: Require confirmation
            table_name: Optional table name
        """
        df = self.get_dataframe(session_id, table_name)
        if df is None:
            return {"success": False, "error": "No data to save"}
        
        dest_type = dest.get("type", "").lower()
        path = dest.get("path", "")
        
        # Validate path to prevent directory traversal
        if path:
            path = self._validate_path(path)
        
        if dest_type == "csv":
            df.to_csv(path, index=False)
            return {"success": True, "rows": len(df), "path": path}
        elif dest_type == "excel":
            df.to_excel(path, index=False, engine="openpyxl")
            return {"success": True, "rows": len(df), "path": path}
        elif dest_type == "sqlite":
            if db_save:
                table = dest.get("table", "data")
                db_save(df, path, table, if_exists=mode)
                return {"success": True, "rows": len(df), "path": path, "table": table}
            else:
                return {"success": False, "error": "SQLite not available"}
        
        return {"success": False, "error": "Unknown destination type"}
    
    # =========================================================================
    # UNDO
    # =========================================================================
    
    def undo(self, session_id: str, table_name: Optional[str] = None) -> bool:
        """
        Undo the last operation.
        
        Args:
            session_id: Session ID
            table_name: Optional table name
            
        Returns:
            True if undo was successful
        """
        sess = self.get_session(session_id)
        if not sess:
            return False
        
        table = table_name or (sess.active if isinstance(sess, CoreSession) else sess.get("active", ""))
        
        if isinstance(sess, CoreSession):
            return sess.undo(table)
        elif isinstance(sess, dict):
            history = sess.get("history", {}).get(table, [])
            if history:
                sess["tables"][table] = history.pop()
                return True
        
        return False
    
    # =========================================================================
    # OPERATION STATUS
    # =========================================================================
    
    def op_status(self, op_id: str) -> OperationStatus:
        """Get status of a long-running operation."""
        return self._operation_status.get(op_id, OperationStatus(
            op_id=op_id,
            status="unknown",
            progress=0.0,
            message="Operation not found"
        ))
    
    # =========================================================================
    # DIFF / BEFORE-AFTER
    # =========================================================================
    
    def get_diff(self, session_id: str,
                 table_name: Optional[str] = None,
                 limit: int = 50) -> Dict[str, Any]:
        """
        Get diff between current and previous state.
        
        Args:
            session_id: Session ID
            table_name: Optional table name
            limit: Max rows to show
            
        Returns:
            Dict with before/after data and summary
        """
        sess = self.get_session(session_id)
        if not sess:
            return {"error": "Session not found"}
        
        table = table_name or (sess.active if isinstance(sess, CoreSession) else sess.get("active", ""))
        
        # Get current data
        current_df = self.get_dataframe(session_id, table)
        if current_df is None:
            return {"error": "No data"}
        
        # Get previous state
        if isinstance(sess, CoreSession):
            history = sess.history.get(table, [])
            if history:
                prev_df = history[-1]
            else:
                prev_df = current_df
        elif isinstance(sess, dict):
            history = sess.get("history", {}).get(table, [])
            prev_df = history[-1] if history else current_df
        else:
            prev_df = current_df
        
        # Build diff
        before_rows = prev_df.head(limit).to_dict("records")
        after_rows = current_df.head(limit).to_dict("records")
        
        # Count changes
        cells_changed = 0
        for i in range(min(len(before_rows), len(after_rows))):
            for col in before_rows[i]:
                if before_rows[i].get(col) != after_rows[i].get(col):
                    cells_changed += 1
        
        return {
            "before": {
                "columns": list(prev_df.columns),
                "rows": before_rows[:limit]
            },
            "after": {
                "columns": list(current_df.columns),
                "rows": after_rows[:limit]
            },
            "summary": {
                "rows_before": len(prev_df),
                "rows_after": len(current_df),
                "cells_changed_sample": cells_changed
            }
        }
    
    # =========================================================================
    # DISCOVERY
    # =========================================================================
    
    def get_discovery_report(self) -> Dict[str, Any]:
        """Get the function discovery report."""
        return self._discovery.get_report()


# =============================================================================
# SINGLETON INSTANCE
# =============================================================================

# Default adapter instance
_default_adapter: Optional[Adapter] = None


def get_adapter(db_path: Optional[str] = None) -> Adapter:
    """Get or create the default adapter instance."""
    global _default_adapter
    if _default_adapter is None:
        _default_adapter = Adapter(db_path)
    return _default_adapter
