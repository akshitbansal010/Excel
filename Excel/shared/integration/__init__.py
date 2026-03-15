"""
DataEngine Pro - Integration Layer
===================================

This package provides the integration layer that connects the existing
data_engine core modules to the Streamlit UI and optional FastAPI backend.

Modules:
    - adapter: Main adapter with canonical API
    - session: Session management with undo snapshots
    - column_resolver: Fuzzy column name resolution
    - inspect_core: Diagnostic script for core module discovery

Usage:
    from integration import Adapter, get_adapter
    
    adapter = get_adapter()
    session = adapter.create_session()
    adapter.load_table(session.id, {"type": "csv", "path": "data.csv"})
"""

from .adapter import Adapter, get_adapter, SessionMeta, PreviewResult, OperationResult, OperationStatus
from .session import SessionManager, get_session_manager, SessionInfo, Snapshot
from .column_resolver import ColumnResolver, resolve_column, resolve_columns, suggest_columns

__all__ = [
    # Adapter
    "Adapter",
    "get_adapter",
    "SessionMeta",
    "PreviewResult", 
    "OperationResult",
    "OperationStatus",
    
    # Session
    "SessionManager",
    "get_session_manager",
    "SessionInfo",
    "Snapshot",
    
    # Column Resolver
    "ColumnResolver",
    "resolve_column",
    "resolve_columns",
    "suggest_columns",
]

__version__ = "1.0.0"
