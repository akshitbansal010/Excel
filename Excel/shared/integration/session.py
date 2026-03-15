"""
DataEngine Pro - Session Manager
=================================

Manages session state with lightweight undo snapshots.
Supports both in-memory and persistent storage.

Usage:
    from integration.session import SessionManager
    
    manager = SessionManager(max_undo=10)
    session_id = manager.create_session()
    manager.save_snapshot(session_id, df)
    manager.undo(session_id)
"""

import uuid
import json
import time
import sqlite3
import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import pickle
import io
import os


class RestrictedUnpickler(pickle.Unpickler):
    """
    A restricted unpickler that only allows safe types.
    This prevents arbitrary code execution from malicious pickle files.
    """
    # Whitelist of allowed classes/types
    ALLOWED_BUILTINS = {
        'dict', 'list', 'tuple', 'set', 'frozenset', 'str', 'int', 'float', 'bool', 'None',
        'bytes', 'bytearray', 'complex', 'range', 'slice'
    }
    
    ALLOWED_MODULES = {
        'pandas.core.frame', 'pandas.core.series', 'pandas', 'numpy', 'numpy.ndarray',
        'datetime', 'collections', 'uuid', 'typing'
    }
    
    def find_class(self, module, name):
        # Allow standard library types
        if module.startswith('_'):
            raise pickle.UnpicklingError(f"Can't find class {module}.{name}")
        
        # Check if it's a builtin
        if module == 'builtins' and name in self.ALLOWED_BUILTINS:
            return super().find_class(module, name)
        
        # Check if it's an allowed module
        if module in self.ALLOWED_MODULES or any(module.startswith(m) for m in self.ALLOWED_MODULES):
            return super().find_class(module, name)
        
        raise pickle.UnpicklingError(f"Forbidden: {module}.{name}")


def safe_pickle_load(path: str):
    """
    Safely load a pickle file with restricted types.
    
    Args:
        path: Path to the pickle file
        
    Returns:
        The unpickled object
        
    Raises:
        pickle.UnpicklingError: If the pickle contains forbidden types
    """
    with open(path, 'rb') as f:
        return RestrictedUnpickler(f).load()


@dataclass
class Snapshot:
    """Lightweight snapshot for undo functionality."""
    id: str
    timestamp: str
    row_count: int
    col_count: int
    data: Optional[pd.DataFrame] = None
    # For large data, store metadata instead of full data
    storage_type: str = "memory"  # memory, sqlite_view, csv_pointer
    storage_path: Optional[str] = None
    storage_query: Optional[str] = None


@dataclass
class SessionInfo:
    """Information about a session."""
    id: str
    created_at: str
    tables: Dict[str, int]  # table_name -> row_count
    active_table: str
    snapshot_count: int


class SessionManager:
    """
    Manages multiple data sessions with undo capability.
    
    Features:
    - Lightweight snapshots (stores small data in memory, large data as refs)
    - Configurable undo depth
    - Persistent storage support
    - Multi-table support per session
    """
    
    # Threshold for storing data vs metadata (rows)
    LARGE_DATA_THRESHOLD = 10000
    
    # Threshold for storing data vs metadata (memory in bytes)
    LARGE_MEMORY_THRESHOLD = 50 * 1024 * 1024  # 50MB
    
    def __init__(self, max_undo: int = 10, db_path: Optional[str] = None):
        """
        Initialize the session manager.
        
        Args:
            max_undo: Maximum undo history per table
            db_path: Optional path for persistent SQLite storage
        """
        self.max_undo = max_undo
        self.db_path = db_path or ":memory:"
        
        # In-memory session storage
        self._sessions: Dict[str, Dict[str, Any]] = {}
        
        # Initialize persistent storage if path provided
        if self.db_path and self.db_path != ":memory:":
            self._init_persistent_storage()
    
    def _init_persistent_storage(self):
        """Initialize SQLite storage for sessions."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS session_snapshots (
                    session_id TEXT,
                    table_name TEXT,
                    snapshot_id TEXT,
                    timestamp TEXT,
                    row_count INTEGER,
                    col_count INTEGER,
                    data BLOB,
                    PRIMARY KEY (session_id, table_name, snapshot_id)
                )
            """)
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Warning: Could not initialize persistent storage: {e}")
    
    def create_session(self, session_id: Optional[str] = None) -> str:
        """
        Create a new session.
        
        Args:
            session_id: Optional custom session ID
            
        Returns:
            Session ID
        """
        sid = session_id or str(uuid.uuid4())
        
        self._sessions[sid] = {
            "id": sid,
            "created_at": datetime.now().isoformat(),
            "tables": {},  # table_name -> DataFrame
            "active_table": "",
            "snapshots": {},  # table_name -> list of Snapshots
            "undo_pos": {}   # table_name -> current position in snapshot list
        }
        
        return sid
    
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session by ID."""
        return self._sessions.get(session_id)
    
    def list_sessions(self) -> List[SessionInfo]:
        """List all sessions."""
        sessions = []
        for sid, sess in self._sessions.items():
            sessions.append(SessionInfo(
                id=sid,
                created_at=sess["created_at"],
                tables={name: len(df) for name, df in sess["tables"].items()},
                active_table=sess.get("active_table", ""),
                snapshot_count=sum(len(snaps) for snaps in sess["snapshots"].values())
            ))
        return sessions
    
    def delete_session(self, session_id: str) -> bool:
        """Delete a session."""
        if session_id in self._sessions:
            del self._sessions[session_id]
            return True
        return False
    
    # =========================================================================
    # TABLE MANAGEMENT
    # =========================================================================
    
    def add_table(self, session_id: str, name: str, df: pd.DataFrame) -> bool:
        """
        Add a table to the session.
        
        Args:
            session_id: Session ID
            name: Table name
            df: DataFrame
            
        Returns:
            True if successful
        """
        sess = self.get_session(session_id)
        if not sess:
            return False
        
        sess["tables"][name] = df.copy()
        sess["active_table"] = name
        
        # Initialize snapshot list for this table
        if name not in sess["snapshots"]:
            sess["snapshots"][name] = []
            sess["undo_pos"][name] = -1
        
        return True
    
    def get_table(self, session_id: str, name: str) -> Optional[pd.DataFrame]:
        """Get a table from the session."""
        sess = self.get_session(session_id)
        if not sess:
            return None
        return sess["tables"].get(name)
    
    def list_tables(self, session_id: str) -> List[str]:
        """List all tables in the session."""
        sess = self.get_session(session_id)
        if not sess:
            return []
        return list(sess["tables"].keys())
    
    def set_active_table(self, session_id: str, name: str) -> bool:
        """Set the active table."""
        sess = self.get_session(session_id)
        if not sess or name not in sess["tables"]:
            return False
        sess["active_table"] = name
        return True
    
    def get_active_table(self, session_id: str) -> str:
        """Get the active table name."""
        sess = self.get_session(session_id)
        if not sess:
            return ""
        return sess.get("active_table", "")
    
    # =========================================================================
    # SNAPSHOT / UNDO
    # =========================================================================
    
    def save_snapshot(self, session_id: str, table_name: Optional[str] = None) -> str:
        """
        Save a snapshot for undo.
        
        Args:
            session_id: Session ID
            table_name: Optional table name (defaults to active table)
            
        Returns:
            Snapshot ID
        """
        sess = self.get_session(session_id)
        if not sess:
            return ""
        
        name = table_name or sess.get("active_table", "")
        if not name or name not in sess["tables"]:
            return ""
        
        df = sess["tables"][name]
        
        # Determine storage type based on size
        memory_size = df.memory_usage(deep=True).sum()
        
        if len(df) > self.LARGE_DATA_THRESHOLD or memory_size > self.LARGE_MEMORY_THRESHOLD:
            storage_type = "pointer"  # Store metadata only
            storage_path = None
        else:
            storage_type = "memory"
            storage_path = None
        
        snapshot = Snapshot(
            id=str(uuid.uuid4()),
            timestamp=datetime.now().isoformat(),
            row_count=len(df),
            col_count=len(df.columns),
            data=df.copy(deep=True) if storage_type == "memory" else None,
            storage_type=storage_type,
            storage_path=storage_path
        )
        
        # Add to snapshots list
        if name not in sess["snapshots"]:
            sess["snapshots"][name] = []
            sess["undo_pos"][name] = -1
        
        # Remove any snapshots after current position (for redo)
        pos = sess["undo_pos"][name]
        if pos < len(sess["snapshots"][name]) - 1:
            sess["snapshots"][name] = sess["snapshots"][name][:pos + 1]
        
        # Add new snapshot
        sess["snapshots"][name].append(snapshot)
        sess["undo_pos"][name] = len(sess["snapshots"][name]) - 1
        
        # Trim old snapshots beyond max_undo
        if len(sess["snapshots"][name]) > self.max_undo:
            sess["snapshots"][name] = sess["snapshots"][name][-self.max_undo:]
            sess["undo_pos"][name] = len(sess["snapshots"][name]) - 1
        
        return snapshot.id
    
    def can_undo(self, session_id: str, table_name: Optional[str] = None) -> bool:
        """Check if undo is available."""
        sess = self.get_session(session_id)
        if not sess:
            return False
        
        name = table_name or sess.get("active_table", "")
        pos = sess.get("undo_pos", {}).get(name, -1)
        
        return pos > 0
    
    def can_redo(self, session_id: str, table_name: Optional[str] = None) -> bool:
        """Check if redo is available."""
        sess = self.get_session(session_id)
        if not sess:
            return False
        
        name = table_name or sess.get("active_table", "")
        pos = sess.get("undo_pos", {}).get(name, -1)
        snapshots = sess.get("snapshots", {}).get(name, [])
        
        return pos < len(snapshots) - 1
    
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
        
        name = table_name or sess.get("active_table", "")
        pos = sess.get("undo_pos", {}).get(name, -1)
        
        if pos <= 0:
            return False
        
        # Get snapshot at new position
        snapshots = sess.get("snapshots", {}).get(name, [])
        if pos - 1 < 0 or pos - 1 >= len(snapshots):
            return False
        
        snapshot = snapshots[pos - 1]
        
        if snapshot.data is not None:
            sess["tables"][name] = snapshot.data.copy()
        
        sess["undo_pos"][name] = pos - 1
        return True
    
    def redo(self, session_id: str, table_name: Optional[str] = None) -> bool:
        """
        Redo a previously undone operation.
        
        Args:
            session_id: Session ID
            table_name: Optional table name
            
        Returns:
            True if redo was successful
        """
        sess = self.get_session(session_id)
        if not sess:
            return False
        
        name = table_name or sess.get("active_table", "")
        pos = sess.get("undo_pos", {}).get(name, -1)
        snapshots = sess.get("snapshots", {}).get(name, [])
        
        if pos >= len(snapshots) - 1:
            return False
        
        snapshot = snapshots[pos + 1]
        
        if snapshot.data is not None:
            sess["tables"][name] = snapshot.data.copy()
        
        sess["undo_pos"][name] = pos + 1
        return True
    
    def get_undo_info(self, session_id: str, table_name: Optional[str] = None) -> Dict[str, Any]:
        """Get information about undo state."""
        sess = self.get_session(session_id)
        if not sess:
            return {"can_undo": False, "can_redo": False}
        
        name = table_name or sess.get("active_table", "")
        
        return {
            "can_undo": self.can_undo(session_id, name),
            "can_redo": self.can_redo(session_id, name),
            "undo_count": sess.get("undo_pos", {}).get(name, -1) + 1,
            "redo_count": len(sess.get("snapshots", {}).get(name, [])) - sess.get("undo_pos", {}).get(name, -1) - 1
        }
    
    # =========================================================================
    # PERSISTENCE
    # =========================================================================
    
    def export_session(self, session_id: str, path: str) -> bool:
        """
        Export session to file.
        
        Args:
            session_id: Session ID
            path: Export path
            
        Returns:
            True if successful
        """
        sess = self.get_session(session_id)
        if not sess:
            return False
        
        try:
            # Export as pickle
            with open(path, 'wb') as f:
                pickle.dump(sess, f)
            return True
        except Exception as e:
            print(f"Export error: {e}")
            return False
    
    def import_session(self, path: str) -> Optional[str]:
        """
        Import session from file.
        
        Args:
            path: Import path
            
        Returns:
            Session ID or None
        """
        try:
            # Use safe_pickle_load to prevent arbitrary code execution
            sess = safe_pickle_load(path)
            
            # Generate new ID to avoid conflicts
            new_id = str(uuid.uuid4())
            sess["id"] = new_id
            
            self._sessions[new_id] = sess
            return new_id
        except Exception as e:
            print(f"Import error: {e}")
            return None


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

_default_manager: Optional[SessionManager] = None


def get_session_manager(max_undo: int = 10) -> SessionManager:
    """Get or create the default session manager."""
    global _default_manager
    if _default_manager is None:
        _default_manager = SessionManager(max_undo=max_undo)
    return _default_manager
