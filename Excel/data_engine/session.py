"""
Session management - holds all dataframes, undo history, and active table pointer.
"""

import pandas as pd
from typing import Optional
from .config import MAX_UNDO_HISTORY


class Session:
    """
    Manages the state of a DataEngine session.
    
    Attributes:
        db_path: Path to the SQLite database
        tables: Dictionary mapping table names to DataFrames
        history: Dictionary mapping table names to lists of previous states (for undo)
        active: Name of the currently active table
    """
    
    def __init__(self, db_path: str):
        """
        Initialize a new Session.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path: str = db_path
        self.tables: dict[str, pd.DataFrame] = {}
        self.history: dict[str, list[pd.DataFrame]] = {}
        self.active: str = ""
    
    def add(self, name: str, df: pd.DataFrame) -> None:
        """
        Add a table to the session.
        
        Args:
            name: Name to assign to the table
            df: DataFrame to store
        """
        if name in self.tables:
            # Preserve existing history when replacing table content
            if name not in self.history:
                self.history[name] = []
            else:
                self.history[name].append(self.tables[name].copy())
        else:
            self.history[name] = []

        self.tables[name] = df
        self.active = name
    
    def push_undo(self, name: Optional[str] = None) -> None:
        """
        Save current state of a table to history for undo functionality.
        
        Args:
            name: Table name to save history for. Defaults to active table.
        """
        n = name or self.active
        if n in self.tables:
            self.history[n].append(self.tables[n].copy())
            if len(self.history[n]) > MAX_UNDO_HISTORY:
                self.history[n].pop(0)
    
    def undo(self, name: Optional[str] = None) -> bool:
        """
        Restore the previous state of a table.
        
        Args:
            name: Table name to restore. Defaults to active table.
            
        Returns:
            True if undo was successful, False if no history available.
        """
        n = name or self.active
        if self.history.get(n):
            self.tables[n] = self.history[n].pop()
            return True
        return False
    
    @property
    def df(self) -> pd.DataFrame:
        """
        Get the currently active DataFrame.
        
        Returns:
            The DataFrame for the active table.
            
        Raises:
            RuntimeError: If no tables are loaded in the session.
        """
        if not self.tables:
            raise RuntimeError("No tables loaded in session")
        if self.active not in self.tables:
            raise RuntimeError("Active table not found in session")
        return self.tables[self.active]
    
    @df.setter
    def df(self, value: pd.DataFrame) -> None:
        """
        Set the DataFrame for the active table.
        
        Args:
            value: New DataFrame to assign to active table.
            
        Raises:
            RuntimeError: If no tables are loaded in the session.
        """
        if not self.tables:
            raise RuntimeError("No tables loaded in session")
        if self.active not in self.tables:
            raise RuntimeError("Active table not found in session")
        self.tables[self.active] = value
    
    def list_tables(self) -> list[str]:
        """
        Get list of all table names in the session.
        
        Returns:
            List of table names.
        """
        return list(self.tables.keys())
    
    def has_undo(self, name: Optional[str] = None) -> bool:
        """
        Check if undo is available for a table.
        
        Args:
            name: Table name to check. Defaults to active table.
            
        Returns:
            True if undo history exists.
        """
        n = name or self.active
        return bool(self.history.get(n))
    
    def table_info(self, name: str) -> dict:
        """
        Get information about a table.
        
        Args:
            name: Table name.
            
        Returns:
            Dictionary with row count, column count, and column names.
        """
        if name not in self.tables:
            return {}
        df = self.tables[name]
        return {
            "rows": len(df),
            "cols": len(df.columns),
            "columns": list(df.columns)
        }
