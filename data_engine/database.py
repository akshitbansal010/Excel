"""
Database operations - SQLite wrapper functions.
"""

import sqlite3
import pandas as pd
import os
import re

from rich.console import Console

console = Console()

# ╔══════════════════════════════════════════════════════════════════╗
# ║  VALIDATION                                                        ║
# ╚══════════════════════════════════════════════════════════════════╝

def validate_table_name(name: str) -> bool:
    """
    Validate table name to prevent SQL injection.
    
    Args:
        name: Table name to validate
        
    Returns:
        True if valid SQL identifier, False otherwise
    """
    if not name:
        return False
    return re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name) is not None


def db_tables(db_path: str) -> list:
    """
    Get list of table names in a SQLite database.
    
    Args:
        db_path: Path to SQLite database file
        
    Returns:
        List of table names
    """
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
    return [r[0] for r in rows]


def db_load(db_path: str, table: str) -> pd.DataFrame:
    """
    Load a table from SQLite database into a DataFrame.
    
    Args:
        db_path: Path to SQLite database file
        table: Name of the table to load
        
    Returns:
        DataFrame containing the table data
    """
    if not validate_table_name(table):
        raise ValueError(f"Invalid table name: {table}")
    
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(f'SELECT * FROM "{table}"', conn)


def db_save(df: pd.DataFrame, db_path: str, table: str, 
            if_exists: str = "replace") -> None:
    """
    Save a DataFrame to a SQLite database table.
    
    Args:
        df: DataFrame to save
        db_path: Path to SQLite database file
        table: Name of the table to save to
        if_exists: How to handle existing table ('replace', 'append', 'fail')
    """
    if not validate_table_name(table):
        raise ValueError(f"Invalid table name: {table}")
    
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table, conn, if_exists=if_exists, index=False)
    console.print(
        f"[green]✔ Saved [bold]{len(df):,} rows[/bold] → "
        f"[cyan]{db_path}[/cyan] / [cyan]{table}[/cyan][/green]"
    )


def db_table_exists(db_path: str, table: str) -> bool:
    """
    Check if a table exists in the database.
    
    Args:
        db_path: Path to SQLite database file
        table: Table name to check
        
    Returns:
        True if table exists
    """
    return table in db_tables(db_path)


def db_get_schema(db_path: str, table: str) -> list:
    """
    Get the schema (column names and types) for a table.
    
    Args:
        db_path: Path to SQLite database file
        table: Table name
        
    Returns:
        List of tuples (column_name, data_type)
    """
    if not validate_table_name(table):
        raise ValueError(f"Invalid table name: {table}")
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(f'PRAGMA table_info("{table}")')
        return [(row[1], row[2]) for row in cursor.fetchall()]


def db_row_count(db_path: str, table: str) -> int:
    """
    Get the number of rows in a table.
    
    Args:
        db_path: Path to SQLite database file
        table: Table name
        
    Returns:
        Number of rows
    """
    if not validate_table_name(table):
        raise ValueError(f"Invalid table name: {table}")
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(f'SELECT COUNT(*) FROM "{table}"')
        return cursor.fetchone()[0]
