"""
excelpy - Interactive CLI and library for treating CSV/DB tables like Excel.

Primary behavior:
- Load CSV (auto-detect delimiter) or DB table (sqlite or via SQLAlchemy)
- Always prompt column names explicitly before any column-based operation
- Support fuzzy/partial column entry using rapidfuzz
- Accept Excel-style column references (A, B, AA) and map to column names
- 3-step conditions: Column → Operator → Value
- After any operation, ask which columns to display
- Export to CSV, SQLite, Postgres

Author: excelpy team
Version: 1.0.0
"""

__version__ = "1.0.0"
__author__ = "excelpy team"

# Core exports
from excelpy.core import (
    load_table,
    fuzzy_select_column,
    ask_condition_and_filter,
    sort_table,
    rank_table,
    aggregate_table,
    save_table,
    show_preview,
    ask_columns_to_display,
)

# Helper exports
from excelpy.helpers import (
    col_letter,
    parse_col_letter,
    build_col_map,
    resolve_column,
    fuzzy_match,
    parse_value,
    parse_operator,
)

# Engine exports
from excelpy.engine import get_engine, is_polars_available

__all__ = [
    # Core
    "load_table",
    "fuzzy_select_column", 
    "ask_condition_and_filter",
    "sort_table",
    "rank_table",
    "aggregate_table",
    "save_table",
    "show_preview",
    "ask_columns_to_display",
    # Helpers
    "col_letter",
    "parse_col_letter",
    "build_col_map",
    "resolve_column",
    "fuzzy_match",
    "parse_value",
    "parse_operator",
    # Engine
    "get_engine",
    "is_polars_available",
    # Version
    "__version__",
]
