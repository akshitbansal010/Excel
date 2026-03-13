"""
Operations sub-package — all interactive data transformations.

Re-exports every public operation so callers can still do:
    from data_engine.operations import op_filter, op_sort, ...
    from data_engine import operations as ops
"""

# ── Smart Fix (1.1) ──────────────────────────────────────────────────────────
from .smart_fix import scan_column_issues, show_load_report, op_smart_fix

# ── Filtering (1.2) ──────────────────────────────────────────────────────────
from .filter import (
    apply_single_condition,
    op_multi_filter,
    op_filter_by_color,
    op_filter,
)

# ── Find & Replace (1.3) ─────────────────────────────────────────────────────
from .find_replace import op_find_replace

# ── View / Inspect ───────────────────────────────────────────────────────────
from .view import (
    op_focus_view,
    op_pin_column,
    op_edit_row,
    op_preview,
    op_search,
    op_stats,
)

# ── Core Transforms ──────────────────────────────────────────────────────────
from .transform import (
    op_add_column,
    op_aggregate,
    op_sort,
    op_handle_nulls,
    op_rename_drop,
    op_dedupe,
    op_pivot,
    op_change_type,
    op_join,
    op_calculated_columns,
)

# ── Table Manager ────────────────────────────────────────────────────────────
from .table_manager import op_table_manager, op_switch_table

# ── Session I/O (Save / Export) ─────────────────────────────────────────────
from .session_io import op_save, op_export

# ── Phase 2: Analysis ────────────────────────────────────────────────────────
from .analysis import (
    detect_column_type,
    op_profile,
    op_outlier_detection,
    op_correlation_matrix,
    op_crosstab,
    op_segment_column,
    op_time_series,
    op_string_analysis,
)

# ── Ranking ─────────────────────────────────────────────────────────────────
from .ranking import op_rank

__all__ = [
    # Smart Fix
    "scan_column_issues", "show_load_report", "op_smart_fix",
    # Filter
    "apply_single_condition", "op_multi_filter", "op_filter_by_color", "op_filter",
    # Find & Replace
    "op_find_replace",
    # View
    "op_focus_view", "op_pin_column", "op_edit_row", "op_preview",
    "op_search", "op_stats",
    # Transform
    "op_add_column", "op_aggregate", "op_sort", "op_handle_nulls",
    "op_rename_drop", "op_dedupe", "op_pivot", "op_change_type",
    "op_join", "op_calculated_columns",
    # Table Manager
    "op_table_manager", "op_switch_table",
    # Session I/O
    "op_save", "op_export",
    # Analysis
    "detect_column_type", "op_profile", "op_outlier_detection",
    "op_correlation_matrix", "op_crosstab", "op_segment_column",
    "op_time_series", "op_string_analysis",
    # Ranking
    "op_rank",
]
