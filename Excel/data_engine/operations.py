"""
Core data operations - all the interactive transformations.

This module re-exports all operations from the modular sub-package.
For new code, prefer importing directly from data_engine.operations:
    from data_engine.operations import op_filter, op_sort, etc.

Or use the package-level import:
    from data_engine import operations as ops
    ops.op_filter(sess)
"""

# Re-export all operations from the modular sub-package
# This maintains backward compatibility for existing code

from data_engine.operations.filter import (
    apply_single_condition,
    op_multi_filter,
    op_filter_by_color,
    op_filter,
)
from data_engine.operations.find_replace import (
    op_find_replace,
)
from data_engine.operations.view import (
    op_focus_view,
    op_pin_column,
    op_edit_row,
    op_preview,
    op_search,
    op_stats,
)
from data_engine.operations.transform import (
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
from data_engine.operations.table_manager import (
    op_table_manager,
    op_switch_table,
)
from data_engine.operations.session_io import (
    op_save,
    op_export,
)
from data_engine.operations.analysis import (
    detect_column_type,
    op_profile,
    op_outlier_detection,
    op_correlation_matrix,
    op_crosstab,
    op_segment_column,
    op_time_series,
    op_string_analysis,
)
from data_engine.operations.smart_fix import (
    scan_column_issues,
    show_load_report,
    op_smart_fix,
)

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
]
