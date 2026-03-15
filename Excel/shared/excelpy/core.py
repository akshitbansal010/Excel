"""
Core operations module for excelpy.
Provides the main interactive functions for data manipulation.
"""

import os
import time
from typing import Optional, List, Any, Union, Callable
from dataclasses import dataclass

from rich.console import Console
from rich.prompt import Prompt, Confirm
from rich.rule import Rule
from rich.table import Table
from rich import box

from excelpy.engine import (
    DataFrameWrapper, 
    read_csv, 
    read_sqlite,
    is_polars_available,
    is_sqlalchemy_available,
)
from excelpy.helpers import (
    col_letter,
    resolve_column,
    fuzzy_select_column,
    fuzzy_select_value,
    parse_value,
    parse_operator,
    get_operator_choices,
    show_operator_help,
    format_value,
    format_number,
    build_col_map,
)

console = Console()


def _show_operation_summary(result: 'OperationResult') -> None:
    """Display summary of operation result."""
    console.print(f"\n[green]✓ {result.operation.title()} complete[/green]")
    console.print(f"[dim]  Rows: {format_number(result.rows_before)} → {format_number(result.rows_after)}  "
                  f"| Columns: {result.cols_before} → {result.cols_after}[/dim]")
    console.print(f"[dim]  Time: {result.time_taken:.2f}s[/dim]")


# ╔══════════════════════════════════════════════════════════════════╗
# ║  RESULT TRACKING                                                  ║
# ╚══════════════════════════════════════════════════════════════════╝

@dataclass
class OperationResult:
    """Result of a data operation with summary info."""
    df: DataFrameWrapper
    rows_before: int
    rows_after: int
    cols_before: int
    cols_after: int
    time_taken: float
    operation: str
    columns_displayed: Optional[List[str]] = None


# ╔══════════════════════════════════════════════════════════════════╗
# ║  LOAD TABLE                                                       ║
# ╚══════════════════════════════════════════════════════════════════╝

def load_table(
    path: str,
    force_engine: Optional[str] = None,
    **kwargs
) -> DataFrameWrapper:
    """
    Load a CSV file or database table.
    
    Args:
        path: Path to CSV file or SQLite database
        force_engine: Force "polars" or "pandas"
        **kwargs: Additional arguments for CSV reader
    
    Returns:
        DataFrameWrapper
    """
    start = time.time()
    
    # Detect file type
    if path.endswith(('.db', '.sqlite', '.sqlite3')):
        # SQLite - need table name
        table_name = kwargs.pop('table_name', None)
        if not table_name:
            console.print("[yellow]Please specify table_name for SQLite database.[/yellow]")
            table_name = Prompt.ask("Table name").strip()
            if not table_name:
                raise ValueError("Table name cannot be empty")
        df = read_sqlite(path, table_name, force_engine)
    else:
        # CSV file
        df = read_csv(path, force_engine, **kwargs)
    
    elapsed = time.time() - start
    _show_load_summary(df, path, elapsed)
    
    return df


def _show_load_summary(df: DataFrameWrapper, path: str, elapsed: float) -> None:
    """Show summary after loading a table."""
    size_mb = os.path.getsize(path) / (1024 * 1024) if os.path.exists(path) else 0
    
    console.print(f"\n[green]✓ Loaded[/green] {os.path.basename(path)}")
    console.print(f"[dim]  Rows: {format_number(len(df))} | Columns: {len(df.columns)} | Engine: {df.engine}[/dim]")
    if size_mb > 0:
        console.print(f"[dim]  Size: {size_mb:.1f} MB | Time: {elapsed:.2f}s[/dim]")


# ╔══════════════════════════════════════════════════════════════════╗
# ║  DISPLAY / PREVIEW                                               ║
# ╚══════════════════════════════════════════════════════════════════╝

def show_preview(
    df: DataFrameWrapper,
    n: int = 10,
    title: str = "Preview",
    columns: Optional[List[str]] = None,
    show_excel_letters: bool = True
) -> None:
    """
    Display a preview of the DataFrame.
    
    Args:
        df: DataFrameWrapper to preview
        n: Number of rows to show
        title: Title for the preview table
        columns: Specific columns to show (None = all)
        show_excel_letters: Show Excel-style column letters
    """
    if df.shape[0] == 0:
        console.print("[red]⚠  No rows to display.[/red]")
        return
    
    # Select columns
    disp_df = df
    if columns:
        disp_df = df.select(columns)
    
    total_rows = len(df)
    showing = min(n, total_rows)
    
    t = Table(
        title=f"{title} ({showing} of {format_number(total_rows)} rows)",
        box=box.SIMPLE_HEAVY,
        show_lines=False,
        title_style="bold"
    )
    t.add_column("#", style="dim", width=5, justify="right")
    
    # Add columns with Excel-style letters
    col_list = disp_df.columns
    for i, col in enumerate(col_list):
        if show_excel_letters:
            # Find original index
            orig_idx = df.columns.index(col)
            header = f"[yellow]{col_letter(orig_idx)}[/yellow]\n{col}"
        else:
            header = col
        t.add_column(
            header,
            overflow="fold",
            min_width=10,
            max_width=20
        )
    
    # Add rows
    for row_i, row in enumerate(disp_df.head(n).iterrows()):
        if isinstance(row, tuple):
            _, row_data = row
        else:
            row_data = row
        if isinstance(row_data, dict):
            t.add_row(str(row_i + 1), *[format_value(row_data.get(c, "")) for c in col_list])
        else:
            t.add_row(str(row_i + 1), *[format_value(v) for v in row_data])
    
    console.print(t)


def ask_columns_to_display(
    df: DataFrameWrapper,
    default_columns: Optional[List[str]] = None,
    allow_all: bool = True
) -> Optional[List[str]]:
    """
    Ask user which columns to display.
    
    Args:
        df: DataFrameWrapper
        default_columns: Default column selection
        allow_all: Allow selecting all columns
    
    Returns:
        List of column names, or None to cancel
    """
    console.print("\n[bold]Select columns to display:[/bold]")
    
    # Show available columns
    t = Table(box=box.SIMPLE, show_header=False)
    t.add_column("#", style="yellow", width=4, justify="center")
    t.add_column("Column", style="white")
    t.add_column("Excel", style="dim", width=5)
    
    for i, col in enumerate(df.columns):
        t.add_row(str(i + 1), col, col_letter(i))
    console.print(t)
    
    # Prompt
    if default_columns:
        default_str = ",".join(default_columns[:5])
    elif allow_all:
        default_str = "all"
    else:
        default_str = ""
    
    choice = Prompt.ask(
        "Columns (number, name, or 'all')",
        default=default_str
    ).strip()
    
    if choice.upper() in ("ALL", "A"):
        return list(df.columns)
    
    if not choice:
        return default_columns if default_columns else list(df.columns)
    
    # Try to resolve
    resolved = resolve_column(choice, df.columns)
    if resolved:
        return [resolved]
    
    # Try comma-separated
    parts = [p.strip() for p in choice.split(",")]
    result = []
    for p in parts:
        r = resolve_column(p, df.columns)
        if r:
            result.append(r)
    
    return result if result else None


# ╔══════════════════════════════════════════════════════════════════╗
# ║  FILTER                                                           ║
# ╚══════════════════════════════════════════════════════════════════╝

def ask_condition_and_filter(df: DataFrameWrapper) -> OperationResult:
    """
    Interactive 3-step filtering: Column → Operator → Value.
    
    Args:
        df: DataFrameWrapper to filter
    
    Returns:
        OperationResult with filtered DataFrame
    """
    start = time.time()
    rows_before = len(df)
    cols_before = len(df.columns)
    
    console.print(Rule("[bold]🔍 Filter Rows[/bold]"))
    
    # Step 1: Select column
    console.print("\n[bold cyan]Step 1:[/bold cyan] Select column to filter on")
    col = fuzzy_select_column(
        df.columns,
        prompt_text="Column",
        default=df.columns[0] if df.columns else None
    )
    
    if not col:
        console.print("[red]No column selected.[/red]")
        return OperationResult(
            df=df, rows_before=rows_before, rows_after=rows_before,
            cols_before=cols_before, cols_after=cols_before,
            time_taken=time.time() - start, operation="filter"
        )
    
    # Show unique values in the column
    console.print(f"\n[dim]Values in '{col}':[/dim]")
    unique_vals = df[col].unique()
    if hasattr(unique_vals, 'to_list'):
        unique_list = unique_vals.to_list()
    elif hasattr(unique_vals, 'to_dict'):
        unique_list = list(unique_vals.to_dict().values())
    else:
        unique_list = list(unique_vals)
    if len(unique_list) <= 10:
        for v in unique_list[:10]:
            console.print(f"  {format_value(v)}")
    else:
        console.print(f"  [dim]({len(unique_list)} unique values)[/dim]")
    
    # Step 2: Select operator
    console.print("\n[bold cyan]Step 2:[/bold cyan] Select operator")
    show_operator_help()
    
    ops = get_operator_choices()
    console.print("\n[bold]Quick options:[/bold]")
    for choice, op, desc in ops[:7]:
        console.print(f"  [yellow]{choice}[/yellow]  {desc}")
    console.print("  [yellow]?[/yellow]  Show all operators")
    
    op_choice = Prompt.ask("Operator", default="1").strip()
    
    if op_choice == "?":
        show_operator_help()
        op_choice = Prompt.ask("Operator", default="1").strip()
    
    # Map choice to operator
    op = None
    for choice, operator, _ in ops:
        if choice == op_choice:
            op = operator
            break
    
    if not op:
        console.print("[red]Invalid operator.[/red]")
        return OperationResult(
            df=df, rows_before=rows_before, rows_after=rows_before,
            cols_before=cols_before, cols_after=cols_before,
            time_taken=time.time() - start, operation="filter"
        )
    
    console.print(f"[dim]Using operator: {op}[/dim]")
    
    # Step 3: Get value based on operator
    value = None
    if op == "IS_NULL":
        value = None
    elif op == "IS_ONE_OF":
        console.print("\n[bold cyan]Step 3:[/bold cyan] Enter values (comma-separated)")
        value_str = Prompt.ask("Values", default="").strip()
        value = [v.strip() for v in value_str.split(",")]
    elif op == "CONTAINS":
        console.print("\n[bold cyan]Step 3:[/bold cyan] Enter text to contain")
        value = Prompt.ask("Text", default="").strip()
    elif op == "STARTSWITH":
        console.print("\n[bold cyan]Step 3:[/bold cyan] Enter starting text")
        value = Prompt.ask("Starts with", default="").strip()
    elif op == "ENDSWITH":
        console.print("\n[bold cyan]Step 3:[/bold cyan] Enter ending text")
        value = Prompt.ask("Ends with", default="").strip()
    else:
        console.print("\n[bold cyan]Step 3:[/bold cyan] Enter value to compare")
        value_str = Prompt.ask("Value", default="").strip()
        value = parse_value(value_str)
    
    # Apply filter
    filtered_df = _apply_filter(df, col, op, value)
    
    rows_after = len(filtered_df)
    cols_after = len(filtered_df.columns)
    time_taken = time.time() - start
    
    # Show summary
    removed = rows_before - rows_after
    console.print(f"\n[green]✓ Filter applied![/green]")
    console.print(f"[bold]{format_number(rows_after)}[/bold] rows kept "
                  f"([yellow]{format_number(removed)}[/yellow] removed)")
    
    # Ask which columns to display
    display_cols = ask_columns_to_display(filtered_df, default_columns=[col])
    
    return OperationResult(
        df=filtered_df,
        rows_before=rows_before,
        rows_after=rows_after,
        cols_before=cols_before,
        cols_after=cols_after,
        time_taken=time_taken,
        operation="filter",
        columns_displayed=display_cols
    )


def _apply_filter(df: DataFrameWrapper, col: str, op: str, value: Any) -> DataFrameWrapper:
    """
    Apply a filter condition to a DataFrame.
    
    Args:
        df: DataFrameWrapper
        col: Column name
        op: Operator
        value: Value to compare
    
    Returns:
        Filtered DataFrameWrapper
    """
    engine = df.engine
    
    if engine == "polars":
        return _apply_filter_polars(df, col, op, value)
    else:
        return _apply_filter_pandas(df, col, op, value)


def _apply_filter_polars(df: DataFrameWrapper, col: str, op: str, value: Any) -> DataFrameWrapper:
    """Apply filter using polars."""
    native = df.native
    
    if op == "IS_NULL":
        result = native.filter(
            native[col].is_null() | (native[col].cast(str).str.strip() == "")
        )
    elif op == "IS_NOT_NULL":
        result = native.filter(
            native[col].is_not_null() & (native[col].cast(str).str.strip() != "")
        )
    elif op == "==":
        result = native.filter(native[col] == value)
    elif op == "!=":
        result = native.filter(native[col] != value)
    elif op == ">":
        result = native.filter(native[col] > value)
    elif op == "<":
        result = native.filter(native[col] < value)
    elif op == ">=":
        result = native.filter(native[col] >= value)
    elif op == "<=":
        result = native.filter(native[col] <= value)
    elif op == "CONTAINS":
        result = native.filter(native[col].cast(str).str.contains(value, literal=False))
    elif op == "STARTSWITH":
        result = native.filter(native[col].cast(str).str.starts_with(value))
    elif op == "ENDSWITH":
        result = native.filter(native[col].cast(str).str.ends_with(value))
    elif op == "IS_ONE_OF":
        result = native.filter(native[col].is_in(value))
    else:
        console.print(f"[yellow]Unknown operator: {op}, using ==[/yellow]")
        result = native.filter(native[col] == value)
    
    return DataFrameWrapper(result, "polars")


def _apply_filter_pandas(df: DataFrameWrapper, col: str, op: str, value: Any) -> DataFrameWrapper:
    """Apply filter using pandas."""
    native = df.native
    
    if op == "IS_NULL":
        mask = native[col].isna() | (native[col].astype(str).str.strip() == "")
    elif op == "IS_NOT_NULL":
        mask = native[col].notna() & (native[col].astype(str).str.strip() != "")
    elif op == "==":
        mask = native[col] == value
    elif op == "!=":
        mask = native[col] != value
    elif op == ">":
        mask = native[col] > value
    elif op == "<":
        mask = native[col] < value
    elif op == ">=":
        mask = native[col] >= value
    elif op == "<=":
        mask = native[col] <= value
    elif op == "CONTAINS":
        mask = native[col].astype(str).str.contains(value, case=False, na=False)
    elif op == "STARTSWITH":
        mask = native[col].astype(str).str.startswith(value, na=False)
    elif op == "ENDSWITH":
        mask = native[col].astype(str).str.endswith(value, na=False)
    elif op == "IS_ONE_OF":
        mask = native[col].isin(value)
    else:
        console.print(f"[yellow]Unknown operator: {op}, using ==[/yellow]")
        mask = native[col] == value
    
    return DataFrameWrapper(native[mask], "pandas")


# ╔══════════════════════════════════════════════════════════════════╗
# ║  SORT                                                             ║
# ╚══════════════════════════════════════════════════════════════════╝

def sort_table(df: DataFrameWrapper) -> OperationResult:
    """
    Interactive sorting by one or more columns.
    
    Args:
        df: DataFrameWrapper to sort
    
    Returns:
        OperationResult with sorted DataFrame
    """
    start = time.time()
    rows_before = len(df)
    cols_before = len(df.columns)
    
    console.print(Rule("[bold]⇅ Sort Data[/bold]"))
    
    # Select columns
    console.print("\n[bold]Select column(s) to sort by:[/bold]")
    cols = []
    
    while True:
        col = fuzzy_select_column(
            df.columns,
            prompt_text=f"Column {len(cols) + 1} (or empty to finish)",
            default=None
        )
        
        if not col:
            break
        
        if col not in cols:
            cols.append(col)
        
        if not Confirm.ask("Add another column?", default=False):
            break
    
    if not cols:
        console.print("[yellow]No columns selected, keeping original order.[/yellow]")
        return OperationResult(
            df=df, rows_before=rows_before, rows_after=rows_before,
            cols_before=cols_before, cols_after=cols_before,
            time_taken=time.time() - start, operation="sort"
        )
    
    # Select order
    console.print("\n[bold]Sort order:[/bold]")
    console.print("  [yellow]A[/yellow]  Ascending  (A→Z, 1→9)")
    console.print("  [yellow]D[/yellow]  Descending (Z→A, 9→1)")
    
    asc = Prompt.ask("Order", choices=["A", "D"], default="A").upper() != "D"
    
    # Apply sort
    try:
        sorted_df = df.sort(cols, ascending=asc)
    except Exception as e:
        console.print(f"[red]Sort error: {e}[/red]")
        return OperationResult(
            df=df, rows_before=rows_before, rows_after=rows_before,
            cols_before=cols_before, cols_after=cols_before,
            time_taken=time.time() - start, operation="sort"
        )
    
    time_taken = time.time() - start
    direction = "↑ Ascending" if asc else "↓ Descending"
    
    console.print(f"\n[green]✓ Sorted[/green] by [bold]{', '.join(cols)}[/bold] ({direction})")
    
    # Ask which columns to display
    display_cols = ask_columns_to_display(sorted_df, default_columns=cols)
    
    return OperationResult(
        df=sorted_df,
        rows_before=rows_before,
        rows_after=len(sorted_df),
        cols_before=cols_before,
        cols_after=len(sorted_df.columns),
        time_taken=time_taken,
        operation="sort",
        columns_displayed=display_cols
    )


# ╔══════════════════════════════════════════════════════════════════╗
# ║  RANK                                                             ║
# ╚══════════════════════════════════════════════════════════════════╝

def rank_table(df: DataFrameWrapper) -> OperationResult:
    """
    Rank rows by column(s) with tie methods.
    
    Features:
    - Rank by columns
    - Tie methods: min, max, average, first, dense
    - New column name
    - TopN per group option
    
    Args:
        df: DataFrameWrapper to rank
    
    Returns:
        OperationResult with ranked DataFrame
    """
    start = time.time()
    rows_before = len(df)
    cols_before = len(df.columns)
    
    console.print(Rule("[bold]📊 Rank Rows[/bold]"))
    
    # Step 1: Select rank column(s)
    console.print("\n[bold cyan]Step 1:[/bold cyan] Select column(s) to rank by")
    
    rank_cols = []
    while True:
        col = fuzzy_select_column(
            df.columns,
            prompt_text=f"Rank column {len(rank_cols) + 1} (empty to finish)",
            default=None
        )
        
        if not col:
            break
        
        if col not in rank_cols:
            rank_cols.append(col)
        
        if not Confirm.ask("Add another rank column?", default=False):
            break
    
    if not rank_cols:
        console.print("[yellow]No rank column selected.[/yellow]")
        return OperationResult(
            df=df, rows_before=rows_before, rows_after=rows_before,
            cols_before=cols_before, cols_after=cols_before,
            time_taken=time.time() - start, operation="rank"
        )
    
    # Step 2: Tie method
    console.print("\n[bold cyan]Step 2:[/bold cyan] Select tie method")
    console.print("  [yellow]1[/yellow]  min       - Lowest rank for ties (1,1,3)")
    console.print("  [yellow]2[/yellow]  max       - Highest rank for ties (2,2,3)")
    console.print("  [yellow]3[/yellow]  average   - Average rank for ties (1,2,3)")
    console.print("  [yellow]4[/yellow]  first     - First occurrence gets lower rank")
    console.print("  [yellow]5[/yellow]  dense     - Consecutive ranks (1,1,2)")
    
    tie_choices = {"1": "min", "2": "max", "3": "average", "4": "first", "5": "dense"}
    tie_choice = Prompt.ask("Tie method", choices=["1", "2", "3", "4", "5"], default="1")
    tie_method = tie_choices[tie_choice]
    
    # Step 3: New column name
    console.print("\n[bold cyan]Step 3:[/bold cyan] Enter name for rank column")
    rank_col_name = Prompt.ask("Rank column name", default="Rank").strip()
    if not rank_col_name:
        rank_col_name = "Rank"
    
    # Step 4: Ascending or descending
    console.print("\n[bold cyan]Step 4:[/bold cyan] Rank order")
    console.print("  [yellow]A[/yellow]  Ascending  (1 = smallest/largest value)")
    console.print("  [yellow]D[/yellow]  Descending (1 = largest/smallest value)")
    asc = Prompt.ask("Order", choices=["A", "D"], default="A").upper() == "A"
    
    # Step 5: Optional group
    has_group = Confirm.ask("\nGroup ranks by category?", default=False)
    group_col = None
    top_n = None
    
    if has_group:
        group_col = fuzzy_select_column(
            df.columns,
            prompt_text="Group by column",
            default=None
        )
        
        if group_col and Confirm.ask("Filter to top N per group?", default=False):
            n_str = Prompt.ask("Top N per group", default="10").strip()
            try:
                top_n = int(n_str)
            except ValueError:
                top_n = None
    
    # Apply ranking
    try:
        ranked_df = _apply_rank(df, rank_cols, rank_col_name, tie_method, asc, group_col, top_n)
    except Exception as e:
        console.print(f"[red]Rank error: {e}[/red]")
        return OperationResult(
            df=df, rows_before=rows_before, rows_after=rows_before,
            cols_before=cols_before, cols_after=cols_before,
            time_taken=time.time() - start, operation="rank"
        )
    
    rows_after = len(ranked_df)
    time_taken = time.time() - start
    
    console.print(f"\n[green]✓ Ranking complete![/green]")
    console.print(f"[dim]  Rank column: {rank_col_name} | Tie method: {tie_method}[/dim]")
    
    # Ask which columns to display
    display_cols = ask_columns_to_display(
        ranked_df, 
        default_columns=[rank_col_name] + rank_cols[:2]
    )
    
    return OperationResult(
        df=ranked_df,
        rows_before=rows_before,
        rows_after=rows_after,
        cols_before=cols_before,
        cols_after=len(ranked_df.columns),
        time_taken=time_taken,
        operation="rank",
        columns_displayed=display_cols
    )


def _apply_rank(
    df: DataFrameWrapper,
    rank_cols: List[str],
    rank_col_name: str,
    tie_method: str,
    ascending: bool,
    group_col: Optional[str] = None,
    top_n: Optional[int] = None
) -> DataFrameWrapper:
    """Apply ranking logic."""
    engine = df.engine
    
    if engine == "polars":
        return _apply_rank_polars(df, rank_cols, rank_col_name, tie_method, ascending, group_col, top_n)
    else:
        return _apply_rank_pandas(df, rank_cols, rank_col_name, tie_method, ascending, group_col, top_n)


def _apply_rank_polars(
    df: DataFrameWrapper,
    rank_cols: List[str],
    rank_col_name: str,
    tie_method: str,
    ascending: bool,
    group_col: Optional[str],
    top_n: Optional[int]
) -> DataFrameWrapper:
    """Apply ranking using polars."""
    native = df.native
    import polars as pl
    
    # Build ranking expression. Use scalar struct based ranking to preserve tie semantics and avoid numeric-only assumptions.
    from polars import col as pl_col

    try:
        key_expr = pl.struct([pl_col(c) for c in rank_cols])
        rank_expr = key_expr.rank(method=tie_method, reverse=not ascending).alias(rank_col_name)
    except Exception as e:
        raise ValueError(
            f"Unable to construct rank expression for columns {rank_cols}: {e}"
        )

    if group_col:
        # Rank within groups
        result = native.with_columns([rank_expr.over(group_col)])
        if top_n:
            result = result.filter(pl_col(rank_col_name) <= top_n)
    else:
        result = native.with_columns([rank_expr])
    
    return DataFrameWrapper(result, "polars")


def _apply_rank_pandas(
    df: DataFrameWrapper,
    rank_cols: List[str],
    rank_col_name: str,
    tie_method: str,
    ascending: bool,
    group_col: Optional[str],
    top_n: Optional[int]
) -> DataFrameWrapper:
    """Apply ranking using pandas."""
    import pandas as pd
    native = df.native.copy()
    
    # Map tie method
    method_map = {
        "min": "min",
        "max": "max", 
        "average": "average",
        "first": "first",
        "dense": "dense"
    }
    method = method_map.get(tie_method, "average")
    
    # Rank
    # Ensure rank columns are numeric or coercible to avoid TypeError
    for c in rank_cols:
        if not pd.api.types.is_numeric_dtype(native[c]):
            native[c] = pd.to_numeric(native[c], errors='coerce')

    if group_col:
        native = native.sort_values(rank_cols, ascending=ascending)
        native[rank_col_name] = native.groupby(group_col).cumcount() + 1
        if top_n:
            native = native[native[rank_col_name] <= top_n]
    else:
        composite = native[rank_cols].apply(lambda row: tuple(row), axis=1)
        native[rank_col_name] = composite.rank(method=method, ascending=ascending)
    
    return DataFrameWrapper(native, "pandas")


# ╔══════════════════════════════════════════════════════════════════╗
# ║  AGGREGATE                                                        ║
# ╚══════════════════════════════════════════════════════════════════╝

def aggregate_table(df: DataFrameWrapper) -> OperationResult:
    """
    Interactive aggregation (sum, avg, count, etc.) by group.
    
    Args:
        df: DataFrameWrapper to aggregate
    
    Returns:
        OperationResult with aggregated DataFrame
    """
    start = time.time()
    rows_before = len(df)
    cols_before = len(df.columns)
    
    console.print(Rule("[bold]∑ Aggregate Data[/bold]"))
    
    # Step 1: Select group column
    console.print("\n[bold cyan]Step 1:[/bold cyan] Select group column")
    group_col = fuzzy_select_column(
        df.columns,
        prompt_text="Group by",
        default=df.columns[0] if df.columns else None
    )
    
    if not group_col:
        console.print("[yellow]No group column selected.[/yellow]")
        return OperationResult(
            df=df, rows_before=rows_before, rows_after=rows_before,
            cols_before=cols_before, cols_after=cols_before,
            time_taken=time.time() - start, operation="aggregate"
        )
    
    # Step 2: Select value column
    console.print("\n[bold cyan]Step 2:[/bold cyan] Select value column to aggregate")
    value_cols = []
    
    while True:
        col = fuzzy_select_column(
            df.columns,
            prompt_text=f"Value column {len(value_cols) + 1} (empty to finish)",
            default=None
        )
        
        if not col:
            break
        
        if col not in value_cols and col != group_col:
            value_cols.append(col)
        
        if not Confirm.ask("Add another value column?", default=False):
            break
    
    if not value_cols:
        console.print("[yellow]No value column selected.[/yellow]")
        return OperationResult(
            df=df, rows_before=rows_before, rows_after=rows_before,
            cols_before=cols_before, cols_after=cols_before,
            time_taken=time.time() - start, operation="aggregate"
        )
    
    # Step 3: Select aggregate function
    console.print("\n[bold cyan]Step 3:[/bold cyan] Select aggregate function")
    console.print("  [yellow]1[/yellow]  sum     - Total")
    console.print("  [yellow]2[/yellow]  mean    - Average")
    console.print("  [yellow]3[/yellow]  count   - Row count")
    console.print("  [yellow]4[/yellow]  min     - Minimum")
    console.print("  [yellow]5[/yellow]  max     - Maximum")
    
    func_choices = {"1": "sum", "2": "mean", "3": "count", "4": "min", "5": "max"}
    func_choice = Prompt.ask("Function", choices=["1", "2", "3", "4", "5"], default="2")
    func = func_choices[func_choice]
    
    # Apply aggregation
    try:
        agg_df = _apply_aggregate(df, group_col, value_cols, func)
    except Exception as e:
        console.print(f"[red]Aggregation error: {e}[/red]")
        return OperationResult(
            df=df, rows_before=rows_before, rows_after=rows_before,
            cols_before=cols_before, cols_after=cols_before,
            time_taken=time.time() - start, operation="aggregate"
        )
    
    rows_after = len(agg_df)
    time_taken = time.time() - start
    
    console.print(f"\n[green]✓ Aggregation complete![/green]")
    console.print(f"[dim]  Group: {group_col} | Function: {func}[/dim]")
    console.print(f"[dim]  Result: {format_number(rows_after)} groups[/dim]")
    
    # Ask which columns to display
    display_cols = ask_columns_to_display(agg_df, default_columns=agg_df.columns[:4])
    
    return OperationResult(
        df=agg_df,
        rows_before=rows_before,
        rows_after=rows_after,
        cols_before=cols_before,
        cols_after=len(agg_df.columns),
        time_taken=time_taken,
        operation="aggregate",
        columns_displayed=display_cols
    )


def _apply_aggregate(
    df: DataFrameWrapper,
    group_col: str,
    value_cols: List[str],
    func: str
) -> DataFrameWrapper:
    """Apply aggregation."""
    engine = df.engine
    
    if engine == "polars":
        return _apply_aggregate_polars(df, group_col, value_cols, func)
    else:
        return _apply_aggregate_pandas(df, group_col, value_cols, func)


def _apply_aggregate_polars(
    df: DataFrameWrapper,
    group_col: str,
    value_cols: List[str],
    func: str
) -> DataFrameWrapper:
    """Apply aggregation using polars."""
    native = df.native
    
    # Build aggregation expressions
    agg_exprs = []
    for col in value_cols:
        from polars import col as pl_col
        if func == "sum":
            agg_exprs.append(pl_col(col).sum().alias(f"sum_{col}"))
        elif func == "mean":
            agg_exprs.append(pl_col(col).mean().alias(f"mean_{col}"))
        elif func == "count":
            agg_exprs.append(pl_col(col).count().alias(f"count_{col}"))
        elif func == "min":
            agg_exprs.append(pl_col(col).min().alias(f"min_{col}"))
        elif func == "max":
            agg_exprs.append(pl_col(col).max().alias(f"max_{col}"))
    
    result = native.group_by(group_col).agg(agg_exprs).sort(group_col)
    
    return DataFrameWrapper(result, "polars")


def _apply_aggregate_pandas(
    df: DataFrameWrapper,
    group_col: str,
    value_cols: List[str],
    func: str
) -> DataFrameWrapper:
    """Apply aggregation using pandas."""
    native = df.native
    
    agg_dict = {col: func for col in value_cols}
    result = native.groupby(group_col, dropna=False).agg(agg_dict).reset_index()
    
    # Rename columns
    new_cols = [group_col]
    for col in value_cols:
        new_cols.append(f"{func}_{col}")
    result.columns = new_cols
    
    return DataFrameWrapper(result.sort_values(group_col), "pandas")


# ╔══════════════════════════════════════════════════════════════════╗
# ║  SAVE / EXPORT                                                    ║
# ╚══════════════════════════════════════════════════════════════════╝

def save_table(
    df: DataFrameWrapper,
    path: str,
    mode: str = "replace",
    **kwargs
) -> bool:
    """
    Save DataFrame to file or database.
    
    Args:
        df: DataFrameWrapper to save
        path: Output path or connection string
        mode: Save mode - "append", "replace", "fail"
        **kwargs: Additional arguments
    
    Returns:
        True if successful
    """
    # Validate mode
    if mode not in ("append", "replace", "fail"):
        console.print(f"[red]Invalid mode: {mode}. Use 'append', 'replace', or 'fail'.[/red]")
        return False
    
    # Check for existing file
    if os.path.exists(path) and mode == "fail":
        console.print(f"[red]File exists and mode is 'fail': {path}[/red]")
        return False
    
    # Warn for destructive operations
    if os.path.exists(path) and mode in ("replace", "append"):
        if not Confirm.ask(f"[red]This will {mode} existing data. Continue?[/red]", default=False):
            console.print("[yellow]Save cancelled.[/yellow]")
            return False
    
    try:
        if path.endswith('.csv'):
            append_mode = mode == "append" or kwargs.get("mode") == "append"
            csv_header = not (append_mode and os.path.exists(path))
            csv_kwargs = {k: v for k, v in kwargs.items() if k not in ('mode', 'header')}
            if append_mode:
                df.to_csv(path, mode='a', header=csv_header, index=False, **csv_kwargs)
            else:
                df.to_csv(path, mode='w', header=csv_header, index=False, **csv_kwargs)
        elif path.endswith(('.db', '.sqlite', '.sqlite3')):
            table_name = kwargs.get('table_name', 'data')
            con_str = f"sqlite:///{path}"
            from sqlalchemy import create_engine
            engine = create_engine(con_str)
            df.to_sql(table_name, engine, if_exists=mode)
        else:
            console.print("[red]Unsupported file format. Use .csv or .sqlite[/red]")
            return False
        
        console.print(f"[green]✓ Saved to {path}[/green]")
        return True
    
    except Exception as e:
        console.print(f"[red]Save error: {e}[/red]")
        return False


def ask_save_options(df: DataFrameWrapper, default_name: str = "output.csv") -> Optional[tuple]:
    """
    Interactive save dialog.
    
    Args:
        df: DataFrameWrapper to save
        default_name: Default file name
    
    Returns:
        Tuple of (path, mode) or None if cancelled
    """
    console.print(Rule("[bold]💾 Save / Export[/bold]"))
    
    # Select format
    console.print("\n[bold]Select export format:[/bold]")
    console.print("  [yellow]1[/yellow]  CSV")
    console.print("  [yellow]2[/yellow]  SQLite")
    
    format_choice = Prompt.ask("Format", choices=["1", "2"], default="1")
    
    if format_choice == "1":
        path = Prompt.ask("CSV file path", default=default_name)
        fmt = "csv"
    else:
        path = Prompt.ask("SQLite path", default="data.sqlite")
        table_name = Prompt.ask("Table name", default="data")
        path = path  # Store table_name in kwargs
    
    # Select mode
    console.print("\n[bold]Save mode:[/bold]")
    console.print("  [yellow]r[/yellow]  Replace - Overwrite existing file")
    console.print("  [yellow]a[/yellow]  Append - Add to existing file")
    console.print("  [yellow]f[/yellow]  Fail - Error if file exists")
    
    mode_map = {"r": "replace", "a": "append", "f": "fail"}
    mode_choice = Prompt.ask("Mode", choices=["r", "a", "f"], default="r")
    mode = mode_map[mode_choice]
    
    return (path, mode, fmt if format_choice == "1" else "sqlite")
