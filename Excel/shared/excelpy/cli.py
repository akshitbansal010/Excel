"""
CLI interface for excelpy using Typer.
Provides interactive command-line interface for data manipulation.
"""

import os
import sys
from typing import Optional, List
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table
from rich import box

# Import excelpy modules
from excelpy import (
    load_table,
    show_preview,
    ask_condition_and_filter,
    sort_table,
    rank_table,
    aggregate_table,
    save_table,
    ask_columns_to_display,
    get_engine,
    is_polars_available,
    __version__,
)
from excelpy.engine import DataFrameWrapper
from excelpy.helpers import col_letter, build_col_map, resolve_column

app = typer.Typer(
    name="excelpy",
    help="Interactive CLI for treating CSV/DB tables like Excel",
    add_completion=False,
)

console = Console()

# Global state
current_df: Optional[DataFrameWrapper] = None
current_path: Optional[str] = None


def get_df() -> DataFrameWrapper:
    """Get current DataFrame or raise error."""
    if current_df is None:
        typer.echo("No data loaded. Use 'excelpy load <path>' first.")
        raise typer.Exit(code=1)
    return current_df


def set_df(df: DataFrameWrapper, path: str) -> None:
    """Set current DataFrame."""
    global current_df, current_path
    current_df = df
    current_path = path


def check_large_file(path: str, force: bool = False) -> bool:
    """
    Check if file is large (>100MB) and warn user.
    
    Args:
        path: Path to file
        force: Force operation without warning
    
    Returns:
        True to proceed, False to cancel
    """
    if force:
        return True
    
    try:
        size_mb = os.path.getsize(path) / (1024 * 1024)
        if size_mb > 100:
            console.print(f"[yellow]⚠ Large file detected: {size_mb:.1f} MB[/yellow]")
            console.print("[dim]Use --force to run full operation without preview mode[/dim]")
            return False
    except OSError:
        # File access error; let caller handle
        pass
    return True


@app.command()
def load(
    path: str = typer.Argument(..., help="Path to CSV file or SQLite database"),
    table_name: Optional[str] = typer.Option(None, "--table", "-t", help="Table name for SQLite"),
    engine: Optional[str] = typer.Option(None, "--engine", "-e", help="Force engine: polars or pandas"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip large file warning"),
) -> None:
    """
    Load a CSV file or database table.
    
    Examples:
        excelpy load data.csv
        excelpy load data.csv --engine pandas
        excelpy load database.sqlite --table my_table
    """
    global current_df, current_path
    
    # Check if file exists
    if not os.path.exists(path):
        console.print(f"[red]File not found: {path}[/red]")
        raise typer.Exit(1)
    
    # Check file size
    if not force and not check_large_file(path, force):
        console.print("[yellow]Operation cancelled. Use --force to proceed anyway.[/yellow]")
        raise typer.Exit(0)
    
    try:
        # Load data
        if path.endswith(('.db', '.sqlite', '.sqlite3')):
            if not table_name:
                console.print("[yellow]Please specify --table for SQLite database.[/yellow]")
                raise typer.Exit(1)
            df = load_table(path, force_engine=engine, table_name=table_name)
        else:
            df = load_table(path, force_engine=engine)
        
        set_df(df, path)
        
        # Show preview
        show_preview(df, n=10, title=f"Loaded: {os.path.basename(path)}")
        
        # Show column summary
        _show_columns(df)
        
    except Exception as e:
        console.print(f"[red]Error loading file: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def columns() -> None:
    """
    Show all columns with their Excel-style letters.
    """
    df = get_df()
    _show_columns(df)


def _show_columns(df: DataFrameWrapper) -> None:
    """Display column summary."""
    t = Table(title="📊 Columns", box=box.ROUNDED, show_lines=True)
    t.add_column("Excel", style="yellow", width=6, justify="center")
    t.add_column("Column Name", style="white", min_width=18)
    t.add_column("Type", style="cyan", width=12)
    
    for i, col in enumerate(df.columns):
        t.add_row(col_letter(i), col, df.dtypes.get(col, "unknown"))
    
    console.print(t)


@app.command()
def preview(
    n: int = typer.Option(10, "--rows", "-n", help="Number of rows to show"),
    cols: Optional[str] = typer.Option(None, "--columns", "-c", help="Columns to show (comma-separated)"),
) -> None:
    """
    Show a preview of the data.
    
    Examples:
        excelpy preview
        excelpy preview --rows 20
        excelpy preview -c "name,age,salary"
    """
    df = get_df()
    
    # Parse columns
    columns = None
    if cols:
        columns = [c.strip() for c in cols.split(",")]
        resolved = []
        unresolved = []
        for c in columns:
            r = resolve_column(c, df.columns, allow_fuzzy=False)
            if r:
                resolved.append(r)
            else:
                unresolved.append(c)
        if unresolved:
            raise ValueError(f"Could not resolve columns: {', '.join(unresolved)}")
        columns = resolved if resolved else None
    
    show_preview(df, n=n, title="Preview", columns=columns)


@app.command()
def filter() -> None:
    """
    Filter rows using interactive 3-step wizard.
    
    Step 1: Select column
    Step 2: Select operator
    Step 3: Enter value
    """
    df = get_df()
    
    result = ask_condition_and_filter(df)
    
    if result.df is not None and result.df != df:
        set_df(result.df, current_path)
        
        # Show preview with selected columns
        if result.columns_displayed:
            show_preview(result.df, n=10, columns=result.columns_displayed)
        
        _show_operation_summary(result)


@app.command()
def sort(
    cols: str = typer.Option(..., "--columns", "-c", help="Columns to sort by (comma-separated)"),
    ascending: bool = typer.Option(True, "--ascending/--descending", "-a/-d"),
) -> None:
    """
    Sort data by column(s).
    
    Examples:
        excelpy sort -c "name"
        excelpy sort -c "date,price" --descending
    """
    df = get_df()
    
    # Parse columns
    col_list = [c.strip() for c in cols.split(",")]
    resolved = []
    for c in col_list:
        r = resolve_column(c, df.columns)
        if r:
            resolved.append(r)
    
    if not resolved:
        console.print("[red]No valid columns specified.[/red]")
        raise typer.Exit(1)
    
    try:
        sorted_df = df.sort(resolved, ascending=ascending)
        set_df(sorted_df, current_path)
        
        direction = "↑ Ascending" if ascending else "↓ Descending"
        console.print(f"[green]✓ Sorted[/green] by [bold]{', '.join(resolved)}[/bold] ({direction})")
        
        # Show preview
        show_preview(sorted_df, n=10)
        
    except Exception as e:
        console.print(f"[red]Sort error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def rank(
    cols: str = typer.Option(..., "--columns", "-c", help="Columns to rank by (comma-separated)"),
    name: str = typer.Option("Rank", "--name", "-n", help="Name for rank column"),
    method: str = typer.Option("min", "--method", "-m", help="Tie method: min, max, average, dense"),
    ascending: bool = typer.Option(True, "--ascending/--descending", "-a/-d"),
    group: Optional[str] = typer.Option(None, "--group", "-g", help="Group column"),
    top: Optional[int] = typer.Option(None, "--top", "-t", help="Top N per group"),
) -> None:
    """
    Rank rows by column(s).
    
    Examples:
        excelpy rank -c "score" -n "rank"
        excelpy rank -c "sales" -m "max" --group "region" --top 5
    """
    df = get_df()
    
    # Parse columns
    col_list = [c.strip() for c in cols.split(",")]
    resolved = []
    for c in col_list:
        r = resolve_column(c, df.columns)
        if r:
            resolved.append(r)
    
    if not resolved:
        console.print("[red]No valid columns specified.[/red]")
        raise typer.Exit(1)
    
    try:
        from excelpy.core import _apply_rank
        ranked_df = _apply_rank(df, resolved, name, method, ascending, group, top)
        set_df(ranked_df, current_path)
        
        console.print(f"[green]✓ Ranked[/green] by [bold]{', '.join(resolved)}[/bold]")
        console.print(f"[dim]  Rank column: {name} | Method: {method}[/dim]")
        
        # Show preview
        show_preview(ranked_df, n=10, columns=[name] + resolved[:2])
        
    except Exception as e:
        console.print(f"[red]Rank error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def aggregate(
    group: str = typer.Option(..., "--group", "-g", help="Group column"),
    cols: str = typer.Option(..., "--columns", "-c", help="Value columns (comma-separated)"),
    func: str = typer.Option("mean", "--function", "-f", help="Aggregate function: sum, mean, count, min, max"),
) -> None:
    """
    Aggregate data by group.
    
    Examples:
        excelpy aggregate -g "category" -c "sales" -f sum
        excelpy aggregate -g "region" -c "price,quantity" -f mean
    """
    df = get_df()
    
    # Resolve group column
    group_col = resolve_column(group, df.columns)
    if not group_col:
        console.print(f"[red]Group column not found: {group}[/red]")
        raise typer.Exit(1)
    
    # Parse value columns
    col_list = [c.strip() for c in cols.split(",")]
    value_cols = []
    for c in col_list:
        r = resolve_column(c, df.columns)
        if r and r != group_col:
            value_cols.append(r)
    
    if not value_cols:
        console.print("[red]No valid value columns specified.[/red]")
        raise typer.Exit(1)
    
    try:
        from excelpy.core import _apply_aggregate
        agg_df = _apply_aggregate(df, group_col, value_cols, func)
        set_df(agg_df, current_path)
        
        console.print(f"[green]✓ Aggregated[/green] by [bold]{group_col}[/bold]")
        console.print(f"[dim]  Function: {func} | Columns: {', '.join(value_cols)}[/dim]")
        
        # Show preview
        show_preview(agg_df, n=20)
        
    except Exception as e:
        console.print(f"[red]Aggregate error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def save(
    path: str = typer.Argument(..., help="Output path"),
    mode: str = typer.Option("replace", "--mode", "-m", help="Save mode: replace, append, fail"),
) -> None:
    """
    Save current data to file.
    
    Examples:
        excelpy save output.csv
        excelpy save data.sqlite --mode append
    """
    df = get_df()
    
    try:
        success = save_table(df, path, mode=mode)
        if not success:
            raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Save error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def info() -> None:
    """
    Show information about current data.
    """
    df = get_df()
    
    t = Table(title="📊 Data Info", box=box.ROUNDED)
    t.add_column("Property", style="cyan")
    t.add_column("Value", style="white")
    
    t.add_row("Rows", f"{len(df):,}")
    t.add_row("Columns", str(len(df.columns)))
    t.add_row("Engine", df.engine)
    t.add_row("Source", current_path or "N/A")
    
    console.print(t)


def _show_operation_summary(result) -> None:
    """Show summary after an operation."""
    console.print(f"\n[bold]Summary:[/bold]")
    console.print(f"  Rows: {result.rows_before:,} → {result.rows_after:,}")
    console.print(f"  Columns: {result.cols_before} → {result.cols_after}")
    console.print(f"  Time: {result.time_taken:.2f}s")


@app.callback()
def main(
    version: bool = typer.Option(False, "--version", help="Show version"),
) -> None:
    """
    excelpy - Interactive CLI for treating CSV/DB tables like Excel.
    """
    if version:
        console.print(f"[bold]excelpy[/bold] version {__version__}")
        console.print(f"Engine: polars {'[green]✓[/green]' if is_polars_available() else '[red]✗[/red]'}")
        raise typer.Exit(0)


if __name__ == "__main__":
    app()
