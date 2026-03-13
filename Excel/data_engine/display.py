"""
Display functions - pure presentation layer using Rich tables and panels.
Improved with better large data handling and Excel-like display.
"""

import pandas as pd
import numpy as np

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box
from rich.rule import Rule

from .config import BANNER, HELP_MENU
from .helpers import col_letter, fmt_val, fmt_val_compact, format_number
from .session import Session

console = Console()


# ╔══════════════════════════════════════════════════════════════════╗
# ║  CORE DISPLAY FUNCTIONS                                            ║
# ╚══════════════════════════════════════════════════════════════════╝

def show_banner() -> None:
    """Display the application banner."""
    console.print(BANNER)


def status_bar(sess: Session) -> None:
    """
    Display the status bar showing current session state.
    Shows row count, column count, and available tables.
    
    Args:
        sess: Current session
    """
    df = sess.df
    name = sess.active
    
    # Build table tabs
    tabs = []
    for t in sess.list_tables():
        if t == name:
            tabs.append(f"[bold yellow]{t}[/bold yellow]")
        else:
            tabs.append(f"[dim]{t}[/dim]")
    
    console.print(
        f"\n[bold green]●[/bold green] [bold]{format_number(len(df))}[/bold] rows   "
        f"[bold blue]▪[/bold blue] [bold]{len(df.columns)}[/bold] columns   "
        f"│ [bold]{' │ '.join(tabs)}[/bold]"
    )


def show_columns(df: pd.DataFrame, compact: bool = False) -> None:
    """
    Display column inspector with type and statistics.
    Shows column letter (A,B,C), name, type, and sample values.
    
    Args:
        df: DataFrame to inspect
        compact: If True, show compact single-line format
    """
    if compact:
        parts = [f"[yellow]{col_letter(i)}[/yellow] {c}" for i, c in enumerate(df.columns)]
        console.print("  " + "   ".join(parts))
        return

    t = Table(title="📊  Column Inspector", box=box.ROUNDED,
              show_lines=True, title_style="bold cyan")
    t.add_column("Col", style="bold yellow", width=5, justify="center")
    t.add_column("Column Name", style="bold white", min_width=18)
    t.add_column("Type", style="cyan", width=12)
    t.add_column("Nulls", style="red", width=12, justify="right")
    t.add_column("Unique", style="magenta", width=8, justify="right")
    t.add_column("Sample Values", style="dim", min_width=30)

    for i, col in enumerate(df.columns):
        letter = col_letter(i)
        dtype = str(df[col].dtype)
        null_count = df[col].isna().sum()
        null_pct = f"{null_count:,} ({null_count/len(df)*100:.0f}%)" if len(df) else "0"
        unique_c = df[col].nunique(dropna=False)

        vc = df[col].dropna().value_counts().head(4)
        if len(vc) == 0:
            top = "[italic red]all empty[/italic red]"
        else:
            top = "  ".join(f"[white]{fmt_val_compact(v)}[/white][dim]×{c}[/dim]"
                            for v, c in vc.items())

        # Highlight problematic columns
        col_disp = f"[bold red]{col}[/bold red]" if null_count == len(df) else col
        t.add_row(letter, col_disp, dtype, null_pct, str(unique_c), top)

    console.print(t)


def show_preview(df: pd.DataFrame, n: int = 8, title: str = "Preview",
                 cols: list = None) -> None:
    """
    Display a preview of the DataFrame.
    Shows row numbers and highlights column letters like Excel.
    
    Args:
        df: DataFrame to preview
        n: Number of rows to show
        title: Title for the table
        cols: Specific columns to show (None = all)
    """
    if df.empty:
        console.print("[red]⚠  No rows.[/red]")
        return

    disp_df = df[cols] if cols else df
    
    # For large data, show progress info
    total_rows = len(df)
    showing = min(n, total_rows)
    
    t = Table(
        title=f"{title} (showing {showing} of {format_number(total_rows)} rows)",
        box=box.SIMPLE_HEAVY, show_lines=False,
        title_style="bold"
    )
    t.add_column("#", style="dim", width=5, justify="right")
    
    # Add columns with Excel-style letters
    for i, col in enumerate(disp_df.columns):
        full_i = list(df.columns).index(col)
        t.add_column(
            f"[yellow]{col_letter(full_i)}[/yellow]\n{col}",
            overflow="fold", min_width=10, max_width=20
        )
    
    for row_i, (_, row) in enumerate(disp_df.head(n).iterrows()):
        t.add_row(str(row_i+1), *[fmt_val(v) for v in row])
    
    console.print(t)


def show_unique_inline(df: pd.DataFrame, col: str, limit: int = 50) -> list:
    """
    Compact inline display of unique values - shown before user types filter.
    Shows value and count in a compact panel.
    
    Args:
        df: DataFrame to analyze
        col: Column name
        limit: Maximum unique values to show
        
    Returns:
        List of unique values
    """
    series = df[col]
    null_n = series.isna().sum()
    vc = series.dropna().value_counts()
    vals = list(vc.index[:limit])

    # Build compact display
    parts = []
    for v in vals:
        parts.append(f"[cyan]{fmt_val_compact(v)}[/cyan][dim]×{vc[v]:,}[/dim]")
    
    arr = "  ".join(parts)
    extra = f"  [dim]… +{len(vc)-limit} more[/dim]" if len(vc) > limit else ""
    null_t = f"  [red]∅ {null_n:,}[/red]" if null_n else ""

    console.print(
        Panel(
            arr + extra + null_t,
            title=f"[yellow]{col}[/yellow] — {len(vc):,} unique values",
            border_style="dim", padding=(0, 1)
        )
    )
    return vals


def show_unique_full(df: pd.DataFrame, col: str, limit: int = 50) -> None:
    """
    Tall table with bar chart - used for U command.
    Shows all unique values with counts and visual bars.
    
    Args:
        df: DataFrame to analyze
        col: Column name
        limit: Maximum unique values to show
    """
    series = df[col]
    null_n = series.isna().sum()
    vc = series.dropna().value_counts()

    t = Table(
        title=f"🔍  Unique Values in [yellow]{col}[/yellow]",
        box=box.ROUNDED, title_style="bold"
    )
    t.add_column("Value", style="white", min_width=24)
    t.add_column("Count", style="cyan", justify="right", width=10)
    t.add_column("Distribution", min_width=20)

    if len(vc) == 0:
        console.print(f"[red]All {null_n:,} values are empty.[/red]")
        return

    max_c = vc.iloc[0]
    bar_max = 20
    
    for val, cnt in vc.head(limit).items():
        bar_len = max(1, int(cnt/max_c*bar_max))
        bar = "█" * bar_len
        t.add_row(str(val)[:40], f"{cnt:,}", f"[cyan]{bar}[/cyan]")
    
    if null_n:
        bar_len = max(1, int(null_n/max_c*bar_max)) if max_c > 0 else 0
        bar = "▒" * bar_len
        t.add_row("[italic red]∅ empty[/italic red]", f"{null_n:,}", f"[red]{bar}[/red]")
    
    if len(vc) > limit:
        t.add_row(f"[dim]… {len(vc)-limit} more[/dim]", "", "")
    
    console.print(t)
    console.print(f"\n[dim]Unique: [bold]{len(vc):,}[/bold]   Empty: [bold red]{null_n:,}[/bold red]   Total: [bold]{len(df):,}[/bold][/dim]")


def show_null_report(df: pd.DataFrame) -> None:
    """
    Display a report on null/missing values in each column.
    Color-coded status for quick assessment.
    
    Args:
        df: DataFrame to analyze
    """
    t = Table(
        title="🩺  Missing Values Report",
        box=box.ROUNDED, title_style="bold yellow"
    )
    t.add_column("Col", style="yellow", width=5, justify="center")
    t.add_column("Column", style="white", min_width=18)
    t.add_column("Type", style="cyan", width=10)
    t.add_column("Missing", justify="right", style="red", width=10)
    t.add_column("%", justify="right", width=6)
    t.add_column("Status", width=15)

    for i, col in enumerate(df.columns):
        n_null = df[col].isna().sum()
        n_blank = (df[col].astype(str).str.strip() == "").sum() if df[col].dtype == object else 0
        pct = n_null/len(df)*100 if len(df) else 0
        dtype = str(df[col].dtype)

        # Status based on severity
        if n_null == 0 and n_blank == 0:
            status = "[green]✓ Clean[/green]"
        elif n_null == len(df):
            status = "[bold red]✖ All Empty[/bold red]"
        elif pct > 50:
            status = "[red]⚠ High Missing[/red]"
        elif pct > 10:
            status = "[yellow]△ Some Missing[/yellow]"
        else:
            status = "[cyan]· Minor[/cyan]"

        t.add_row(col_letter(i), col, dtype, f"{n_null:,}", f"{pct:.0f}%", status)
    
    console.print(t)
    
    # Summary
    total_cells = len(df) * len(df.columns)
    total_missing = df.isna().sum().sum()
    if total_missing > 0:
        console.print(f"\n[dim]Total: {format_number(total_missing)} cells missing ({total_missing/total_cells*100:.1f}%)[/dim]")


def show_menu() -> None:
    """Display the help menu."""
    console.print(Panel(
        HELP_MENU,
        title="📋  DataEngine Pro  v2.0  —  Commands",
        border_style="cyan", padding=(0, 2)
    ))


def show_table_list(sess: Session) -> None:
    """
    Display list of tables in the session with row/column counts.
    
    Args:
        sess: Current session
    """
    t = Table(title="📁  Tables in Session", box=box.ROUNDED, title_style="bold cyan")
    t.add_column("●", width=3, justify="center")
    t.add_column("Table Name", style="bold white", min_width=20)
    t.add_column("Rows", justify="right", style="cyan")
    t.add_column("Cols", justify="right", style="magenta")
    t.add_column("Columns", style="dim", min_width=25)

    for name, tdf in sess.tables.items():
        marker = "[bold green]●[/bold green]" if name == sess.active else ""
        col_prev = ", ".join(tdf.columns[:5].tolist())
        if len(tdf.columns) > 5:
            col_prev += f" +{len(tdf.columns)-5} more"
        t.add_row(marker, name, f"{len(tdf):,}", str(len(tdf.columns)), col_prev)
    
    console.print(t)


def show_progress(current: int, total: int, message: str = "") -> None:
    """
    Show simple progress indicator for long operations.
    
    Args:
        current: Current progress count
        total: Total count
        message: Optional message
    """
    pct = (current / total * 100) if total > 0 else 0
    bar_len = 20
    filled = int(bar_len * current / total) if total > 0 else 0
    bar = "█" * filled + "░" * (bar_len - filled)
    
    msg = f" {message}" if message else ""
    console.print(f"\r[cyan]{bar}[/cyan] {pct:.0f}% ({current:,}/{total:,}){msg}", end="")


def clear_progress() -> None:
    """Clear the progress line."""
    console.print("\r" + " " * 60, end="\r")
