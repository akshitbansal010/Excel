"""
Display functions - pure presentation layer using Rich tables and panels.
"""

import pandas as pd
import numpy as np

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box
from rich.rule import Rule

from config import BANNER, HELP_MENU
from helpers import col_letter, fmt_val
from session import Session

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
    
    Args:
        sess: Current session
    """
    df = sess.df
    name = sess.active
    tabs = "  ".join(
        f"[bold yellow]{t}[/bold yellow]" if t == name else f"[dim]{t}[/dim]"
        for t in sess.list_tables()
    )
    console.print(
        f"\n[bold green]● {len(df):,} rows[/bold green]  "
        f"[bold blue]▪ {len(df.columns)} cols[/bold blue]  "
        f"│ tables: {tabs}  [dim]([bold]H[/bold]=help)[/dim]"
    )


def show_columns(df: pd.DataFrame, compact: bool = False) -> None:
    """
    Display column inspector with type and statistics.
    
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
    t.add_column("Ref", style="bold yellow", width=5, justify="center")
    t.add_column("Column", style="bold white", min_width=18)
    t.add_column("Type", style="cyan", width=12)
    t.add_column("Nulls", style="red", width=15, justify="right")
    t.add_column("Unique", style="magenta", width=8, justify="right")
    t.add_column("Top 5 Values (val×count)", style="dim", min_width=38)

    for i, col in enumerate(df.columns):
        letter = col_letter(i)
        dtype = str(df[col].dtype)
        null_count = df[col].isna().sum()
        null_pct = f"{null_count:,} ({null_count/len(df)*100:.1f}%)" if len(df) else "0"
        unique_c = df[col].nunique(dropna=False)

        vc = df[col].dropna().value_counts().head(5)
        if len(vc) == 0:
            top = "[italic red]all null[/italic red]"
        else:
            top = "   ".join(f"[white]{str(v)[:16]}[/white][dim]×{c}[/dim]"
                            for v, c in vc.items())

        col_disp = f"[bold red]{col}[/bold red]" if null_count == len(df) else col
        t.add_row(letter, col_disp, dtype, null_pct, str(unique_c), top)

    console.print(t)


def show_preview(df: pd.DataFrame, n: int = 8, title: str = "Preview",
                 cols: list = None) -> None:
    """
    Display a preview of the DataFrame.
    
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
    t = Table(title=title, box=box.SIMPLE_HEAVY, show_lines=False,
              title_style="bold")
    t.add_column("#", style="dim", width=5, justify="right")
    for i, col in enumerate(disp_df.columns):
        full_i = list(df.columns).index(col)
        t.add_column(f"[yellow]{col_letter(full_i)}[/yellow]\n{col}",
                     overflow="fold", min_width=9, max_width=22)
    for row_i, (_, row) in enumerate(disp_df.head(n).iterrows()):
        t.add_row(str(row_i+1), *[fmt_val(v) for v in row])
    console.print(t)


def show_unique_inline(df: pd.DataFrame, col: str, limit: int = 60) -> list:
    """
    Compact boxed array of unique values — shown before user types filter.
    
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

    arr = "  ".join(
        f"[cyan]{str(v)[:28]}[/cyan][dim]×{vc[v]}[/dim]" for v in vals
    )
    extra = f"  [dim]…+{len(vc)-limit} more[/dim]" if len(vc) > limit else ""
    null_t = f"  [red]∅null×{null_n}[/red]" if null_n else ""

    console.print(
        Panel(arr + extra + null_t,
              title=f"[yellow]{col}[/yellow] — {len(vc)} unique",
              border_style="dim", padding=(0, 1))
    )
    return vals


def show_unique_full(df: pd.DataFrame, col: str, limit: int = 50) -> None:
    """
    Tall table with bar chart — used for U command.
    
    Args:
        df: DataFrame to analyze
        col: Column name
        limit: Maximum unique values to show
    """
    series = df[col]
    null_n = series.isna().sum()
    vc = series.dropna().value_counts()

    t = Table(title=f"🔍  Unique Values — [yellow]{col}[/yellow]",
              box=box.ROUNDED, title_style="bold")
    t.add_column("Value", style="white", min_width=24)
    t.add_column("Count", style="cyan", justify="right", width=9)
    t.add_column("Bar", min_width=24)

    if len(vc) == 0:
        console.print(f"[red]All {null_n} values are null.[/red]")
        return

    max_c = vc.iloc[0]
    for val, cnt in vc.head(limit).items():
        bar = "█" * max(1, int(cnt/max_c*24))
        t.add_row(str(val)[:50], str(cnt), f"[cyan]{bar}[/cyan]")
    if null_n:
        bar = "█" * max(1, int(null_n/max_c*24))
        t.add_row("[italic red]∅ null/blank[/italic red]", str(null_n), f"[red]{bar}[/red]")
    if len(vc) > limit:
        t.add_row(f"[dim]… {len(vc)-limit} more[/dim]", "", "")
    console.print(t)
    console.print(f"  [dim]Unique (non-null): [bold]{len(vc)}[/bold]  Null: [bold red]{null_n}[/bold red][/dim]\n")


def show_null_report(df: pd.DataFrame) -> None:
    """
    Display a report on null/missing values in each column.
    
    Args:
        df: DataFrame to analyze
    """
    t = Table(title="🩺  Null / Missing Report", box=box.ROUNDED,
              title_style="bold yellow")
    t.add_column("Ref", style="yellow", width=5, justify="center")
    t.add_column("Column", style="white", min_width=20)
    t.add_column("Type", style="cyan", width=10)
    t.add_column("Null #", justify="right", style="red")
    t.add_column("Null %", justify="right")
    t.add_column("Blanks", justify="right", style="magenta")
    t.add_column("Status")

    for i, col in enumerate(df.columns):
        n_null = df[col].isna().sum()
        n_blank = (df[col].astype(str).str.strip() == "").sum() if df[col].dtype == object else 0
        pct = n_null/len(df)*100 if len(df) else 0
        dtype = str(df[col].dtype)

        if n_null == 0 and n_blank == 0:
            status = "[green]✔ Clean[/green]"
        elif n_null == len(df):
            status = "[bold red]✖ All Null[/bold red]"
        elif pct > 50:
            status = "[red]⚠ High Nulls[/red]"
        elif pct > 10:
            status = "[yellow]△ Some Nulls[/yellow]"
        else:
            status = "[cyan]· Minor[/cyan]"

        t.add_row(col_letter(i), col, dtype, f"{n_null:,}", f"{pct:.1f}%",
                  f"{n_blank:,}", status)
    console.print(t)


def show_menu() -> None:
    """Display the help menu."""
    console.print(Panel(
        HELP_MENU,
        title="📋  DataEngine Pro  v2.0",
        border_style="cyan", padding=(0, 2)
    ))


def show_table_list(sess: Session) -> None:
    """
    Display list of tables in the session.
    
    Args:
        sess: Current session
    """
    from rich.table import Table
    t = Table(title="Session Tables", box=box.ROUNDED, title_style="bold cyan")
    t.add_column("●", width=3, justify="center")
    t.add_column("Name", style="bold white", min_width=20)
    t.add_column("Rows", justify="right", style="cyan")
    t.add_column("Cols", justify="right", style="magenta")
    t.add_column("Columns (preview)", style="dim", min_width=30)

    for name, tdf in sess.tables.items():
        marker = "[bold green]●[/bold green]" if name == sess.active else ""
        col_prev = ", ".join(tdf.columns[:6].tolist())
        if len(tdf.columns) > 6:
            col_prev += f" … +{len(tdf.columns)-6}"
        t.add_row(marker, name, f"{len(tdf):,}", str(len(tdf.columns)), col_prev)
    console.print(t)
