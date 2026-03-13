"""
View module — operations for viewing, inspecting, and searching data.
"""

import numpy as np
import pandas as pd
from rapidfuzz import fuzz
from rapidfuzz import process as fz_process

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.rule import Rule
from rich.table import Table

from ..display import show_columns, show_preview, show_unique_inline
from ..helpers import ask_cols, resolve
from ..session import Session

console = Console()

# Store focus columns in session
_focus_columns: dict = {}


def op_focus_view(sess: Session) -> None:
    """Focus mode - select columns to always show in preview."""
    global _focus_columns
    df = sess.df
    console.print(Rule("[bold]Focus View (Freeze Columns)[/bold]"))
    
    show_columns(df, compact=True)
    
    console.print("\n[bold]Current focus columns:[/bold]")
    current = _focus_columns.get(sess.active, [])
    if current:
        console.print(f"  [cyan]{', '.join(current)}[/cyan]")
    else:
        console.print("  [dim]None (showing all columns)[/dim]")
    
    console.print("\n[dim]Options: [bold]S[/bold]et focus columns  [bold]C[/bold]lear  [bold]V[/bold]iew with focus  [bold]0[/bold] back[/dim]")
    action = Prompt.ask("Action", choices=["S", "C", "V", "0"]).upper()
    
    if action == "0":
        return
    
    if action == "C":
        _focus_columns.pop(sess.active, None)
        console.print("[dim]Focus cleared.[/dim]")
        return
    
    if action == "S":
        cols = ask_cols("Columns to focus on (comma-sep)", df)
        if not cols:
            console.print("[red]No columns selected.[/red]")
            return
        _focus_columns[sess.active] = cols
        console.print(f"[green]✔ Focus set to: {', '.join(cols)}[/green]")
        return
    
    if action == "V":
        current = _focus_columns.get(sess.active, [])
        if not current:
            console.print("[yellow]No focus columns set. Use S to set first.[/yellow]")
            return
        show_preview(df, cols=current, title=f"Focus View: {', '.join(current)}")
        return


def op_pin_column(sess: Session) -> None:
    """Pin mode - always show column A alongside view."""
    df = sess.df
    console.print(Rule("[bold]Pin Column (Always Visible)[/bold]"))
    
    if len(df.columns) == 0:
        console.print("[red]No columns to pin.[/red]")
        return
    
    # Default to first column (like Excel's freeze panes)
    default_col = df.columns[0]
    show_columns(df, compact=True)
    
    col_input = Prompt.ask("Column to pin (letter/name, default=first)", default=default_col)
    col = resolve(col_input, df)
    
    if not col:
        console.print("[red]Column not found.[/red]")
        return
    
    # Show preview with pinned column first
    pinned_cols = [col] + [c for c in df.columns if c != col]
    show_preview(df, cols=pinned_cols, title=f"Pinned: {col}")


def op_edit_row(sess: Session) -> pd.DataFrame:
    """Edit a specific row's cell values."""
    df = sess.df
    console.print(Rule("[bold]Row-Level Edit[/bold]"))
    
    console.print("\n[bold]Find row by:[/bold]")
    console.print("  [yellow]1[/yellow]  Row number (e.g., 42)")
    console.print("  [yellow]2[/yellow]  Filter to find row first")
    mode = Prompt.ask("Mode", choices=["1", "2"], default="1")
    
    row_idx = None
    
    if mode == "1":
        row_num = Prompt.ask("Row number", default="1")
        try:
            row_idx = int(row_num) - 1  # Convert to 0-based
            if row_idx < 0 or row_idx >= len(df):
                console.print(f"[red]Row {row_num} out of range (1-{len(df)}).[/red]")
                return df
        except ValueError:
            console.print("[red]Invalid row number.[/red]")
            return df
    else:
        # Use filter to find the row
        show_columns(df, compact=True)
        col = resolve(Prompt.ask("Column to filter on"), df)
        if not col:
            console.print("[red]Column not found.[/red]"); return df
        
        show_unique_inline(df, col)
        val = Prompt.ask("Value to match")
        
        # Find matching rows
        matches = df[df[col].astype(str).str.lower() == val.lower()]
        if len(matches) == 0:
            console.print("[red]No matching rows found.[/red]"); return df
        elif len(matches) > 1:
            console.print(f"[yellow]Multiple matches ({len(matches)} rows). Using first one.[/yellow]")
        
        row_idx = matches.index[0]
        console.print(f"[green]Found row at index {row_idx}.[/green]")
    
    # Show the row
    console.print(f"\n[bold]Row {row_idx + 1}:[/bold]")
    t = Table(box=box.SIMPLE)
    t.add_column("Column", style="cyan", width=20)
    t.add_column("Current Value", style="white", min_width=25)
    t.add_column("New Value", style="yellow", width=25)
    
    row_data = df.loc[row_idx]
    for col_name in df.columns:
        current_val = row_data[col_name]
        display_val = str(current_val)[:50] if pd.notna(current_val) else "[null]"
        t.add_row(col_name, display_val, "")
    console.print(t)
    
    # Edit options
    console.print("\n[dim]Options: [bold]E[/bold]dit a cell  [bold]D[/bold]one  [bold]0[/bold] cancel[/dim]")
    
    while True:
        action = Prompt.ask("Action", choices=["E", "D", "0"]).upper()
        
        if action == "0":
            return df
        
        if action == "D":
            console.print(f"[green]✔ Row {row_idx + 1} edited.[/green]")
            return df
        
        if action == "E":
            col = resolve(Prompt.ask("Column to edit"), df)
            if not col:
                console.print("[red]Column not found.[/red]")
                continue
            
            current = row_data[col]
            console.print(f"Current value: [cyan]{current}[/cyan]")
            new_val = Prompt.ask("New value (blank = null)").strip()
            
            # Convert to appropriate type
            if new_val == "":
                df.at[row_idx, col] = None
            else:
                # Try to infer type
                try:
                    if '.' in new_val:
                        df.at[row_idx, col] = float(new_val)
                    else:
                        df.at[row_idx, col] = int(new_val)
                except ValueError:
                    df.at[row_idx, col] = new_val
            
            console.print(f"[green]✔ Updated {col} = {new_val if new_val else 'null'}[/green]")
            # Refresh row_data
            row_data = df.loc[row_idx]


def op_preview(sess: Session):
    """Preview with column picker."""
    df = sess.df
    console.print(Rule("[bold]Preview[/bold]"))
    while True:
        n_raw = Prompt.ask("How many rows?", default="10").strip()
        try:
            n = int(n_raw)
            if n > 0:
                break
            console.print("[yellow]Please enter a positive integer.[/yellow]")
        except ValueError:
            console.print("[yellow]Invalid number. Please enter a valid integer.[/yellow]")
    ci = Prompt.ask("Columns to show (letters/names comma-sep, blank = all)").strip()
    cols = ask_cols(ci, df) if ci else None
    show_preview(df, n=n, cols=cols, title=f"▸ {sess.active}")


def op_search(sess: Session):
    """Search operations."""
    df = sess.df
    console.print(Rule("[bold]Search[/bold]"))
    console.print("  [bold]1[/bold]  Full-text search across all columns")
    console.print("  [bold]2[/bold]  Fuzzy search within one column")
    mode = Prompt.ask("Mode", choices=["1","2"], default="1")

    if mode == "1":
        term = Prompt.ask("Search term")
        mask = pd.Series(False, index=df.index)
        for col in df.columns:
            mask |= df[col].astype(str).str.contains(term, case=False, na=False, regex=False)
        result = df[mask]
        console.print(f"[cyan]{len(result):,} rows found for '{term}'.[/cyan]")
        show_preview(result, n=15)

    else:
        show_columns(df, compact=True)
        col  = resolve(Prompt.ask("Column"), df)
        if not col: return
        term = Prompt.ask("Search term")
        uniques = df[col].dropna().astype(str).unique().tolist()
        matches = fz_process.extract(term, uniques, scorer=fuzz.WRatio, limit=10)

        t = Table(title=f"Fuzzy Search — {col}", box=box.ROUNDED)
        t.add_column("Value",  style="white")
        t.add_column("Score",  style="cyan",    justify="right")
        t.add_column("Count",  style="magenta", justify="right")
        for val, score, _ in matches:
            cnt = str((df[col].astype(str)==val).sum())
            t.add_row(val, f"{score:.0f}%", cnt)
        console.print(t)


def op_stats(sess: Session):
    """Column statistics."""
    df = sess.df
    console.print(Rule("[bold]Column Statistics[/bold]"))
    show_columns(df, compact=True)
    ci = Prompt.ask("Column (blank = all numeric)").strip()
    if ci:
        cols = [c for c in [resolve(ci, df)] if c]
    else:
        cols = df.select_dtypes(include=np.number).columns.tolist()
    if not cols:
        console.print("[yellow]No numeric columns.[/yellow]"); return

    desc = df[cols].describe(percentiles=[.1,.25,.5,.75,.9,.95]).round(4)
    t = Table(box=box.ROUNDED, show_lines=True)
    t.add_column("Stat", style="yellow")
    for c in cols: t.add_column(c, justify="right")
    for stat in desc.index:
        t.add_row(stat, *[str(desc.loc[stat, c]) for c in cols])
    console.print(t)
