"""
Core data operations - all the interactive transformations.
Each function takes a Session object, performs some action, and
often returns a modified DataFrame.
"""

import os
from datetime import datetime

import numpy as np
import pandas as pd
from rapidfuzz import fuzz
from rapidfuzz import process as fz_process

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.rule import Rule
from rich.table import Table

from database import db_load, db_save, db_tables
from display import (
    show_columns, show_null_report, show_preview,
    show_unique_full, show_unique_inline
)
from helpers import (
    ask_cols, col_letter, fmt_val, fuzzy_pick_value,
    fuzzy_pick_values_list, resolve, clean_number_string
)
from session import Session

console = Console()


# ╔══════════════════════════════════════════════════════════════════╗
# ║  SMART LOAD & TYPE DETECTION (1.1)                                ║
# ╚══════════════════════════════════════════════════════════════════╝

def scan_column_issues(df: pd.DataFrame) -> dict:
    """
    Scan all columns and identify type issues.
    Returns a dict with issue categories and affected columns.
    """
    issues = {
        "date_like_strings": [],    # Looks like dates but stored as strings
        "float_id_precision": [],    # Large integers stored as float (precision loss)
        "high_nulls": [],            # 90%+ null values
        "unique_values": [],         # All values unique (likely ID)
        "mixed_types": [],           # Column has mixed data types
    }
    
    for col in df.columns:
        # Check for high nulls (90%+)
        null_pct = df[col].isna().sum() / len(df) if len(df) > 0 else 0
        if null_pct >= 0.9:
            issues["high_nulls"].append(col)
        
        # Check for unique values (likely ID column)
        unique_count = df[col].nunique(dropna=False)
        if unique_count == len(df) and len(df) > 10:
            issues["unique_values"].append(col)
        
        # Check for float precision issues with large integers
        if df[col].dtype == 'float64' or df[col].dtype == 'float32':
            non_null = df[col].dropna()
            if len(non_null) > 0:
                # Check if values look like integers (within 0.1 of integer)
                int_like = non_null.apply(lambda x: abs(x - round(x)) < 0.1 if pd.notna(x) else False)
                if int_like.all():
                    # Check for large numbers that might have precision loss
                    large_nums = non_null[abs(non_null) >= 1e15]
                    if len(large_nums) > 0:
                        issues["float_id_precision"].append(col)
        
        # Check for date-like strings
        if df[col].dtype == 'object':
            sample = df[col].dropna().head(20).astype(str)
            if len(sample) > 0:
                # Try parsing as dates
                try:
                    parsed = pd.to_datetime(sample, errors='coerce')
                    success_rate = parsed.notna().sum() / len(sample)
                    if success_rate >= 0.7:
                        issues["date_like_strings"].append(col)
                except:
                    pass
        
        # Check for mixed types in same column
        if df[col].dtype == 'object':
            type_sample = df[col].dropna().head(100)
            has_int = type_sample.apply(lambda x: str(x).isdigit() if pd.notna(x) else False).any()
            has_float = type_sample.apply(lambda x: '.' in str(x) and str(x).replace('.','',1).isdigit() if pd.notna(x) else False).any()
            has_text = type_sample.apply(lambda x: any(c.isalpha() for c in str(x)) if pd.notna(x) else False).any()
            if sum([has_int, has_float, has_text]) >= 2:
                issues["mixed_types"].append(col)
    
    return issues


def show_load_report(df: pd.DataFrame, issues: dict) -> None:
    """Display a load report showing identified issues."""
    t = Table(title="📋  Load Report — Column Issues Detected", 
              box=box.ROUNDED, title_style="bold cyan")
    t.add_column("Issue Type", style="yellow", width=25)
    t.add_column("Affected Columns", style="white", min_width=30)
    t.add_column("Count", style="magenta", width=8, justify="right")
    
    issue_labels = {
        "date_like_strings": "Date-like strings",
        "float_id_precision": "Float ID (precision risk)",
        "high_nulls": "High nulls (90%+)",
        "unique_values": "Unique values (likely ID)",
        "mixed_types": "Mixed types",
    }
    
    total_issues = 0
    for issue_type, cols in issues.items():
        if cols:
            label = issue_labels.get(issue_type, issue_type)
            col_str = ", ".join(cols[:5])
            if len(cols) > 5:
                col_str += f" … +{len(cols)-5} more"
            t.add_row(label, col_str, str(len(cols)))
            total_issues += len(cols)
    
    if total_issues == 0:
        console.print(Panel("[green]✔ No issues detected. Your data looks clean![/green]",
                          title="Load Report", border_style="green"))
    else:
        console.print(Panel(f"[yellow]⚠ Found {total_issues} potential issues across {sum(1 for v in issues.values() if v)} columns.[/yellow]\n\n"
                          "Use 'F' to run the Smart Fix wizard.",
                          title="Load Report", border_style="yellow"))
        console.print(t)


def op_smart_fix(sess: Session) -> pd.DataFrame:
    """Smart Load Fix - scan and fix identified column issues."""
    df = sess.df
    console.print(Rule("[bold]Smart Load Fix Wizard[/bold]"))
    
    issues = scan_column_issues(df)
    show_load_report(df, issues)
    
    # If no issues, exit
    if not any(issues.values()):
        return df
    
    console.print("\n[dim]Select an issue to fix, or 0 to exit.[/dim]")
    
    options = []
    for issue_type, cols in issues.items():
        if cols:
            options.append((issue_type, cols))
    
    if not options:
        return df
    
    for i, (issue_type, cols) in enumerate(options):
        label = {
            "date_like_strings": "1. Convert date-like strings to actual dates",
            "float_id_precision": "2. Convert float IDs to integers (avoid precision loss)",
            "high_nulls": "3. Drop columns with 90%+ nulls",
            "unique_values": "4. Mark columns as likely IDs (for your reference)",
            "mixed_types": "5. Standardize mixed-type columns",
        }.get(issue_type, f"{i+1}. {issue_type}")
        console.print(f"  {label}")
        console.print(f"     [dim]Affected: {', '.join(cols[:3])}{' ...' if len(cols) > 3 else ''}[/dim]")
    
    choice = Prompt.ask("Fix which issue?", choices=["0"] + [str(i+1) for i in range(len(options))])
    
    if choice == "0":
        return df
    
    issue_type, cols = options[int(choice)-1]
    
    if issue_type == "date_like_strings":
        console.print(f"[cyan]Converting {len(cols)} columns to dates...[/cyan]")
        for col in cols:
            fmt = Prompt.ask(f"  Date format for '{col}' (blank=auto)", default="").strip()
            if fmt:
                df[col] = pd.to_datetime(df[col], format=fmt, errors='coerce')
            else:
                df[col] = pd.to_datetime(df[col], infer_datetime_format=True, errors='coerce')
            console.print(f"    [green]✔[/green] {col}")
    
    elif issue_type == "float_id_precision":
        console.print(f"[cyan]Converting {len(cols)} columns to integers...[/cyan]")
        for col in cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            console.print(f"    [green]✔[/green] {col}")
    
    elif issue_type == "high_nulls":
        console.print(f"[cyan]Dropping {len(cols)} columns with 90%+ nulls...[/cyan]")
        if Confirm.ask(f"Drop {len(cols)} columns?", default=True):
            df = df.drop(columns=cols)
            console.print(f"[green]✔ Dropped: {', '.join(cols)}[/green]")
    
    elif issue_type == "unique_values":
        console.print("[cyan]Adding _is_id flag columns...[/cyan]")
        for col in cols:
            df[f"{col}_is_id"] = True
            console.print(f"    [green]✔[/green] Created {col}_is_id flag")
    
    elif issue_type == "mixed_types":
        console.print(f"[cyan]Cleaning {len(cols)} mixed-type columns...[/cyan]")
        for col in cols:
            # Try to convert to most specific type
            df[col] = pd.to_numeric(df[col], errors='ignore')
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
            console.print(f"    [green]✔[/green] {col}")
    
    console.print(f"[green]✔ Smart fix complete.[/green]")
    show_preview(df, n=5, title="After Fix")
    return df


# ╔══════════════════════════════════════════════════════════════════╗
# ║  MULTI-CONDITION FILTER (1.2)                                      ║
# ╚══════════════════════════════════════════════════════════════════╝

def apply_single_condition(df: pd.DataFrame, col: str, op: str, val) -> pd.Series:
    """Apply a single filter condition and return boolean mask."""
    if op in ("IS NULL", "ISNULL", "NULL"):
        return df[col].isna() | (df[col].astype(str).str.strip() == "")
    
    if op in ("IS NOT NULL", "ISNOTNULL", "NOT NULL"):
        return df[col].notna() & (df[col].astype(str).str.strip() != "")
    
    if op in ("IN", "NOT IN"):
        raw_list = [v.strip().strip("'\"") for v in str(val).split(",")]
        try:
            num_list = [float(v) for v in raw_list]
            mask = df[col].isin(num_list)
        except:
            mask = df[col].astype(str).str.strip().isin(raw_list)
        return mask if op == "IN" else ~mask
    
    if op in ("CONTAINS", "~"):
        return df[col].astype(str).str.contains(val, case=False, na=False)
    
    if op == "NOT CONTAINS":
        return ~df[col].astype(str).str.contains(val, case=False, na=False)
    
    if op == "STARTSWITH":
        return df[col].astype(str).str.startswith(val, na=False)
    
    if op == "ENDSWITH":
        return df[col].astype(str).str.endswith(val, na=False)
    
    # Standard comparison operators
    try:
        num_val = float(val)
        return df.query(f"`{col}` {op} {num_val}").index.isin(df.index)
    except ValueError:
        return df.query(f"`{col}` {op} '{val}'").index.isin(df.index)


def op_multi_filter(sess: Session) -> pd.DataFrame:
    """
    Multi-condition filter - stack multiple conditions and apply together.
    Similar to Excel AutoFilter with AND/OR logic.
    """
    df = sess.df
    console.print(Rule("[bold]Multi-Condition Filter[/bold]"))
    
    conditions = []  # List of (column, operator, value, logic_type)
    
    while True:
        # Show current conditions as chips
        if conditions:
            console.print("\n[bold]Current Filter Chips:[/bold]")
            for i, (col, op, val, logic) in enumerate(conditions):
                chip = f"[cyan]{col}[/cyan] [yellow]{op}[/yellow] [white]{val}[/white]"
                if logic == "AND":
                    chip += " [green]AND[/green]"
                elif logic == "OR":
                    chip += " [magenta]OR[/magenta]"
                console.print(f"  {i+1}. {chip}  [dim](R=remove)[/dim]")
        else:
            console.print("\n[dim]No conditions added yet.[/dim]")
        
        console.print("\n[dim]Options: [bold]A[/bold]dd condition   [bold]E[/bold]xecute   [bold]C[/bold]lear all   [bold]0[/bold] back[/dim]")
        action = Prompt.ask("Action", choices=["A", "E", "C", "0", "a", "e", "c"]).upper()
        
        if action == "0":
            return df
        
        if action == "C":
            conditions.clear()
            console.print("[dim]Conditions cleared.[/dim]")
            continue
        
        if action == "E":
            if not conditions:
                console.print("[yellow]No conditions to apply.[/yellow]")
                continue
            # Execute all conditions
            mask = pd.Series([True] * len(df), index=df.index)
            
            for i, (col, op, val, logic) in enumerate(conditions):
                try:
                    cond_mask = apply_single_condition(df, col, op, val)
                    if i == 0:
                        mask = cond_mask
                    else:
                        if logic == "AND":
                            mask = mask & cond_mask
                        else:  # OR
                            mask = mask | cond_mask
                except Exception as e:
                    console.print(f"[red]Error applying condition {i+1}: {e}[/red]")
                    continue
            
            result = df[mask]
            removed = len(df) - len(result)
            console.print(f"[green]✔ {len(result):,} rows kept ({removed} removed)[/green]")
            show_preview(result, n=8, title="Filtered Result")
            
            if Confirm.ask("Apply this filter to working table?", default=True):
                return result
            return df
        
        if action == "A":
            # Add a new condition
            show_columns(df, compact=True)
            col = resolve(Prompt.ask("Column"), df)
            if not col:
                console.print("[red]Column not found.[/red]"); continue
            
            show_unique_inline(df, col)
            console.print("\n[dim]Operators: == != > < >= <= IN NOT IN CONTAINS IS NULL[/dim]")
            op = Prompt.ask("Operator").strip().upper()
            
            if op in ("IS NULL", "ISNULL", "NULL"):
                val = None
            else:
                val = Prompt.ask("Value (for IN use commas)").strip()
            
            # Determine AND/OR logic
            if conditions:
                logic = Prompt.ask("Combine with [A]ND or [O]R?", default="A").upper()
            else:
                logic = "AND"  # First condition doesn't need combinator
            
            conditions.append((col, op, val, logic))
            console.print(f"[green]✔ Added: {col} {op} {val}[/green]")
    
    return df


def op_filter_by_color(sess: Session) -> pd.DataFrame:
    """Filter by flag column (like Excel's filter by color)."""
    df = sess.df
    console.print(Rule("[bold]Filter by Flag / Color[/bold]"))
    
    # Find columns that could be flags (0/1, True/False, Yes/No)
    flag_cols = []
    for col in df.columns:
        if df[col].dtype == 'bool':
            flag_cols.append(col)
        elif df[col].dtype in ('int64', 'Int64', 'float64'):
            unique = df[col].dropna().unique()
            if set(unique).issubset({0, 1}):
                flag_cols.append(col)
        elif df[col].dtype == 'object':
            unique = df[col].dropna().str.lower().unique()
            if set(unique).issubset({'yes', 'no', 'y', 'n', 'true', 'false'}):
                flag_cols.append(col)
    
    if not flag_cols:
        console.print("[yellow]No flag columns found (0/1, Yes/No, True/False).[/yellow]")
        return df
    
    console.print("[bold]Flag columns found:[/bold]")
    for i, col in enumerate(flag_cols):
        console.print(f"  [yellow]{i+1}[/yellow]  {col}")
    
    choice = Prompt.ask("Filter by which column?", 
                        choices=[str(i+1) for i in range(len(flag_cols))])
    col = flag_cols[int(choice)-1]
    
    # Determine values to keep
    if df[col].dtype == 'bool':
        console.print(f"[dim]Values: True / False[/dim]")
        keep = Prompt.ask("Keep [T]rue or [F]alse?", default="T").upper()
        result = df[df[col] == (keep == "T")]
    elif df[col].dtype in ('int64', 'Int64', 'float64'):
        console.print(f"[dim]Values: 0 / 1[/dim]")
        keep = Prompt.ask("Keep [1] or [0]?", default="1").strip()
        result = df[df[col] == int(keep)]
    else:
        console.print(f"[dim]Values: Yes/No or similar[/dim]")
        keep = Prompt.ask("Keep [Y]es or [N]o?", default="Y").upper()
        keep_val = 'yes' if keep == 'Y' else 'no'
        result = df[df[col].str.lower() == keep_val]
    
    console.print(f"[green]✔ {len(result):,} rows kept (flag={keep})[/green]")
    show_preview(result, n=5)
    return result


# ╔══════════════════════════════════════════════════════════════════╗
# ║  FIND & REPLACE (1.3)                                              ║
# ╚══════════════════════════════════════════════════════════════════╝

def op_find_replace(sess: Session) -> pd.DataFrame:
    """
    Classic Ctrl+H Find & Replace functionality.
    Supports exact match, contains, case sensitive, preview before commit.
    """
    df = sess.df
    console.print(Rule("[bold]Find & Replace (Ctrl+H)[/bold]"))
    
    console.print("\n[bold]Scope:[/bold]")
    console.print("  [yellow]1[/yellow]  One column")
    console.print("  [yellow]2[/yellow]  All columns")
    scope = Prompt.ask("Scope", choices=["1", "2"], default="1")
    
    if scope == "1":
        show_columns(df, compact=True)
        col = resolve(Prompt.ask("Column to search in"), df)
        if not col:
            console.print("[red]Column not found.[/red]"); return df
    else:
        col = None
    
    find_str = Prompt.ask("Find what").strip()
    replace_str = Prompt.ask("Replace with").strip()
    
    console.print("\n[dim]Match options: [bold]E[/bold]xact / [bold]C[/bold]ontains / [bold]R[/bold]egex[/dim]")
    match_mode = Prompt.ask("Match mode", choices=["E", "C", "R"], default="E").upper()
    
    case_sensitive = Confirm.ask("Case sensitive?", default=False)
    
    console.print("\n[dim]Replace: [bold]F[/bold]irst only / [bold]A[/bold]ll / [bold]P[/bold]review first[/dim]")
    replace_mode = Prompt.ask("Replace mode", choices=["F", "A", "P"], default="P").upper()
    
    # Build mask based on scope and match mode
    if col:
        target_df = df[[col]]
    else:
        target_df = df
    
    if match_mode == "E":
        if case_sensitive:
            mask = target_df == find_str
        else:
            mask = target_df.astype(str).str.lower() == find_str.lower()
    elif match_mode == "C":
        if case_sensitive:
            mask = target_df.astype(str).str.contains(find_str, regex=False, na=False)
        else:
            mask = target_df.astype(str).str.contains(find_str, case=False, regex=False, na=False)
    else:  # Regex
        mask = target_df.astype(str).str.contains(find_str, case=case_sensitive, regex=True, na=False)
    
    # Count matches
    if col:
        match_count = mask[col].sum()
    else:
        match_count = mask.any(axis=1).sum()
    
    console.print(f"\n[cyan]Found {match_count:,} cells matching '{find_str}'.[/cyan]")
    
    if match_count == 0:
        return df
    
    # Show preview of what will change
    if col:
        preview_rows = df[mask[col]].head(5)
    else:
        preview_mask = mask.any(axis=1)
        preview_rows = df[preview_mask].head(5)
    
    console.print("\n[bold]Preview (first 5 changes):[/bold]")
    t = Table(box=box.SIMPLE)
    if col:
        t.add_column("Row", style="dim", width=5)
        t.add_column("Column", style="cyan", width=15)
        t.add_column("Old Value", style="red", min_width=20)
        t.add_column("New Value", style="green", min_width=20)
        for idx, row in preview_rows.iterrows():
            t.add_row(str(idx+1), col, str(row[col])[:30], replace_str)
    else:
        # Show all columns that will change
        t.add_column("Row", style="dim", width=5)
        t.add_column("Changes", style="white", min_width=40)
        for idx in preview_rows.index:
            changed_cols = [c for c in df.columns if mask.loc[idx, c]]
            change_str = ", ".join([f"{c}: {df.loc[idx, c][:15]}→{replace_str[:15]}" for c in changed_cols[:3]])
            t.add_row(str(idx+1), change_str)
    console.print(t)
    
    if replace_mode == "P":
        console.print(f"\n[yellow]{match_count} cells will change.[/yellow]")
        if Confirm.ask("Proceed with replacement?", default=True):
            replace_mode = "A"  # All
        else:
            return df
    
    if replace_mode == "F":
        # Replace first only
        if col:
            first_match_idx = mask[mask[col]].index[0]
            df.at[first_match_idx, col] = replace_str
        else:
            first_match_idx = mask.any(axis=1).idxmax()
            for c in df.columns:
                if mask.loc[first_match_idx, c]:
                    df.at[first_match_idx, c] = replace_str
                    break
        console.print(f"[green]✔ Replaced 1 cell.[/green]")
    else:  # All
        if col:
            df.loc[mask[col], col] = replace_str
        else:
            for c in df.columns:
                df.loc[mask[c], c] = replace_str
        console.print(f"[green]✔ Replaced {match_count:,} cells.[/green]")
    
    show_preview(df, n=5, title="After Replace")
    return df


# ╔══════════════════════════════════════════════════════════════════╗
# ║  FREEZE / FOCUS VIEW (1.4)                                         ║
# ╚══════════════════════════════════════════════════════════════════╝

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


# ╔══════════════════════════════════════════════════════════════════╗
# ║  ROW-LEVEL EDIT (1.5)                                              ║
# ╚══════════════════════════════════════════════════════════════════╝

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


# ╔══════════════════════════════════════════════════════════════════╗
# ║  CALCULATED COLUMNS LIBRARY (1.6)                                 ║
# ╚══════════════════════════════════════════════════════════════════╝

def op_calculated_columns(sess: Session) -> pd.DataFrame:
    """Pre-built formula templates for common calculations."""
    df = sess.df
    console.print(Rule("[bold]Calculated Columns Library[/bold]"))
    
    templates = {
        "1": ("Age from birthdate", "Calculate age in years from a date column"),
        "2": ("Days since / until", "Days between date column and today"),
        "3": ("Percent of total", "Value / column_sum * 100"),
        "4": ("Rank within group", "Rank column A within groups of column B"),
        "5": ("Running total", "Cumulative sum of a column"),
        "6": ("Month/Year/Quarter", "Extract month, year, or quarter from date"),
        "7": ("First/Last word", "Extract first or last word from text"),
        "8": ("Extract numbers", "Extract numeric values from messy strings"),
    }
    
    console.print("\n[bold]Available Templates:[/bold]")
    for num, (name, desc) in templates.items():
        console.print(f"  [yellow]{num}[/yellow]  {name} - [dim]{desc}[/dim]")
    
    choice = Prompt.ask("Choose template", choices=list(templates.keys()))
    
    new_col = Prompt.ask("Name for new column")
    if not new_col:
        console.print("[red]Column name required.[/red]")
        return df
    
    if choice == "1":  # Age from birthdate
        show_columns(df, compact=True)
        col = resolve(Prompt.ask("Date of birth column"), df)
        if not col:
            console.print("[red]Column not found.[/red]"); return df
        
        try:
            df[new_col] = (pd.Timestamp.now() - pd.to_datetime(df[col], errors='coerce')).dt.days / 365.25
            df[new_col] = df[new_col].round(1)
            console.print(f"[green]✔ Age calculated in years.[/green]")
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]"); return df
    
    elif choice == "2":  # Days since/until
        show_columns(df, compact=True)
        col = resolve(Prompt.ask("Date column"), df)
        if not col:
            console.print("[red]Column not found.[/red]"); return df
        
        direction = Prompt.ask("Direction: [S]ince (past dates positive) or [U]til (future positive)", 
                               choices=["S", "U"], default="S").upper()
        
        try:
            days = (pd.to_datetime(df[col], errors='coerce') - pd.Timestamp.now()).dt.days
            if direction == "S":
                df[new_col] = -days
            else:
                df[new_col] = days
            console.print(f"[green]✔ Days calculated.[/green]")
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]"); return df
    
    elif choice == "3":  # Percent of total
        show_columns(df, compact=True)
        col = resolve(Prompt.ask("Value column"), df)
        if not col:
            console.print("[red]Column not found.[/red]"); return df
        
        try:
            total = pd.to_numeric(df[col], errors='coerce').sum()
            if total == 0:
                console.print("[red]Total is zero, can't calculate percentage.[/red]"); return df
            df[new_col] = pd.to_numeric(df[col], errors='coerce') / total * 100
            df[new_col] = df[new_col].round(2)
            console.print(f"[green]✔ Percent of total calculated (total={total:,.2f}).[/green]")
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]"); return df
    
    elif choice == "4":  # Rank within group
        show_columns(df, compact=True)
        val_col = resolve(Prompt.ask("Value column to rank"), df)
        if not val_col:
            console.print("[red]Column not found.[/red]"); return df
        
        group_col_input = Prompt.ask("Group by column (blank = no grouping)").strip()
        
        try:
            if group_col_input:
                group_col = resolve(group_col_input, df)
                df[new_col] = df.groupby(group_col, dropna=False)[val_col].rank(method='min', ascending=False)
            else:
                df[new_col] = df[val_col].rank(method='min', ascending=False)
            console.print(f"[green]✔ Rank calculated.[/green]")
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]"); return df
    
    elif choice == "5":  # Running total
        show_columns(df, compact=True)
        col = resolve(Prompt.ask("Column to sum"), df)
        if not col:
            console.print("[red]Column not found.[/red]"); return df
        
        try:
            df[new_col] = pd.to_numeric(df[col], errors='coerce').cumsum()
            console.print(f"[green]✔ Running total calculated.[/green]")
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]"); return df
    
    elif choice == "6":  # Month/Year/Quarter
        show_columns(df, compact=True)
        col = resolve(Prompt.ask("Date column"), df)
        if not col:
            console.print("[red]Column not found.[/red]"); return df
        
        extract = Prompt.ask("Extract: [M]onth / [Y]ear / [Q]uarter", 
                            choices=["M", "Y", "Q"], default="M").upper()
        
        try:
            dt = pd.to_datetime(df[col], errors='coerce')
            if extract == "M":
                df[new_col] = dt.dt.month
            elif extract == "Y":
                df[new_col] = dt.dt.year
            else:  # Q
                df[new_col] = dt.dt.quarter
            console.print(f"[green]✔ Extracted {extract}.[/green]")
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]"); return df
    
    elif choice == "7":  # First/Last word
        show_columns(df, compact=True)
        col = resolve(Prompt.ask("Text column"), df)
        if not col:
            console.print("[red]Column not found.[/red]"); return df
        
        which = Prompt.ask("Extract: [F]irst word / [L]ast word", 
                          choices=["F", "L"], default="F").upper()
        
        try:
            if which == "F":
                df[new_col] = df[col].astype(str).str.split().str[0]
            else:
                df[new_col] = df[col].astype(str).str.split().str[-1]
            console.print(f"[green]✔ Extracted {'first' if which == 'F' else 'last'} word.[/green]")
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]"); return df
    
    elif choice == "8":  # Extract numbers
        show_columns(df, compact=True)
        col = resolve(Prompt.ask("Column to extract numbers from"), df)
        if not col:
            console.print("[red]Column not found.[/red]"); return df
        
        try:
            df[new_col] = df[col].astype(str).str.extract(r'(\d+)', expand=False)
            console.print(f"[green]✔ Numbers extracted.[/green]")
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]"); return df
    
    show_preview(df, n=5, title=f"After: {new_col}")
    return df


# ── [1] FILTER ────────────────────────────────────────────────────────────────

def op_filter(sess: Session) -> pd.DataFrame:
    df = sess.df
    console.print(Rule("[bold]Filter / Keep Rows[/bold]"))
    show_columns(df, compact=True)

    col = resolve(Prompt.ask("Column to filter (letter or name)"), df)
    if not col:
        console.print("[red]Column not found.[/red]"); return df

    # Always show inline uniques — user can see exactly what to type
    console.print()
    show_unique_inline(df, col)

    console.print(
        "\n[dim]Operators: [bold]== != > < >= <=[/bold]  "
        "[bold]IN  NOT IN  CONTAINS  NOT CONTAINS  STARTSWITH  ENDSWITH  IS NULL  IS NOT NULL[/bold][/dim]"
    )
    op = Prompt.ask("Operator").strip().upper()

    if op in ("IS NULL","ISNULL","NULL"):
        new_df = df[df[col].isna() | (df[col].astype(str).str.strip()=="")]
        console.print(f"[green]✔ {len(new_df):,} rows where {col} IS NULL[/green]")
        show_preview(new_df, n=5); return new_df

    if op in ("IS NOT NULL","ISNOTNULL","NOT NULL"):
        new_df = df[df[col].notna() & (df[col].astype(str).str.strip()!="")]
        console.print(f"[green]✔ {len(new_df):,} rows where {col} IS NOT NULL[/green]")
        show_preview(new_df, n=5); return new_df

    val_raw = Prompt.ask("Value(s)  [for IN/NOT IN use commas]").strip()

    try:
        if op in ("IN","NOT IN"):
            raw_list      = [v.strip().strip("'\"") for v in val_raw.split(",")]
            resolved_list = fuzzy_pick_values_list(raw_list, df, col)
            if not resolved_list:
                console.print("[red]No values resolved.[/red]"); return df
            try:
                num_list = [float(v) for v in resolved_list]
                mask = df[col].isin(num_list)
            except ValueError:
                mask = df[col].astype(str).str.strip().isin(resolved_list)
            new_df = df[mask] if op=="IN" else df[~mask]

        elif op in ("CONTAINS","~"):
            new_df = df[df[col].astype(str).str.contains(val_raw, case=False, na=False)]

        elif op in ("NOT CONTAINS","!~"):
            new_df = df[~df[col].astype(str).str.contains(val_raw, case=False, na=False)]

        elif op == "STARTSWITH":
            new_df = df[df[col].astype(str).str.startswith(val_raw, na=False)]

        elif op == "ENDSWITH":
            new_df = df[df[col].astype(str).str.endswith(val_raw, na=False)]

        else:
            actual = val_raw
            if op in ("==","!="):
                actual = fuzzy_pick_value(val_raw, df, col) or val_raw
            try:
                num_v  = float(actual)
                new_df = df.query(f"`{col}` {op} {num_v}")
            except ValueError:
                new_df = df.query(f"`{col}` {op} '{actual}'")

        removed = len(df)-len(new_df)
        console.print(
            f"[green]✔ {len(new_df):,} rows kept ({removed} removed)[/green]"
        )
        show_preview(new_df, n=5, title="After Filter")
        return new_df

    except Exception as e:
        console.print(f"[red]❌ Filter error: {e}[/red]")
        return df


# ── [2] ADD COLUMN ────────────────────────────────────────────────────────────

def op_add_column(sess: Session) -> pd.DataFrame:
    df = sess.df
    console.print(Rule("[bold]Add New Column[/bold]"))
    console.print(Panel(
        "  [bold]1[/bold]  [cyan]Formula[/cyan]     Math / Logic  [dim](e.g. `Price` * `Qty`)[/dim]\n"
        "  [bold]2[/bold]  [cyan]Conditional[/cyan] IF / THEN / ELSE logic\n"
        "  [bold]3[/bold]  [cyan]Pipeline[/cyan]    Build step-by-step\n"
        "  [bold]4[/bold]  [cyan]Map Values[/cyan]  Remap specific values",
        title="Builder Mode", border_style="green"
    ))
    mode = Prompt.ask("Choose tool", choices=["1","2","3","4"], default="1")
    new_col = Prompt.ask("Name for new column")

    # ── 1. FORMULA ────────────────────────────────────────────────────────────
    if mode == "1":
        console.print("\n[dim]Examples: [cyan]`Price` * `Qty`[/cyan]   "
                      "[cyan]`First` + ' ' + `Last`[/cyan][/dim]")
        show_columns(df, compact=True)
        formula = Prompt.ask("Formula")
        try:
            df[new_col] = df.eval(formula)
            console.print(f"[green]✔ '{new_col}' calculated.[/green]")
        except Exception as e:
            console.print(f"[red]❌ {e}[/red]")

    # ── 2. CONDITIONAL (IF/ELSE) ──────────────────────────────────────────────
    elif mode == "2":
        show_columns(df, compact=True)
        console.print("\n[bold]Logic Wizard:[/bold] [dim]IF (condition) THEN (true_val) ELSE (false_val)[/dim]")
        
        cond = Prompt.ask("  IF Condition")
        
        # Preview condition hits
        try:
            hits = df.eval(cond).sum()
            console.print(f"  [dim]↳ Matches {hits:,} rows[/dim]")
        except:
            console.print("[yellow]⚠ Invalid condition syntax (check backticks?)[/yellow]")

        true_v  = Prompt.ask("  THEN Value")
        false_v = Prompt.ask("  ELSE Value")

        try:
            # Helper to check if input looks like a column name wrapped in backticks
            def parse_val(v):
                v = v.strip()
                if v.startswith("`") and v.endswith("`") and v[1:-1] in df.columns:
                    return df[v[1:-1]]
                try: return float(v)
                except: return v

            # Apply np.where
            # We use df.eval for the condition mask
            mask = df.eval(cond)
            
            # For values, we need to handle mixed types (scalars vs series) manually or via eval
            # Simplest approach: use eval for everything to allow column math in result
            # But eval requires quotes for strings. Let's try to be smart.
            
            # Construct a full eval string if possible: np.where(cond, val_if_true, val_if_false)
            # But quoting is tricky. Let's use Python engine in eval for complex logic.
            df[new_col] = np.where(mask, parse_val(true_v), parse_val(false_v))
            console.print(f"[green]✔ '{new_col}' created based on condition.[/green]")
            
        except Exception as e:
            console.print(f"[red]❌ Logic error: {e}[/red]")

    # ── 4. MAP (VLOOKUP-ish) ──────────────────────────────────────────────────
    elif mode == "4":
        show_columns(df, compact=True)
        src_col = resolve(Prompt.ask("Source column to map from"), df)
        if not src_col:
            console.print("[red]Column not found.[/red]"); return df
        show_unique_inline(df, src_col)

        mappings: dict = {}
        console.print("\n[dim]Enter pairs. Blank 'From' to finish.[/dim]")
        while True:
            from_v = Prompt.ask("  From (blank to finish)").strip()
            if not from_v: break
            from_v = fuzzy_pick_value(from_v, df, src_col) or from_v
            to_v   = Prompt.ask("  → To")
            mappings[from_v] = to_v

        default = Prompt.ask("Default for unmapped (blank = null)").strip()
        df[new_col] = df[src_col].astype(str).map(mappings)
        if default: df[new_col] = df[new_col].fillna(default)
        console.print(f"[green]✔ '{new_col}' — {df[new_col].notna().sum():,} values mapped.[/green]")

    # ── 3. PIPELINE (BLOCKS) ──────────────────────────────────────────────────
    elif mode == "3":
        show_columns(df, compact=True)
        src_col = resolve(Prompt.ask("Start with column"), df)
        if not src_col:
            console.print("[red]Column not found.[/red]"); return df
        
        # Initialize working series
        s = df[src_col].copy()
        history = [src_col]

        while True:
            console.print(f"\n[bold]Current Pipeline:[/bold] [cyan]{' → '.join(history)}[/cyan]")
            console.print(
                "[dim]Blocks: [bold]upper lower strip title len[/bold] (text) "
                "[bold]int float clean round[/bold] (num) "
                "[bold]date[/bold] (time) [bold]extract[/bold] (regex)\n"
                "        [bold]fill[/bold] (nulls) [bold]done[/bold] (finish)[/dim]"
            )
            op = Prompt.ask("Add Block").strip().lower()
            
            if op == "done": break
            
            try:
                if   op == "upper":   s = s.astype(str).str.upper()
                elif op == "lower":   s = s.astype(str).str.lower()
                elif op == "strip":   s = s.astype(str).str.strip()
                elif op == "title":   s = s.astype(str).str.title()
                
                elif op == "int":     s = pd.to_numeric(s, errors="coerce").astype("Int64")
                elif op == "float":   s = pd.to_numeric(s, errors="coerce")
                elif op == "clean":   s = clean_number_string(s)
                elif op == "len":     s = s.astype(str).str.len()
                
                elif op == "round":
                    p = int(Prompt.ask("  Decimal places", default="2"))
                    s = pd.to_numeric(s, errors="coerce").round(p)
                
                elif op == "date":    s = pd.to_datetime(s, errors="coerce")
                
                elif op == "fill":
                    val = Prompt.ask("  Fill nulls with")
                    s = s.fillna(val)
                
                elif op == "extract":
                    pat = Prompt.ask("  Regex pattern (e.g. `(\d+)`)")
                    s = s.astype(str).str.extract(pat, expand=False)
                
                history.append(op)
            except Exception as e:
                console.print(f"[red]❌ Block failed: {e}[/red]")

        df[new_col] = s
        console.print(f"[green]✔ '{new_col}' created via pipeline.[/green]")
        
    show_preview(df, n=5); return df


# ── [3] AGGREGATE ─────────────────────────────────────────────────────────────

def op_aggregate(sess: Session):
    df = sess.df
    console.print(Rule("[bold]Count-If / Sum-If / Aggregations[/bold]"))
    show_columns(df, compact=True)

    group_in  = Prompt.ask("Group by column (blank = whole table)").strip()
    group_col = resolve(group_in, df) if group_in else None

    console.print("[dim]Aggregations: [bold]count countunique sum mean median min max std[/bold][/dim]")
    agg = Prompt.ask("Aggregation", default="count").strip().lower()

    try:
        if agg in ("count","countunique"):
            if group_col:
                if agg == "countunique":
                    vc_col = resolve(Prompt.ask("Column to count unique of"), df)
                    result = df.groupby(group_col, dropna=False)[vc_col].nunique().reset_index(name="Unique_Count")
                else:
                    result = df.groupby(group_col, dropna=False).size().reset_index(name="Count")
                    result = result.sort_values("Count", ascending=False)
            else:
                console.print(f"[cyan]Total: [bold]{len(df):,}[/bold][/cyan]"); return
        else:
            val_col = resolve(Prompt.ask("Column to aggregate"), df)
            if not val_col:
                console.print("[red]Column not found.[/red]"); return
            fn_map  = {"sum":"sum","mean":"mean","median":"median",
                       "min":"min","max":"max","std":"std"}
            fn = fn_map.get(agg, "sum")
            if group_col:
                result = df.groupby(group_col, dropna=False)[val_col].agg(fn).reset_index()
                result.columns = [group_col, f"{fn}_{val_col}"]
                result = result.sort_values(result.columns[-1], ascending=False)
            else:
                v = getattr(df[val_col].dropna(), fn)()
                console.print(f"[cyan]{agg} of [bold]{val_col}[/bold]: [bold]{v:,.4g}[/bold][/cyan]"); return

        t = Table(box=box.ROUNDED, show_lines=True)
        for c in result.columns: t.add_column(str(c))
        for _, row in result.iterrows():
            t.add_row(*[fmt_val(v) for v in row])
        console.print(t)

        if Confirm.ask("Save as a new working table?", default=False):
            tname = Prompt.ask("Table name", default="agg_result")
            sess.add(tname, result)
            console.print(f"[green]✔ '{tname}' added to session.[/green]")

    except Exception as e:
        console.print(f"[red]❌ Aggregation error: {e}[/red]")


# ── [4] SORT ──────────────────────────────────────────────────────────────────

def op_sort(sess: Session) -> pd.DataFrame:
    df = sess.df
    console.print(Rule("[bold]Sort[/bold]"))
    show_columns(df, compact=True)
    cols = ask_cols("Column(s) to sort by (comma-sep)", df)
    if not cols: return df
    asc  = Prompt.ask("Order [A]scending / [D]escending", default="A").upper() != "D"
    df   = df.sort_values(by=cols, ascending=asc)
    console.print(f"[green]✔ Sorted by {', '.join(cols)} ({'↑ A→Z' if asc else '↓ Z→A'}).[/green]")
    show_preview(df, n=5, title="After Sort")
    return df


# ── [5] NULLS ─────────────────────────────────────────────────────────────────

def op_handle_nulls(sess: Session) -> pd.DataFrame:
    df = sess.df
    console.print(Rule("[bold]Handle Nulls / Missing Values[/bold]"))
    show_null_report(df)
    console.print(Panel(
        "  [bold]1[/bold]  Drop rows where column IS NULL\n"
        "  [bold]2[/bold]  Fill nulls with a constant value\n"
        "  [bold]3[/bold]  Fill nulls with mean / median / mode\n"
        "  [bold]4[/bold]  Replace blank strings → null\n"
        "  [bold]5[/bold]  Drop columns that are ALL null\n"
        "  [bold]6[/bold]  Forward-fill (carry last known value)\n"
        "  [bold]7[/bold]  Create is_null flag column",
        title="Null Actions", border_style="yellow"
    ))
    action = Prompt.ask("Action", choices=["1","2","3","4","5","6","7"])

    if action != "5":
        show_columns(df, compact=True)
        ci   = Prompt.ask("Column (letter/name, or ALL)")
        cols = list(df.columns) if ci.strip().upper()=="ALL" else [resolve(ci, df)]
        cols = [c for c in cols if c and c in df.columns]
        if not cols:
            console.print("[red]No valid columns.[/red]"); return df

    if action == "1":
        before = len(df); df = df.dropna(subset=cols)
        console.print(f"[green]✔ Dropped {before-len(df):,} rows.[/green]")

    elif action == "2":
        fv = Prompt.ask("Fill with")
        try: fv = int(fv) if "." not in fv else float(fv)
        except: pass
        for c in cols: df[c] = df[c].fillna(fv)
        console.print(f"[green]✔ Filled with '{fv}'.[/green]")

    elif action == "3":
        meth = Prompt.ask("Method", choices=["mean","median","mode"])
        for c in cols:
            if   meth == "mean":   fv = pd.to_numeric(df[c], errors="coerce").mean()
            elif meth == "median": fv = pd.to_numeric(df[c], errors="coerce").median()
            else:
                mode_result = df[c].mode()
                fv = mode_result.iloc[0] if not mode_result.empty else None
            df[c] = df[c].fillna(fv)
            console.print(f"  [cyan]{c}[/cyan] → {meth} = [bold]{fv}[/bold]")

    elif action == "4":
        for c in cols:
            if df[c].dtype == object:
                df[c] = df[c].replace(r"^\s*$", np.nan, regex=True)
        console.print("[green]✔ Blank strings → null.[/green]")

    elif action == "5":
        all_null = [c for c in df.columns if df[c].isna().all()]
        if all_null:
            df = df.drop(columns=all_null)
            console.print(f"[green]✔ Dropped: {', '.join(all_null)}[/green]")
        else:
            console.print("[dim]No fully-null columns.[/dim]")

    elif action == "6":
        for c in cols: df[c] = df[c].ffill()
        console.print("[green]✔ Forward-filled.[/green]")

    elif action == "7":
        for c in cols:
            fc = f"{c}_is_null"; df[fc] = df[c].isna().astype(int)
            console.print(f"  Created [cyan]{fc}[/cyan]")

    return df


# ── [6] RENAME / DROP ─────────────────────────────────────────────────────────

def op_rename_drop(sess: Session) -> pd.DataFrame:
    df = sess.df
    console.print(Rule("[bold]Rename / Drop Columns[/bold]"))
    show_columns(df, compact=True)
    console.print("  [bold]R[/bold] Rename   [bold]D[/bold] Drop")
    action = Prompt.ask("Action", choices=["R","D","r","d"]).upper()

    if action == "R":
        col = resolve(Prompt.ask("Column to rename"), df)
        if not col:
            console.print("[red]Not found.[/red]"); return df
        new_name = Prompt.ask(f"New name for '{col}'")
        df = df.rename(columns={col: new_name})
        console.print(f"[green]✔ '{col}' → '{new_name}'[/green]")

    elif action == "D":
        cols = ask_cols("Column(s) to drop (comma-sep)", df)
        if not cols: return df
        if Confirm.ask(f"Drop {cols}?", default=True):
            df = df.drop(columns=cols)
            console.print(f"[green]✔ Dropped: {', '.join(cols)}[/green]")
    return df


# ── [7] DEDUPE ────────────────────────────────────────────────────────────────

def op_dedupe(sess: Session) -> pd.DataFrame:
    df = sess.df
    console.print(Rule("[bold]Remove Duplicates[/bold]"))
    show_columns(df, compact=True)

    ci     = Prompt.ask("Columns to check (blank = ALL, comma-sep)").strip()
    subset = ask_cols(ci, df) if ci else None
    keep   = {"F":"first","L":"last","N":False}.get(
        Prompt.ask("Keep [F]irst / [L]ast / [N]one", default="F").upper(), "first"
    )
    before = len(df)
    df     = df.drop_duplicates(subset=subset, keep=keep)
    console.print(f"[green]✔ Removed {before-len(df):,} duplicates. {len(df):,} remain.[/green]")
    return df


# ── [8] PIVOT ─────────────────────────────────────────────────────────────────

def op_pivot(sess: Session):
    df = sess.df
    console.print(Rule("[bold]Pivot / Group Summary[/bold]"))
    show_columns(df, compact=True)

    row_cols = ask_cols("Row groups (comma-sep)", df)
    ci       = Prompt.ask("Pivot column as headers (blank to skip)").strip()
    pivot_col = resolve(ci, df) if ci else None
    val_col  = resolve(Prompt.ask("Value column"), df)
    agg_fn   = Prompt.ask("Aggregation (sum/mean/count/median)", default="sum")

    try:
        if pivot_col:
            result = df.pivot_table(index=row_cols, columns=pivot_col,
                                    values=val_col, aggfunc=agg_fn,
                                    fill_value=0).reset_index()
        else:
            result = df.groupby(row_cols)[val_col].agg(agg_fn).reset_index()

        t = Table(box=box.ROUNDED, show_lines=True)
        for c in result.columns: t.add_column(str(c), justify="right")
        for _, row in result.head(40).iterrows():
            t.add_row(*[f"{v:,.2f}" if isinstance(v, float) else str(v) for v in row])
        console.print(t)

        if Confirm.ask("Save as new working table?", default=False):
            tname = Prompt.ask("Name", default="pivot_result")
            sess.add(tname, result)
            console.print(f"[green]✔ '{tname}' added.[/green]")
    except Exception as e:
        console.print(f"[red]❌ Pivot error: {e}[/red]")


# ── [9] CHANGE COLUMN TYPE ────────────────────────────────────────────────────

def op_change_type(sess: Session) -> pd.DataFrame:
    df = sess.df
    console.print(Rule("[bold]Change Column Type[/bold]"))
    show_columns(df)   # full view so user sees current types

    col = resolve(Prompt.ask("Column to retype (letter or name)"), df)
    if not col:
        console.print("[red]Column not found.[/red]"); return df

    current = str(df[col].dtype)
    sample  = df[col].dropna().head(5).tolist()
    console.print(f"\n  Current type : [yellow]{current}[/yellow]")
    console.print(f"  Sample values: [dim]{sample}[/dim]")
    console.print(
        "\n[dim]Target types: "
        "[bold]str  int  float  bool  date  datetime  category[/bold][/dim]"
    )
    target = Prompt.ask("Convert to").strip().lower()

    try:
        if target == "str":
            # Ensure we don't turn NaN into "nan" string unless asked
            # Preserve real nulls
            df[col] = df[col].astype(str)
            df[col] = df[col].replace({"nan": np.nan, "<NA>": np.nan,
                                       "None": np.nan, "NaT": np.nan})

        elif target == "int":
            # Use smart clean first to handle "1,000" or "$50"
            converted = clean_number_string(df[col])
            n_fail = int(converted.isna().sum() - df[col].isna().sum())
            if n_fail > 0:
                console.print(f"[yellow]⚠ {n_fail:,} values can't convert → will become null[/yellow]")
                if not Confirm.ask("Proceed with nulls?", default=True): return df
            df[col] = converted.astype("Int64")     # nullable integer

        elif target == "float":
            converted = pd.to_numeric(df[col], errors="coerce")
            n_fail = int(converted.isna().sum() - df[col].isna().sum())
            if n_fail > 0:
                console.print(f"[yellow]⚠ {n_fail:,} values can't convert → null[/yellow]")
                if not Confirm.ask("Proceed?", default=True): return df
            df[col] = converted

        elif target == "bool":
            tv = {"true","1","yes","y","t"}
            fv = {"false","0","no","n","f"}
            df[col] = df[col].astype(str).str.lower().map(
                lambda x: True if x in tv else (False if x in fv else np.nan)
            )

        elif target in ("date","datetime"):
            fmt = Prompt.ask(
                "Date format (blank = auto-detect, e.g. %Y-%m-%d or %d/%m/%Y)"
            ).strip()
            if fmt:
                df[col] = pd.to_datetime(df[col], format=fmt, errors="coerce")
            else:
                df[col] = pd.to_datetime(df[col], infer_datetime_format=True,
                                         errors="coerce")
            n_fail = int(df[col].isna().sum())
            if n_fail > 0:
                console.print(f"[yellow]⚠ {n_fail:,} values failed to parse → null[/yellow]")

        elif target == "category":
            df[col] = df[col].astype("category")
            cats    = list(df[col].cat.categories[:10])
            console.print(f"  Categories: [dim]{cats}[/dim]")

        else:
            console.print(f"[red]Unknown type '{target}'.[/red]"); return df

        new_type = str(df[col].dtype)
        console.print(
            f"[green]✔ '{col}': [yellow]{current}[/yellow] → [cyan]{new_type}[/cyan][/green]"
        )

    except Exception as e:
        console.print(f"[red]❌ Type conversion error: {e}[/red]")

    return df


# ── [J] JOIN / MERGE ──────────────────────────────────────────────────────────

def op_join(sess: Session) -> pd.DataFrame:
    df = sess.df
    console.print(Rule("[bold]VLOOKUP / Join Tables[/bold]"))
    tables = sess.list_tables()

    if len(tables) < 2:
        console.print("[yellow]Need 2+ tables in session. Use [bold]T[/bold] to load more.[/yellow]")
        return df

    for i, n in enumerate(tables):
        mark = " ← active" if n == sess.active else ""
        console.print(f"  [yellow]{i+1}[/yellow]  {n}  ({len(sess.tables[n]):,}r × {len(sess.tables[n].columns)}c){mark}")

    right_n = Prompt.ask("Join with table #")
    try:    right_df = sess.tables[tables[int(right_n)-1]]
    except: console.print("[red]Invalid.[/red]"); return df

    right_name = tables[int(right_n)-1]
    console.print(f"\n[bold]Left:[/bold] {sess.active}  [bold]Right:[/bold] {right_name}")
    show_columns(df, compact=True)
    left_key  = resolve(Prompt.ask("Left  join key"), df)
    console.print()
    show_columns(right_df, compact=True)
    right_key = resolve(Prompt.ask("Right join key"), right_df)

    how = Prompt.ask("Join type [inner / left / right / outer]", default="left")

    try:
        merged = pd.merge(df, right_df, left_on=left_key, right_on=right_key,
                          how=how, suffixes=("","_right"))
        
        console.print(f"[green]✔ Matched and Merged.[/green]")

        # VLOOKUP-style: Ask which columns to keep immediately
        if Confirm.ask("Pick columns to keep now? (VLOOKUP style)", default=True):
            # Show columns that came from right table
            r_cols = [c for c in right_df.columns if c != right_key]
            console.print(f"[dim]Available from {right_name}: {', '.join(r_cols)}[/dim]")
            keep_cols = ask_cols("Columns to add to left table (comma-sep)", right_df)
            
            # Rename logic to avoid collision/suffix mess
            final_cols = list(df.columns)
            for c in keep_cols:
                final_cols.append(f"{c}_right" if c in df.columns else c)
            merged = merged[final_cols]

        new_name = Prompt.ask("Save merged table as", default=f"{sess.active}_joined")
        sess.add(new_name, merged)
        console.print(f"[green]✔ Saved as '{new_name}'.[/green]")
        return merged
    except Exception as e:
        console.print(f"[red]❌ Merge error: {e}[/red]")
        return df


# ── [S] SEARCH ────────────────────────────────────────────────────────────────

def op_search(sess: Session):
    df = sess.df
    console.print(Rule("[bold]Search[/bold]"))
    console.print("  [bold]1[/bold]  Full-text search across all columns")
    console.print("  [bold]2[/bold]  Fuzzy search within one column")
    mode = Prompt.ask("Mode", choices=["1","2"], default="1")

    if mode == "1":
        term = Prompt.ask("Search term")
        mask = pd.Series(False, index=df.index)
        for col in df.columns:
            mask |= df[col].astype(str).str.contains(term, case=False, na=False)
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


# ── [I] STATS ─────────────────────────────────────────────────────────────────

def op_stats(sess: Session):
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


# ── [T] TABLE MANAGER ─────────────────────────────────────────────────────────

def op_table_manager(sess: Session):
    while True:
        console.print(Rule("[bold]Table Manager[/bold]"))
        t = Table(title="Session Tables", box=box.ROUNDED, title_style="bold cyan")
        t.add_column("●",     width=3, justify="center")
        t.add_column("Name",  style="bold white", min_width=20)
        t.add_column("Rows",  justify="right", style="cyan")
        t.add_column("Cols",  justify="right", style="magenta")
        t.add_column("Columns (preview)", style="dim", min_width=30)

        for name, tdf in sess.tables.items():
            marker   = "[bold green]●[/bold green]" if name == sess.active else ""
            col_prev = ", ".join(tdf.columns[:6].tolist())
            if len(tdf.columns) > 6: col_prev += f" … +{len(tdf.columns)-6}"
            t.add_row(marker, name, f"{len(tdf):,}", str(len(tdf.columns)), col_prev)
        console.print(t)

        console.print(Panel(
            "  [bold]1[/bold]  Load existing table from DB into session\n"
            "  [bold]2[/bold]  Clone active table (in-session copy)\n"
            "  [bold]3[/bold]  Slim — keep only chosen columns → new table\n"
            "  [bold]4[/bold]  Stack / Append two tables vertically\n"
            "  [bold]5[/bold]  Delete a session table\n"
            "  [bold]6[/bold]  Preview a table\n"
            "  [bold]7[/bold]  Create BRAND NEW empty table → save to DB\n"
            "  [bold]8[/bold]  Save any session table → DB (create/overwrite)\n"
            "  [bold]9[/bold]  List all tables currently in DB\n"
            "  [bold]X[/bold]  Import Excel (.xlsx) → Session\n"
            "  [bold]0[/bold]  Back",
            title="Actions", border_style="dim"
        ))
        action = Prompt.ask("Action", choices=["0","1","2","3","4","5","6","7","8","9","X","x"]).upper()

        if action == "0": break

        elif action == "1":
            tbls = db_tables(sess.db_path)
            if not tbls:
                console.print("[red]No tables in DB.[/red]"); continue
            for i, n in enumerate(tbls): console.print(f"  [yellow]{i+1}[/yellow]  {n}")
            c = Prompt.ask("Pick table")
            try:    tname = tbls[int(c)-1]
            except: tname = c
            try:
                new_df = db_load(sess.db_path, tname)
                new_df.columns = [col.strip() for col in new_df.columns]
                alias = Prompt.ask("Name in session", default=tname)
                sess.add(alias, new_df)
                console.print(f"[green]✔ '{alias}' loaded ({len(new_df):,} rows × {len(new_df.columns)} cols).[/green]")
                show_columns(new_df, compact=True)
            except Exception as e:
                console.print(f"[red]❌ Load error: {e}[/red]")

        elif action == "2":
            clone_name = Prompt.ask("Clone name", default=f"{sess.active}_clone")
            sess.add(clone_name, sess.df.copy())
            console.print(f"[green]✔ Cloned as '{clone_name}'.[/green]")

        elif action == "3":
            # Pick source table
            all_t = sess.list_tables()
            for i, n in enumerate(all_t): console.print(f"  [yellow]{i+1}[/yellow]  {n}")
            c = Prompt.ask("Source table #", default=str(all_t.index(sess.active)+1))
            try:    src_name = all_t[int(c)-1]
            except: src_name = sess.active
            src_df = sess.tables[src_name]

            console.print(f"\n[bold]{src_name}[/bold] has {len(src_df.columns)} columns:")
            show_columns(src_df)
            cols = ask_cols("Pick columns to keep (comma-sep letters or names)", src_df)
            if not cols:
                console.print("[red]No valid columns.[/red]"); continue
            slim_name = Prompt.ask("New table name", default=f"{src_name}_slim")
            sess.add(slim_name, src_df[cols].copy())
            console.print(f"[green]✔ '{slim_name}' created with {len(cols)} columns from {src_name}.[/green]")
            show_preview(sess.tables[slim_name], n=4, title=slim_name)

        elif action == "4":
            all_t = sess.list_tables()
            for i, n in enumerate(all_t): console.print(f"  [yellow]{i+1}[/yellow]  {n}  ({len(sess.tables[n]):,}r)")
            a = Prompt.ask("First table #"); b = Prompt.ask("Second table #")
            try:
                t1 = sess.tables[all_t[int(a)-1]]
                t2 = sess.tables[all_t[int(b)-1]]
                # Warn if column mismatch
                missing_in_t2 = set(t1.columns) - set(t2.columns)
                missing_in_t1 = set(t2.columns) - set(t1.columns)
                if missing_in_t2:
                    console.print(f"[yellow]⚠ Cols in table1 not in table2 → will be null: {missing_in_t2}[/yellow]")
                if missing_in_t1:
                    console.print(f"[yellow]⚠ Cols in table2 not in table1 → will be null: {missing_in_t1}[/yellow]")
                stacked = pd.concat([t1, t2], ignore_index=True)
                sname = Prompt.ask("Name for stacked table", default="stacked")
                sess.add(sname, stacked)
                console.print(f"[green]✔ {len(t1):,} + {len(t2):,} = {len(stacked):,} rows → '{sname}'.[/green]")
            except (ValueError, IndexError):
                console.print("[red]Invalid selection.[/red]")
            except Exception as e:
                console.print(f"[red]❌ Stack error: {e}[/red]")

        elif action == "5":
            all_t = [n for n in sess.list_tables() if n != sess.active]
            if not all_t:
                console.print("[yellow]Can't delete the active table. Switch first.[/yellow]"); continue
            for i, n in enumerate(all_t): console.print(f"  [yellow]{i+1}[/yellow]  {n}")
            c = Prompt.ask("Delete table #")
            try:
                to_del = all_t[int(c)-1]
                if Confirm.ask(f"Delete '{to_del}' from session?", default=False):
                    del sess.tables[to_del]; del sess.history[to_del]
                    console.print(f"[green]✔ '{to_del}' removed from session. (DB not affected)[/green]")
            except (ValueError, IndexError):
                console.print("[red]Invalid.[/red]")

        elif action == "6":
            all_t = sess.list_tables()
            for i, n in enumerate(all_t): console.print(f"  [yellow]{i+1}[/yellow]  {n}")
            c = Prompt.ask("Preview table #")
            try:
                tname = all_t[int(c)-1]
                show_columns(sess.tables[tname], compact=True)
                show_preview(sess.tables[tname], n=8, title=tname)
            except (ValueError, IndexError):
                console.print("[red]Invalid.[/red]")

        elif action == "7":
            # ── Create a brand new empty table and save to DB ──────────────
            console.print(Rule("[bold]Create New Table in DB[/bold]"))
            console.print(
                "[dim]Define column names and types. This creates an empty table "
                "in your DB that you can populate later.[/dim]\n"
            )
            new_tname = Prompt.ask("New table name (as it will appear in DB)")

            console.print("\n[dim]Types: [bold]str  int  float  bool  date[/bold][/dim]")
            columns_spec: list = []
            console.print("[dim]Add columns one by one. Blank name to finish.[/dim]")
            while True:
                cname = Prompt.ask("  Column name (blank to finish)").strip()
                if not cname: break
                ctype = Prompt.ask(f"  '{cname}' type", default="str").strip().lower()
                columns_spec.append((cname, ctype))

            if not columns_spec:
                console.print("[red]No columns defined — cancelled.[/red]"); continue

            # Show summary
            t = Table(title=f"New table: {new_tname}", box=box.ROUNDED)
            t.add_column("Column", style="white")
            t.add_column("Type",   style="cyan")
            for cname, ctype in columns_spec:
                t.add_row(cname, ctype)
            console.print(t)

            if not Confirm.ask("Create this table?", default=True): continue

            # Build empty DataFrame with correct dtypes
            dtype_map = {
                "str": "object", "int": "Int64", "float": "float64",
                "bool": "boolean", "date": "datetime64[ns]",
            }
            empty_df = pd.DataFrame({
                cname: pd.Series(dtype=dtype_map.get(ctype, "object"))
                for cname, ctype in columns_spec
            })

            # Optionally pre-fill with some rows
            n_rows = Prompt.ask(
                "Pre-fill with N empty rows? (0 = empty table)", default="0"
            ).strip()
            try:
                n_rows = int(n_rows)
                if n_rows > 0:
                    empty_df = pd.DataFrame(
                        {cname: [None]*n_rows for cname, _ in columns_spec}
                    )
            except ValueError:
                pass

            # Save to DB
            out_db = Prompt.ask("Save to DB path", default=sess.db_path)
            try:
                db_save(empty_df, out_db, new_tname, if_exists="fail" if
                    Confirm.ask("Fail if table already exists?", default=True) else "replace")
                # Also add to session
                sess.add(new_tname, empty_df)
                console.print(
                    f"[green]✔ Table '[bold]{new_tname}[/bold]' created in DB and added to session.[/green]"
                )
            except Exception as e:
                console.print(f"[red]❌ Error: {e}[/red]")

        elif action == "8":
            # ── Save any session table → DB (explicit create/overwrite) ──
            console.print(Rule("[bold]Save Session Table → DB[/bold]"))
            all_t = sess.list_tables()
            for i, n in enumerate(all_t):
                console.print(f"  [yellow]{i+1}[/yellow]  {n}  ({len(sess.tables[n]):,}r × {len(sess.tables[n].columns)}c)")
            c = Prompt.ask("Which table to save?", default=str(all_t.index(sess.active)+1))
            try:    save_tname = all_t[int(c)-1]
            except: save_tname = sess.active

            out_db    = Prompt.ask("Save to DB path", default=sess.db_path)
            out_tname = Prompt.ask("Table name in DB", default=save_tname)

            # Check if table already exists in DB
            existing = db_tables(out_db) if os.path.exists(out_db) else []
            if out_tname in existing:
                console.print(f"[yellow]⚠ Table '{out_tname}' already exists in {out_db}.[/yellow]")
                action2 = Prompt.ask(
                    "  [R]eplace  [A]ppend  [C]ancel",
                    choices=["R","A","C","r","a","c"], default="R"
                ).upper()
                if action2 == "C": continue
                if_exists = "replace" if action2 == "R" else "append"
            else:
                if_exists = "replace"

            try:
                db_save(sess.tables[save_tname], out_db, out_tname, if_exists=if_exists)
                console.print(f"[dim]Source DB '{sess.db_path}' was NOT modified.[/dim]")
            except Exception as e:
                console.print(f"[red]❌ Save error: {e}[/red]")

        elif action == "9":
            # ── List all tables in DB ──────────────────────────────────────
            console.print(Rule("[bold]Tables in DB[/bold]"))
            tbls = db_tables(sess.db_path)
            t = Table(title=f"DB: {sess.db_path}", box=box.ROUNDED)
            t.add_column("#",     style="yellow", width=5, justify="center")
            t.add_column("Table", style="white",  min_width=25)
            t.add_column("In Session?", style="cyan", justify="center")
            for i, n in enumerate(tbls):
                in_sess = "[green]✔[/green]" if n in sess.tables else "[dim]–[/dim]"
                t.add_row(str(i+1), n, in_sess)
            console.print(t)

        elif action == "X":
            console.print(Rule("[bold]Import Excel[/bold]"))
            path = Prompt.ask("Path to .xlsx file").strip().strip("'\"")
            if os.path.exists(path):
                try:
                    xl = pd.ExcelFile(path)
                    sheets = xl.sheet_names
                    if len(sheets) == 1:
                        sheet = sheets[0]
                    else:
                        console.print(f"Sheets: [cyan]{', '.join(sheets)}[/cyan]")
                        sheet = Prompt.ask("Sheet to load", default=sheets[0])
                    
                    df_xl = pd.read_excel(path, sheet_name=sheet)
                    df_xl.columns = [str(c).strip() for c in df_xl.columns]
                    
                    tname = Prompt.ask("Table name in session", default=sheet)
                    sess.add(tname, df_xl)
                    console.print(f"[green]✔ Imported '{sheet}' ({len(df_xl):,} rows).[/green]")
                    
                    if Confirm.ask(f"Save '{tname}' to DB ({os.path.basename(sess.db_path)})?", default=False):
                        db_save(df_xl, sess.db_path, tname)
                except Exception as e:
                    console.print(f"[red]❌ Import error: {e}[/red]")
            else:
                console.print(f"[red]File not found: {path}[/red]")


# ── [K] SWITCH TABLE ──────────────────────────────────────────────────────────

def op_switch_table(sess: Session):
    tables = sess.list_tables()
    if len(tables) == 1:
        console.print("[dim]Only one table in session.[/dim]"); return
    for i, n in enumerate(tables):
        mark = " [bold cyan]← active[/bold cyan]" if n==sess.active else ""
        console.print(f"  [yellow]{i+1}[/yellow]  {n}{mark}")
    c = Prompt.ask("Switch to table #")
    try:
        sess.active = tables[int(c)-1]
        console.print(f"[green]✔ Active: '[bold]{sess.active}[/bold]' "
                      f"— {len(sess.df):,}r × {len(sess.df.columns)}c[/green]")
        show_columns(sess.df, compact=True)
    except (ValueError, IndexError):
        console.print("[red]Invalid.[/red]")


# ── [P] PREVIEW WITH COLUMN PICKER ───────────────────────────────────────────

def op_preview(sess: Session):
    df = sess.df
    console.print(Rule("[bold]Preview[/bold]"))
    n = int(Prompt.ask("How many rows?", default="10"))
    ci = Prompt.ask("Columns to show (letters/names comma-sep, blank = all)").strip()
    cols = ask_cols(ci, df) if ci else None
    show_preview(df, n=n, cols=cols, title=f"▸ {sess.active}")


# ── [W] SAVE ──────────────────────────────────────────────────────────────────

def op_save(sess: Session):
    console.print(Rule("[bold]Save to Database[/bold]"))
    tables = sess.list_tables()
    for i, n in enumerate(tables):
        console.print(f"  [yellow]{i+1}[/yellow]  {n}  "
                      f"({len(sess.tables[n]):,}r × {len(sess.tables[n].columns)}c)")

    default_idx = str(tables.index(sess.active)+1)
    c = Prompt.ask("Which table to save?", default=default_idx)
    try:    tname = tables[int(c)-1]
    except: tname = sess.active

    out_db    = Prompt.ask("Save to DB path",  default=f"work_{os.path.basename(sess.db_path)}")
    out_table = Prompt.ask("Table name in DB", default=tname)
    exists    = Prompt.ask("If table exists: [R]eplace / [A]ppend", default="R").upper()

    try:
        db_save(sess.tables[tname], out_db, out_table,
                if_exists="replace" if exists=="R" else "append")
        console.print("[dim]Your source database was NOT modified.[/dim]")
    except Exception as e:
        console.print(f"[red]❌ Save error: {e}[/red]")


# ── [E] EXPORT ────────────────────────────────────────────────────────────────

def op_export(sess: Session):
    console.print(Rule("[bold]Export[/bold]"))
    tables = sess.list_tables()
    for i, n in enumerate(tables): console.print(f"  [yellow]{i+1}[/yellow]  {n}")

    default_idx = str(tables.index(sess.active)+1)
    c = Prompt.ask("Which table?", default=default_idx)
    try:    tname = tables[int(c)-1]
    except: tname = sess.active

    df_out = sess.tables[tname]
    ts     = datetime.now().strftime("%Y%m%d_%H%M")
    fmt    = Prompt.ask("Format [C]SV / [X]LSX", default="C").upper()
    ext    = "csv" if fmt=="C" else "xlsx"
    path   = Prompt.ask("File path", default=f"{tname}_{ts}.{ext}")

    try:
        if fmt == "C":
            df_out.to_csv(path, index=False)
        else:
            df_out.to_excel(path, index=False, engine="openpyxl")
        console.print(f"[green]✔ Exported {len(df_out):,} rows → [bold]{path}[/bold][/green]")
    except Exception as e:
        console.print(f"[red]❌ Export error: {e}[/red]")