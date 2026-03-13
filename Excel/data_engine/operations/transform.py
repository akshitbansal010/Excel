"""
Transform module — core data transformation operations.
Improved with Excel-like guided experience.
"""

import os
import numpy as np
import pandas as pd

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.rule import Rule
from rich.table import Table

from ..database import db_load, db_save, db_tables
from ..display import show_columns, show_null_report, show_preview
from ..helpers import ask_cols, clean_number_string, fmt_val, fuzzy_pick_value, resolve
from ..session import Session

console = Console()


def op_add_column(sess: Session) -> pd.DataFrame:
    """Add new column - Excel-like guided wizard."""
    df = sess.df
    console.print(Rule("[bold]➕ Add New Column[/bold]"))
    console.print(Panel(
        "Create new columns using formulas, conditional logic, or transformations.\n"
        "Choose a method below:",
        title="Column Builder", border_style="green"
    ))
    
    # Show easy numbered options
    console.print("\n[bold]What do you want to create?[/bold]")
    console.print("  [bold yellow]1[/bold yellow]  [cyan]Formula Column[/cyan]      Math formula using existing columns")
    console.print("  [bold yellow]2[/bold yellow]  [cyan]Conditional (IF)[/cyan]   IF/THEN/ELSE logic")
    console.print("  [bold yellow]3[/bold yellow]  [cyan]Text Pipeline[/cyan]      Clean/transform text step-by-step")
    console.print("  [bold yellow]4[/bold yellow]  [cyan]Map Values[/cyan]        Replace specific values (VLOOKUP-like)")
    
    mode = Prompt.ask("\nChoose type", choices=["1","2","3","4"], default="1")
    new_col = Prompt.ask("\nName for new column", default="NewColumn")

    # ── 1. FORMULA ────────────────────────────────────────────────────────────
    if mode == "1":
        console.print("\n[bold]📐 Formula Builder[/bold]")
        show_columns(df, compact=True)
        
        console.print("\n[bold]Examples:[/bold]")
        console.print("  [cyan]`Price` * `Quantity`[/cyan]          → Multiply two columns")
        console.print("  [cyan]`FirstName` + ' ' + `LastName`[/cyan] → Combine text")
        console.print("  [cyan]`Amount` * 1.1[/cyan]                 → Multiply by number")
        console.print("  [cyan]`Price` - `Cost`[/cyan]              → Subtract columns")
        
        formula = Prompt.ask("\nEnter formula", default="")
        if not formula.strip():
            console.print("[yellow]No formula entered.[/yellow]")
            return df
            
        try:
            safe_scope = {col: df[col] for col in df.columns}
            df[new_col] = pd.eval(formula, local_dict=safe_scope, engine='numexpr')
            console.print(f"[green]✔ Created column '{new_col}'[/green]")
        except Exception as e:
            console.print(f"[red]❌ Formula error: {e}[/red]")
            console.print("[dim]Tip: Use backticks around column names with spaces.[/dim]")

    # ── 2. CONDITIONAL (IF/ELSE) ──────────────────────────────────────────────
    elif mode == "2":
        console.print("\n[bold]🔀 Conditional Logic (IF/THEN/ELSE)[/bold]")
        show_columns(df, compact=True)
        
        console.print("\n[bold]How it works:[/bold]")
        console.print("  IF [condition] THEN [value if true] ELSE [value if false]")
        
        console.print("\n[bold]Examples:[/bold]")
        console.print("  [cyan]`Score` >= 50[/cyan]  THEN [green]'Pass'[/green]  ELSE [red]'Fail'[/red]")
        console.print("  [cyan]`Status` == 'Active'[/cyan]  THEN [green]1[/green]  ELSE [red]0[/red]")
        
        cond = Prompt.ask("\nIF condition (use column names)", default="")
        if not cond.strip():
            return df
            
        # Preview condition hits
        try:
            hits = df.eval(cond).sum()
            console.print(f"  [dim]↳ This matches [bold]{hits:,} rows[/bold][/dim]")
        except Exception as e:
            console.print(f"[yellow]⚠ Check your condition syntax: {e}[/yellow]")

        true_v = Prompt.ask("THEN value (if condition is TRUE)", default="1")
        false_v = Prompt.ask("ELSE value (if condition is FALSE)", default="0")

        try:
            # Helper to check if input looks like a column name wrapped in backticks
            def parse_val(v):
                if v is None:
                    return np.nan
                v = str(v).strip()
                # Check backtick-wrapped column reference
                if v.startswith("`") and v.endswith("`") and len(v) > 2:
                    col_name = v[1:-1]
                    if col_name in df.columns:
                        return df[col_name]
                # Try numeric conversion
                try:
                    return float(v)
                except (ValueError, TypeError):
                    return np.nan

            # Apply np.where
            mask = df.eval(cond)
            df[new_col] = np.where(mask, parse_val(true_v), parse_val(false_v))
            console.print(f"[green]✔ Created '{new_col}' with conditional logic.[/green]")
            
        except Exception as e:
            console.print(f"[red]❌ Error: {e}[/red]")

    # ── 4. MAP (VLOOKUP-ish) ──────────────────────────────────────────────────
    elif mode == "4":
        console.print("\n[bold]🔍 Map Values (Like VLOOKUP)[/bold]")
        show_columns(df, compact=True)
        
        src_col = resolve(Prompt.ask("\nColumn to map from"), df)
        if not src_col:
            console.print("[red]Column not found.[/red]"); return df
        
        show_preview(df[[src_col]], n=10, title=f"Unique values in '{src_col}'")

        mappings: dict = {}
        console.print("\n[bold]Enter value mappings:[/bold]")
        console.print("[dim]Enter 'From' value → 'To' value. Leave 'From' blank to finish.[/dim]\n")
        
        while True:
            from_v = Prompt.ask("  From (blank = done)", default="").strip()
            if not from_v:
                if mappings:
                    break
                else:
                    console.print("[yellow]Add at least one mapping.[/yellow]")
                    continue
            # fuzzy_pick_value may return None; use fallback
            fuzzy_result = fuzzy_pick_value(from_v, df, src_col)
            from_v = fuzzy_result if fuzzy_result is not None else from_v
            from_key = str(from_v)
            to_v = Prompt.ask(f"  → To (replace '{from_v}' with)", default="").strip()
            mappings[from_key] = to_v
            console.print(f"     [green]✓[/green] {from_v} → {to_v}")

        default = Prompt.ask("\nDefault value for unmapped (press Enter for blank)", default="").strip()
        # Map values while preserving types when possible
        df[new_col] = df[src_col].astype(str).map(
            lambda v: mappings.get(str(v).strip(), default if default else np.nan)
        )
        # Try to convert to numeric if all values look numeric
        try:
            df[new_col] = pd.to_numeric(df[new_col], errors="ignore")
        except Exception:
            pass  # Keep as-is if conversion fails
        
        mapped_count = df[new_col].notna().sum()
        console.print(f"[green]✔ '{new_col}' — {mapped_count:,} values mapped.[/green]")

    # ── 3. PIPELINE (BLOCKS) ─────────────────────────────────────────────────
    elif mode == "3":
        console.print("\n[bold]🔄 Text Transformation Pipeline[/bold]")
        show_columns(df, compact=True)
        
        src_col = resolve(Prompt.ask("\nColumn to transform"), df)
        if not src_col:
            console.print("[red]Column not found.[/red]"); return df
        
        # Initialize working series
        s = df[src_col].copy()
        history = [src_col]

        console.print("\n[bold]Available transformations:[/bold]")
        console.print("  [bold]upper[/bold]    → UPPERCASE")
        console.print("  [bold]lower[/bold]    → lowercase")
        console.print("  [bold]strip[/bold]    → Remove extra spaces")
        console.print("  [bold]title[/bold]    → Title Case")
        console.print("  [bold]int[/bold]     → Convert to whole number")
        console.print("  [bold]float[/bold]   → Convert to decimal")
        console.print("  [bold]clean[/bold]    → Remove $ , % symbols")
        console.print("  [bold]round[/bold]    → Round to decimals")
        console.print("  [bold]date[/bold]     → Parse as date")
        console.print("  [bold]fill[/bold]     → Fill empty cells")
        console.print("  [bold]extract[/bold]  → Extract with regex")
        console.print("  [bold]done[/bold]     → Finish and save")
        
        while True:
            console.print(f"\n[bold]Pipeline:[/bold] [cyan]{' → '.join(history)}[/cyan]")
            
            op = Prompt.ask("Add transformation (or 'done' to finish)", default="done").strip().lower()
            
            if op == "done": break
            
            try:
                if op == "upper":
                    s = s.astype(str).str.upper()
                elif op == "lower":
                    s = s.astype(str).str.lower()
                elif op == "strip":
                    s = s.astype(str).str.strip()
                elif op == "title":
                    s = s.astype(str).str.title()
                elif op == "int":
                    s = pd.to_numeric(s, errors="coerce").astype("Int64")
                elif op == "float":
                    s = pd.to_numeric(s, errors="coerce")
                elif op == "clean":
                    s = clean_number_string(s)
                elif op == "len":
                    s = s.astype(str).str.len()
                elif op == "round":
                    try:
                        p = int(Prompt.ask("  Decimal places", default="2"))
                    except ValueError:
                        p = 2  # default if user enters non-integer
                    s = pd.to_numeric(s, errors="coerce").round(p)
                elif op == "date":
                    s = pd.to_datetime(s, errors="coerce")
                elif op == "fill":
                    val = Prompt.ask("  Fill empty cells with", default="")
                    s = s.fillna(val)
                elif op == "extract":
                    pat = Prompt.ask(r"  Regex pattern (e.g. `(\d+)` for numbers)")
                    s = s.astype(str).str.extract(pat, expand=False)
                else:
                    console.print(f"[yellow]Unknown: {op}[/yellow]")
                    continue
                    
                history.append(op)
            except Exception as e:
                console.print(f"[red]❌ Error: {e}[/red]")

        df[new_col] = s
        console.print(f"[green]✔ Created '{new_col}' via pipeline.[/green]")
        
    show_preview(df, n=5); return df


def op_aggregate(sess: Session):
    """Count-If / Sum-If / Aggregations - Excel-like guided."""
    df = sess.df
    console.print(Rule("[bold]∑ Count / Sum / Average[/bold]"))
    
    console.print(Panel(
        "Calculate totals, counts, averages grouped by category.\n"
        "Like Excel's SUMIF, COUNTIF, or Pivot Tables.",
        title="Aggregations", border_style="cyan"
    ))
    
    show_columns(df, compact=True)

    # Ask if they want simple or grouped
    console.print("\n[bold]What do you want to calculate?[/bold]")
    console.print("  [yellow]1[/yellow]  Count total rows")
    console.print("  [yellow]2[/yellow]  Count by category (like Excel's COUNTIF)")
    console.print("  [yellow]3[/yellow]  Sum a column by group")
    console.print("  [yellow]4[/yellow]  Average by group")
    console.print("  [yellow]5[/yellow]  Custom aggregation")
    
    choice = Prompt.ask("\nChoose", choices=["1","2","3","4","5"], default="1")
    
    try:
        if choice == "1":
            # Simple count
            console.print(f"\n[bold]Total rows: [cyan]{len(df):,}[/cyan][/bold]")
            return
            
        elif choice == "2":
            # Count by category
            group_col = resolve(Prompt.ask("Group by column (e.g., Category, Status)"), df)
            if not group_col:
                console.print("[red]Column not found.[/red]"); return
                
            result = df.groupby(group_col, dropna=False).size().reset_index(name="Count")
            result = result.sort_values("Count", ascending=False)
            
        elif choice == "3":
            # Sum by group
            group_col = resolve(Prompt.ask("Group by column"), df)
            if not group_col: return
            
            val_col = resolve(Prompt.ask("Column to SUM"), df)
            if not val_col: return
            
            result = df.groupby(group_col, dropna=False)[val_col].agg('sum').reset_index()
            result.columns = [group_col, f"Sum_{val_col}"]
            result = result.sort_values(result.columns[-1], ascending=False)
            
        elif choice == "4":
            # Average by group
            group_col = resolve(Prompt.ask("Group by column"), df)
            if not group_col: return
            
            val_col = resolve(Prompt.ask("Column to AVERAGE"), df)
            if not val_col: return
            
            result = df.groupby(group_col, dropna=False)[val_col].agg('mean').reset_index()
            result.columns = [group_col, f"Avg_{val_col}"]
            result[result.columns[-1]] = result[result.columns[-1]].round(2)
            result = result.sort_values(result.columns[-1], ascending=False)
            
        else:
            # Custom
            group_col = resolve(Prompt.ask("Group by column (blank = total)", default="").strip(), df)
            group_col = group_col if group_col else None
            
            console.print("\n[bold]Aggregation functions:[/bold]")
            console.print("  [cyan]count[/cyan]  - Count rows")
            console.print("  [cyan]sum[/cyan]    - Add up values")
            console.print("  [cyan]mean[/cyan]   - Average")
            console.print("  [cyan]median[/cyan] - Middle value")
            console.print("  [cyan]min[/cyan]    - Smallest")
            console.print("  [cyan]max[/cyan]    - Largest")
            
            agg = Prompt.ask("Function", default="sum").strip().lower()
            
            if group_col:
                val_col = resolve(Prompt.ask("Column to aggregate"), df)
                if not val_col: return
                result = df.groupby(group_col, dropna=False)[val_col].agg(agg).reset_index()
                result.columns = [group_col, f"{agg}_{val_col}"]
                result = result.sort_values(result.columns[-1], ascending=False)
            else:
                val_col = resolve(Prompt.ask("Column to aggregate"), df)
                if not val_col: return
                v = getattr(df[val_col].dropna(), agg)()
                console.print(f"\n[bold]{agg.upper()} of {val_col}: [cyan]{v:,.2f}[/cyan][/bold]")
                return

        # Display result
        t = Table(box=box.ROUNDED, show_lines=True)
        for c in result.columns: t.add_column(str(c), justify="right")
        for _, row in result.head(30).iterrows():
            t.add_row(*[fmt_val(v) for v in row])
        console.print(t)

        if len(result) > 30:
            console.print(f"[dim]... showing 30 of {len(result)} rows[/dim]")

        if Confirm.ask("\nSave as a new working table?", default=False):
            tname = Prompt.ask("Table name", default="summary")
            sess.add(tname, result)
            console.print(f"[green]✔ Added '{tname}' to session.[/green]")

    except Exception as e:
        console.print(f"[red]❌ Error: {e}[/red]")


def op_sort(sess: Session) -> pd.DataFrame:
    """Sort data - Excel-like."""
    df = sess.df
    console.print(Rule("[bold]⇅ Sort Data[/bold]"))
    show_columns(df, compact=True)

    console.print("\n[bold]What to sort by?[/bold]")
    console.print("  [yellow]1[/yellow]  Single column")
    console.print("  [yellow]2[/yellow]  Multiple columns")
    
    choice = Prompt.ask("Choice", choices=["1","2"], default="1")
    
    if choice == "1":
        cols = [resolve(Prompt.ask("Column to sort by"), df)]
    else:
        console.print("[dim]Enter columns separated by commas (e.g., Category, Date)[/dim]")
        cols = ask_cols("Columns to sort by", df)
    
    if not cols or not any(cols):
        console.print("[red]No valid column selected.[/red]")
        return df
        
    cols = [c for c in cols if c]
    if not cols:
        return df
        
    console.print("\n[bold]Sort order:[/bold]")
    console.print("  [yellow]A[/yellow]  Ascending  (A→Z, 1→9, oldest→newest)")
    console.print("  [yellow]D[/yellow]  Descending (Z→A, 9→1, newest→oldest)")
    
    asc = Prompt.ask("Order", choices=["A","D"], default="A").upper() != "D"
    df = df.sort_values(by=cols, ascending=asc)
    
    direction = "↑ A→Z" if asc else "↓ Z→A"
    console.print(f"[green]✔ Sorted by [bold]{', '.join(cols)}[/bold] ({direction})[/green]")
    show_preview(df, n=5, title="Sorted Result")
    return df


def op_handle_nulls(sess: Session) -> pd.DataFrame:
    """Handle nulls / missing values - Excel-like wizard."""
    df = sess.df
    console.print(Rule("[bold]🧹 Clean Missing Values[/bold]"))
    show_null_report(df)
    
    console.print("\n[bold]What do you want to do?[/bold]")
    console.print("  [yellow]1[/yellow]  Delete rows with missing values")
    console.print("  [yellow]2[/yellow]  Fill with a specific value")
    console.print("  [yellow]3[/yellow]  Fill with mean/median/mode")
    console.print("  [yellow]4[/yellow]  Convert blank text to empty")
    console.print("  [yellow]5[/yellow]  Delete columns that are all empty")
    console.print("  [yellow]6[/yellow]  Forward fill (carry last value down)")
    console.print("  [yellow]7[/yellow]  Create a 'is blank' flag column")
    
    action = Prompt.ask("\nChoose action", choices=["1","2","3","4","5","6","7"], default="2")

    if action != "5":
        show_columns(df, compact=True)
        ci = Prompt.ask("\nColumn (or ALL for all columns)", default="ALL").strip()
        cols = list(df.columns) if ci.upper() == "ALL" else [resolve(ci, df)]
        cols = [c for c in cols if c and c in df.columns]
        if not cols:
            console.print("[red]No valid columns.[/red]"); return df

    if action == "1":
        before = len(df); 
        df = df.dropna(subset=cols)
        removed = before - len(df)
        console.print(f"[green]✔ Deleted {removed:,} rows with missing values.[/green]")

    elif action == "2":
        val = Prompt.ask("Fill with what value?", default="0")
        try:
            try:
                val = float(val)
            except ValueError:
                pass
            for c in cols: 
                df[c] = df[c].fillna(val)
            console.print(f"[green]✔ Filled empty cells with '{val}'.[/green]")
        except Exception as e:
            console.print(f"[red]❌ {e}[/red]"); return df

    elif action == "3":
        console.print("\n[bold]Fill method:[/bold]")
        console.print("  [yellow]M[/yellow]  Mean (average)")
        console.print("  [yellow]D[/yellow]  Median (middle value)")
        console.print("  [yellow]O[/yellow]  Mode (most common)")
        
        agg = Prompt.ask("Method", choices=["M","D","O"], default="M").upper()
        
        for c in cols:
            if agg == "M":
                fill_val = df[c].mean()
            elif agg == "D":
                fill_val = df[c].median()
            else:
                mode_result = df[c].mode()
                fill_val = mode_result[0] if len(mode_result) > 0 else None
                
            if fill_val is not None and not pd.isna(fill_val):
                df[c] = df[c].fillna(fill_val)
                console.print(f"[green]✔ '{c}' filled with {fill_val:.2f}[/green]")
        show_preview(df, n=5); return df

    elif action == "4":
        for c in cols:
            df[c] = df[c].replace("", np.nan)
        console.print("[green]✔ Converted blank text to empty cells.[/green]")

    elif action == "5":
        all_null = [c for c in df.columns if df[c].isna().all()]
        if not all_null:
            console.print("[yellow]No all-empty columns found.[/yellow]"); return df
        df = df.drop(columns=all_null)
        console.print(f"[green]✔ Deleted {len(all_null)} empty columns: {all_null}[/green]")

    elif action == "6":
        for c in cols:
            df[c] = df[c].ffill()
        console.print("[green]✔ Forward-filled (copied last value down).[/green]")

    elif action == "7":
        for c in cols:
            df[f"{c}_is_blank"] = df[c].isna()
        console.print(f"[green]✔ Created {len(cols)} blank flag columns.[/green]")

    show_preview(df, n=5); return df


def op_rename_drop(sess: Session) -> pd.DataFrame:
    """Rename or drop columns - Excel-like."""
    df = sess.df
    console.print(Rule("[bold]✏️ Rename or Delete Columns[/bold]"))
    show_columns(df)

    console.print("\n[bold]What do you want to do?[/bold]")
    console.print("  [yellow]1[/yellow]  Rename a column")
    console.print("  [yellow]2[/yellow]  Delete column(s)")
    console.print("  [yellow]3[/yellow]  Clean all column names (spaces→_, lowercase)")
    
    action = Prompt.ask("Action", choices=["1","2","3"], default="1")

    if action == "1":
        col = resolve(Prompt.ask("Column to rename"), df)
        if not col: console.print("[red]Not found.[/red]"); return df
        new_name = Prompt.ask("New name", default=col)
        df = df.rename(columns={col: new_name})
        console.print(f"[green]✔ Renamed: '{col}' → '{new_name}'[/green]")

    elif action == "2":
        cols = ask_cols("Columns to delete (comma-separated)", df)
        if not cols: return df
        df = df.drop(columns=cols)
        console.print(f"[green]✔ Deleted: {', '.join(cols)}[/green]")

    elif action == "3":
        new_cols = {c: c.strip().lower().replace(" ", "_") for c in df.columns}
        df = df.rename(columns=new_cols)
        console.print("[green]✔ Cleaned all column names.[/green]")

    show_preview(df, n=5); return df


def op_dedupe(sess: Session) -> pd.DataFrame:
    """Remove duplicates - Excel-like."""
    df = sess.df
    console.print(Rule("[bold]📋 Remove Duplicates[/bold]"))
    show_columns(df, compact=True)

    cols = ask_cols("Columns to check (blank = all columns)", df)
    subset = cols if cols else None

    before = len(df)
    df = df.drop_duplicates(subset=subset)
    removed = before - len(df)
    console.print(f"[green]✔ Removed {removed:,} duplicate rows ({len(df):,} unique remain).[/green]")
    show_preview(df, n=5)
    return df


def op_pivot(sess: Session):
    """Pivot / Group summary - Excel-like."""
    df = sess.df
    console.print(Rule("[bold]📊 Pivot Table / Group Summary[/bold]"))
    show_columns(df, compact=True)

    console.print("\n[bold]Create a summary like Excel's Pivot Table[/bold]\n")
    
    row_cols_in = Prompt.ask("Group by column(s) (what to group by)", default="").strip()
    row_cols = [resolve(c.strip(), df) for c in row_cols_in.split(",") if c.strip()]
    if not row_cols:
        console.print("[red]Need at least one group column.[/red]"); return

    pivot_col_in = Prompt.ask("Column to summarize (optional, blank=just count)", default="").strip()
    pivot_col = resolve(pivot_col_in, df) if pivot_col_in else None

    if not pivot_col:
        # Just counting
        val_col = None
        agg_fn = "count"
    else:
        val_col = pivot_col
        console.print("\n[bold]How to summarize:[/bold]")
        console.print("  [yellow]sum[/yellow]     Total")
        console.print("  [yellow]mean[/yellow]    Average")
        console.print("  [yellow]count[/yellow]   Count")
        console.print("  [yellow]median[/yellow] Middle")
        console.print("  [yellow]min[/yellow]    Smallest")
        console.print("  [yellow]max[/yellow]    Largest")
        agg_fn = Prompt.ask("Method", default="sum").strip().lower()

    try:
        if val_col:
            result = df.groupby(row_cols, dropna=False)[val_col].agg(agg_fn).reset_index()
            result.columns = list(row_cols) + [f"{agg_fn}_{val_col}"]
        else:
            result = df.groupby(row_cols, dropna=False).size().reset_index(name="Count")
        
        result = result.sort_values(result.columns[-1], ascending=False)
        
        t = Table(box=box.ROUNDED, show_lines=True)
        for c in result.columns: t.add_column(str(c), justify="right")
        for _, row in result.head(40).iterrows():
            t.add_row(*[f"{v:,.2f}" if isinstance(v, float) else str(v) for v in row])
        console.print(t)

        if Confirm.ask("\nSave as new table?", default=False):
            tname = Prompt.ask("Name", default="pivot")
            sess.add(tname, result)
            console.print(f"[green]✔ Added '{tname}'[/green]")
    except Exception as e:
        console.print(f"[red]❌ Error: {e}[/red]")


def op_change_type(sess: Session) -> pd.DataFrame:
    """Change column type - Excel-like wizard."""
    df = sess.df
    console.print(Rule("[bold]🔄 Change Data Type[/bold]"))
    show_columns(df)

    col = resolve(Prompt.ask("Column to change (letter or name)"), df)
    if not col:
        console.print("[red]Column not found.[/red]"); return df

    current = str(df[col].dtype)
    sample = df[col].dropna().head(5).tolist()
    console.print(f"\n[bold]Current:[/bold] {current}")
    console.print(f"[bold]Sample:[/bold] {sample}")
    
    console.print("\n[bold]Convert to:[/bold]")
    console.print("  [yellow]1[/yellow]  Text/String")
    console.print("  [yellow]2[/yellow]  Whole Number")
    console.print("  [yellow]3[/yellow]  Decimal Number")
    console.print("  [yellow]4[/yellow]  Yes/No (True/False)")
    console.print("  [yellow]5[/yellow]  Date")
    console.print("  [yellow]6[/yellow]  Category")
    
    choice = Prompt.ask("Type", choices=["1","2","3","4","5","6"]).strip()
    
    type_map = {"1": "str", "2": "int", "3": "float", "4": "bool", "5": "date", "6": "category"}
    target = type_map.get(choice, "str")

    try:
        if target == "str":
            df[col] = df[col].astype(str)
            df[col] = df[col].replace({"nan": np.nan, "<NA>": np.nan, "None": np.nan})

        elif target == "int":
            converted = clean_number_string(df[col])
            n_fail = int(converted.isna().sum() - df[col].isna().sum())
            if n_fail > 0:
                console.print(f"[yellow]⚠ {n_fail:,} values can't convert → will become empty[/yellow]")
                if not Confirm.ask("Continue?", default=True): return df
            df[col] = converted.astype("Int64")

        elif target == "float":
            converted = pd.to_numeric(df[col], errors="coerce")
            n_fail = int(converted.isna().sum() - df[col].isna().sum())
            if n_fail > 0:
                console.print(f"[yellow]⚠ {n_fail:,} values failed → empty[/yellow]")
            df[col] = converted

        elif target == "bool":
            tv = {"true","1","yes","y","t","on"}
            fv = {"false","0","no","n","f","off"}
            df[col] = df[col].astype(str).str.lower().map(
                lambda x: True if x in tv else (False if x in fv else np.nan)
            )

        elif target == "date":
            fmt = Prompt.ask("Date format (blank=auto-detect)", default="").strip()
            initial_na = df[col].isna().sum()
            if fmt:
                df[col] = pd.to_datetime(df[col], format=fmt, errors="coerce")
            else:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            n_fail = int(df[col].isna().sum() - initial_na)
            if n_fail > 0:
                console.print(f"[yellow]⚠ {n_fail:,} values couldn't be parsed as dates[/yellow]")

        elif target == "category":
            df[col] = df[col].astype("category")

        new_type = str(df[col].dtype)
        console.print(f"[green]✔ Changed: {current} → {new_type}[/green]")

    except Exception as e:
        console.print(f"[red]❌ Error: {e}[/red]")

    show_preview(df, n=5); return df


def op_join(sess: Session):
    """Join tables - Excel VLOOKUP-like."""
    console.print(Rule("[bold]🔗 Join Tables (VLOOKUP)[/bold]"))
    
    tables = sess.list_tables()
    if len(tables) < 2:
        console.print("[yellow]Need at least 2 tables for join.[/yellow]")
        return
    
    console.print("\n[bold]Available tables:[/bold]")
    for i, n in enumerate(tables):
        console.print(f"  [yellow]{i+1}[/yellow]  {n} ({len(sess.tables[n]):,} rows)")
    
    # Select tables
    t1_idx = Prompt.ask("Main table", default="1").strip()
    t2_idx = Prompt.ask("Lookup table", default="2" if len(tables) > 1 else "1").strip()
    
    try:
        t1_name = tables[int(t1_idx)-1]
        t2_name = tables[int(t2_idx)-1]
    except (ValueError, IndexError):
        console.print("[red]Invalid selection.[/red]"); return
    
    t1 = sess.tables[t1_name]
    t2 = sess.tables[t2_name]
    
    # Select key columns
    show_columns(t1, compact=True)
    k1 = resolve(Prompt.ask(f"Join key in {t1_name}"), t1)
    if not k1: return
    
    show_columns(t2, compact=True)
    k2 = resolve(Prompt.ask(f"Join key in {t2_name}"), t2)
    if not k2: return
    
    # Join type
    console.print("\n[bold]Join type:[/bold]")
    console.print("  [yellow]1[/yellow]  LEFT JOIN - keep all from main, add matching from lookup")
    console.print("  [yellow]2[/yellow]  INNER JOIN - only keep rows that match in both")
    console.print("  [yellow]3[/yellow]  FULL JOIN - keep all rows from both")
    
    jtype = Prompt.ask("Type", choices=["1","2","3"], default="1")
    
    # Columns to add
    add_cols = [c for c in t2.columns if c != k2]
    console.print(f"\n[dim]Columns to add from {t2_name}: {add_cols}[/dim]")
    
    try:
        if jtype == "1":
            result = t1.merge(t2[[k2] + add_cols], left_on=k1, right_on=k2, how="left")
        elif jtype == "2":
            result = t1.merge(t2[[k2] + add_cols], left_on=k1, right_on=k2, how="inner")
        else:
            result = t1.merge(t2[[k2] + add_cols], left_on=k1, right_on=k2, how="outer")
        
        console.print(f"[green]✔ Joined: {len(result):,} rows × {len(result.columns)} columns[/green]")
        show_preview(result, n=5)
        
        if Confirm.ask("Save as new table?", default=True):
            name = Prompt.ask("Table name", default=f"{t1_name}_joined")
            sess.add(name, result)
            console.print(f"[green]✔ Added '{name}'[/green]")
    except Exception as e:
        console.print(f"[red]❌ Error: {e}[/red]")


def op_calculated_columns(sess: Session) -> pd.DataFrame:
    """Legacy alias for op_add_column."""
    return op_add_column(sess)
