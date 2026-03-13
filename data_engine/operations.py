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