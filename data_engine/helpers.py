"""
Helper utilities for column operations, fuzzy matching, and value formatting.
"""

import pandas as pd
from typing import Optional

from rapidfuzz import process as fz_process, fuzz
from rich.console import Console
from rich.prompt import Prompt, Confirm
from rich.table import Table
from rich import box

console = Console()


# ╔══════════════════════════════════════════════════════════════════╗
# ║  COLUMN HELPERS                                                   ║
# ╚══════════════════════════════════════════════════════════════════╝

def col_letter(idx: int) -> str:
    """
    Convert column index to Excel-style letter (0 -> A, 1 -> B, 26 -> AA).
    
    Args:
        idx: Column index (0-based)
        
    Returns:
        Excel-style column letter
    """
    result, idx = "", idx + 1
    while idx > 0:
        idx, r = divmod(idx - 1, 26)
        result = chr(65 + r) + result
    return result


def build_col_map(df: pd.DataFrame) -> dict:
    """
    Build a mapping from column references (letters or names) to actual column names.
    
    Args:
        df: DataFrame to build map for
        
    Returns:
        Dictionary mapping lowercase letters and column names to actual column names
    """
    m = {}
    for i, col in enumerate(df.columns):
        m[col_letter(i).lower()] = col
        m[col.lower()] = col
    return m


def resolve(user_input: str, df: pd.DataFrame) -> Optional[str]:
    """
    Resolve user input to exact column name.
    
    Accepts letter (A, b, AA), column name, or partial name.
    
    Args:
        user_input: User's column reference
        df: DataFrame to resolve against
        
    Returns:
        Exact column name, or None if not found.
    """
    cm = build_col_map(df)
    key = user_input.strip().lower()
    return cm.get(key)


def ask_cols(prompt_text: str, df: pd.DataFrame, allow_all: bool = False) -> list:
    """
    Prompt user for column selection and resolve to actual column names.
    
    Args:
        prompt_text: Prompt message to display
        df: DataFrame to resolve columns against
        allow_all: If True, accept "ALL" or empty string to return all columns
        
    Returns:
        List of resolved column names
    """
    raw = Prompt.ask(prompt_text).strip()
    if allow_all and raw.upper() in ("ALL", ""):
        return list(df.columns)
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    resolved, bad = [], []
    for p in parts:
        c = resolve(p, df)
        if c:
            resolved.append(c)
        else:
            bad.append(p)
    if bad:
        console.print(f"[yellow]⚠ Not found: {', '.join(bad)}[/yellow]")
    return resolved


# ╔══════════════════════════════════════════════════════════════════╗
# ║  VALUE FORMATTING                                                  ║
# ╚══════════════════════════════════════════════════════════════════╝

def fmt_val(v) -> str:
    """
    Format a value for display in tables.
    
    Args:
        v: Value to format
        
    Returns:
        Formatted string representation
    """
    if pd.isna(v) or v is None or str(v).strip() == "":
        return "[dim italic red]∅ null[/dim italic red]"
    return str(v)[:60]


def clean_number_string(s: pd.Series) -> pd.Series:
    """
    Aggressively clean string series to numbers.
    Removes $, %, commas, and spaces before converting.
    
    Args:
        s: Series to clean
        
    Returns:
        Numeric series
    """
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors='coerce')
    
    # Remove common non-numeric chars: $ , % and whitespace
    clean = s.astype(str).str.replace(r'[$,%\s]', '', regex=True)
    return pd.to_numeric(clean, errors='coerce')

# ╔══════════════════════════════════════════════════════════════════╗
# ║  FUZZY VALUE MATCHING                                              ║
# ╚══════════════════════════════════════════════════════════════════╝

def fuzzy_pick_value(raw: str, df: pd.DataFrame, col: str,
                     threshold: int = 60, limit: int = 6) -> Optional[str]:
    """
    If 'raw' is not an exact value in col, show fuzzy matches and let
    user pick. Returns chosen value string, or raw if confirmed.
    
    Args:
        raw: User's input value
        df: DataFrame to search in
        col: Column to search in
        threshold: Minimum match score (0-100)
        limit: Maximum number of matches to show
        
    Returns:
        Selected value string, or original raw if confirmed, or None if cancelled
    """
    uniques = df[col].dropna().astype(str).unique().tolist()
    if raw in uniques:
        return raw  # exact match, skip fuzzy

    matches = fz_process.extract(raw, uniques, scorer=fuzz.WRatio, limit=limit)
    good = [(m, s) for m, s, _ in matches if s >= threshold]

    if not good:
        console.print(f"[yellow]⚠ No close match for '{raw}' in '{col}'.[/yellow]")
        return raw if Confirm.ask("Use as-is?", default=True) else None

    console.print(f"\n[yellow]❓ '{raw}' not found exactly. Best matches:[/yellow]")
    t = Table(box=box.SIMPLE)
    t.add_column("#", style="yellow", width=4, justify="center")
    t.add_column("Value", style="white", min_width=20)
    t.add_column("Match", style="cyan", width=8, justify="right")
    for i, (val, score) in enumerate(good):
        t.add_row(str(i+1), val, f"{score:.0f}%")
    t.add_row("0", "[dim]Keep my original text[/dim]", "")
    console.print(t)

    pick = Prompt.ask("Pick #", default="1").strip()
    if pick == "0":
        return raw
    try:
        return good[int(pick)-1][0]
    except:
        return raw


def fuzzy_pick_values_list(raw_list: list, df: pd.DataFrame, col: str) -> list:
    """
    Apply fuzzy_pick_value to a list of values.
    
    Args:
        raw_list: List of values to resolve
        df: DataFrame to search in
        col: Column to search in
        
    Returns:
        List of resolved values (None values filtered out)
    """
    return [v for v in (fuzzy_pick_value(r, df, col) for r in raw_list) if v is not None]


# ╔══════════════════════════════════════════════════════════════════╗
# ║  MISC HELPERS                                                      ║
# ╚══════════════════════════════════════════════════════════════════╝

def find_dbs() -> list:
    """Find all SQLite database files in current directory."""
    import os
    return sorted(f for f in os.listdir(".") if f.endswith((".db", ".sqlite", ".sqlite3")))
