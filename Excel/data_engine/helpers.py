"""
Helper utilities for column operations, fuzzy matching, and value formatting.
Improved with Excel-like numbered column selection.
"""

import pandas as pd
from typing import Optional, List

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
    
    Accepts letter (A, b, AA), column name, number (1, 2, 3), or partial name.
    
    Args:
        user_input: User's column reference
        df: DataFrame to resolve against
        
    Returns:
        Exact column name, or None if not found.
    """
    if not user_input or not user_input.strip():
        return None
    
    cm = build_col_map(df)
    key = user_input.strip().lower()
    
    # Check direct match first
    if key in cm:
        return cm[key]
    
    # Try numeric index (1-based like Excel)
    try:
        idx = int(user_input.strip())
        if 1 <= idx <= len(df.columns):
            return df.columns[idx - 1]
    except ValueError:
        pass
    
    # Fallback: partial substring match (case-insensitive)
    candidates = [col for col in df.columns if key in str(col).lower()]
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    # If ambiguous, choose longest matching column name as best effort
    return max(candidates, key=lambda c: len(str(c)))


def resolve_multiple(user_input: str, df: pd.DataFrame) -> List[str]:
    """
    Resolve multiple column references separated by commas.
    
    Args:
        user_input: Comma-separated column references
        df: DataFrame to resolve against
        
    Returns:
        List of resolved column names
    """
    if not user_input or not user_input.strip():
        return []
    
    parts = [p.strip() for p in user_input.split(",") if p.strip()]
    resolved = []
    for p in parts:
        c = resolve(p, df)
        if c:
            resolved.append(c)
    return resolved


def ask_cols_numbered(prompt_text: str, df: pd.DataFrame, allow_all: bool = True) -> list:
    """
    Prompt user for column selection using numbered list (Excel-like).
    Shows columns with numbers 1,2,3... for easy selection.
    
    Args:
        prompt_text: Prompt message to display
        df: DataFrame to resolve columns against
        allow_all: If True, allow selecting all columns
        
    Returns:
        List of resolved column names
    """
    console.print("\n[bold]Available Columns:[/bold]")
    
    # Show numbered list
    t = Table(box=box.SIMPLE, show_header=False)
    t.add_column("#", style="yellow", width=4, justify="center")
    t.add_column("Column", style="white")
    t.add_column("Type", style="dim", width=12)
    
    for i, col in enumerate(df.columns):
        t.add_row(str(i+1), col, str(df[col].dtype))
    console.print(t)
    
    if allow_all:
        console.print("\n[dim]Enter column numbers (e.g., 1,3,5 or 1-5) or 'all' for all columns[/dim]")
    else:
        console.print("\n[dim]Enter column numbers (e.g., 1,3,5 or 1-5)[/dim]")
    
    raw = Prompt.ask(prompt_text, default="all" if allow_all else "").strip()
    
    if allow_all and raw.upper() in ("ALL", "A"):
        return list(df.columns)
    
    # Parse number ranges
    cols = []
    parts = raw.replace("-", ":").replace(",", " ").split()
    
    for part in parts:
        if ":" in part:
            # Range like 1:5
            try:
                start, end = part.split(":")
                start, end = int(start), int(end)
                for i in range(start, min(end+1, len(df.columns)+1)):
                    if 1 <= i <= len(df.columns):
                        cols.append(df.columns[i-1])
            except:
                pass
        else:
            # Single number
            try:
                i = int(part)
                if 1 <= i <= len(df.columns):
                    cols.append(df.columns[i-1])
            except:
                # Try as column name
                resolved = resolve(part, df)
                if resolved:
                    cols.append(resolved)
    
    return list(dict.fromkeys(cols))  # Remove duplicates, preserve order


def ask_cols(prompt_text: str, df: pd.DataFrame, allow_all: bool = False) -> list:
    """
    Prompt user for column selection and resolve to actual column names.
    Can accept either numbered list or comma-separated names/letters.
    
    Args:
        prompt_text: Prompt message to display
        df: DataFrame to resolve columns against
        allow_all: If True, accept "ALL" or empty string to return all columns
        
    Returns:
        List of resolved column names
    """
    # First try numbered selection
    return ask_cols_numbered(prompt_text, df, allow_all)


# ╔══════════════════════════════════════════════════════════════════╗
# ║  VALUE FORMATTING                                                  ║
# ╚══════════════════════════════════════════════════════════════════╝

def fmt_val(v, max_len: int = 60) -> str:
    """
    Format a value for display in tables.
    
    Args:
        v: Value to format
        max_len: Maximum length before truncating
        
    Returns:
        Formatted string representation
    """
    if pd.isna(v) or v is None or str(v).strip() == "":
        return "[dim italic red]∅[/dim italic red]"
    s = str(v)
    if len(s) > max_len:
        s = s[:max_len-3] + "..."
    return s


def fmt_val_compact(v) -> str:
    """Compact value for inline display."""
    if pd.isna(v) or v is None:
        return "∅"
    s = str(v)
    return s[:25] + "..." if len(s) > 25 else s


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

    console.print(f"\n[yellow]❓ '{raw}' not found. Best matches:[/yellow]")
    t = Table(box=box.SIMPLE)
    t.add_column("#", style="yellow", width=4, justify="center")
    t.add_column("Value", style="white", min_width=20)
    t.add_column("Match", style="cyan", width=8, justify="right")
    for i, (val, score) in enumerate(good):
        t.add_row(str(i+1), val, f"{score:.0f}%")
    t.add_row("0", "[dim]Keep my original[/dim]", "")
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
# ║  LARGE DATA HELPERS                                               ║
# ╚══════════════════════════════════════════════════════════════════╝

def format_number(n: int) -> str:
    """Format large numbers with commas."""
    return f"{n:,}"


def truncate_middle(s: str, max_len: int = 40) -> str:
    """Truncate string in the middle if too long."""
    if len(s) <= max_len:
        return s
    return s[:max_len//2-2] + "..." + s[-max_len//2+1:]


# ╔══════════════════════════════════════════════════════════════════╗
# ║  MISC HELPERS                                                      ║
# ╚══════════════════════════════════════════════════════════════════╝

def find_dbs() -> list:
    """Find all SQLite database files in current directory."""
    import os
    return sorted(f for f in os.listdir(".") if f.endswith((".db", ".sqlite", ".sqlite3")))


def confirm_action(message: str, default: bool = True) -> bool:
    """
    Ask for confirmation with clear message.
    
    Args:
        message: Question to ask
        default: Default value if user just presses Enter
        
    Returns:
        True if confirmed, False otherwise
    """
    return Confirm.ask(message, default=default)
