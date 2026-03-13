"""
Helper utilities for excelpy.
Includes fuzzy matching, column resolution, Excel-style column references, and value parsing.
"""

from typing import Optional, List, Tuple, Any, Union
from datetime import datetime
import re

# RapidFuzz for fuzzy matching
try:
    from rapidfuzz import process as fz_process, fuzz
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False
    fz_process = None
    fuzz = None

from rich.console import Console
from rich.prompt import Prompt, Confirm
from rich.table import Table
from rich import box

console = Console()


# ╔══════════════════════════════════════════════════════════════════╗
# ║  EXCEL-STYLE COLUMN REFERENCES (A, B, AA, etc.)                 ║
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


def parse_col_letter(letter: str) -> int:
    """
    Convert Excel-style column letter to index (A -> 0, B -> 1, AA -> 26).
    
    Args:
        letter: Excel-style column letter (case-insensitive)
    
    Returns:
        Column index (0-based)
    
    Raises:
        ValueError: If the letter is not valid
    """
    letter = letter.upper().strip()
    if not letter:
        raise ValueError("Empty column letter")
    
    result = 0
    for char in letter:
        if not char.isalpha():
            raise ValueError(f"Invalid character in column letter: {char}")
        result = result * 26 + (ord(char) - ord('A') + 1)
    return result - 1  # Convert to 0-based


def build_col_map(columns: List[str]) -> dict:
    """
    Build a mapping from column references (letters or names) to actual column names.
    
    Args:
        columns: List of column names
    
    Returns:
        Dictionary mapping lowercase letters and column names to actual column names
    """
    m = {}
    for i, col in enumerate(columns):
        m[col_letter(i).lower()] = col
        m[col.lower()] = col
        # Also map 1-based numbers
        m[str(i + 1)] = col
    return m


def resolve_column(
    user_input: str, 
    columns: List[str], 
    allow_fuzzy: bool = True,
    threshold: int = 90
) -> Optional[str]:
    """
    Resolve user input to exact column name.
    
    Accepts:
    - Letter (A, B, AA)
    - Column name
    - Number (1, 2, 3)
    - Partial name
    
    Args:
        user_input: User's column reference
        columns: List of available column names
        allow_fuzzy: Whether to allow fuzzy matching
        threshold: Minimum fuzzy match score (0-100)
    
    Returns:
        Exact column name, or None if not found
    """
    if not user_input or not user_input.strip():
        return None
    
    user_input = user_input.strip()
    col_map = build_col_map(columns)
    key = user_input.lower()
    
    # Check direct match first (letter or exact name)
    if key in col_map:
        return col_map[key]
    
    # Try numeric index (1-based like Excel)
    try:
        idx = int(user_input)
        if 1 <= idx <= len(columns):
            return columns[idx - 1]
    except ValueError:
        pass
    
    # Try Excel-style letter
    try:
        idx = parse_col_letter(user_input)
        if 0 <= idx < len(columns):
            return columns[idx]
    except ValueError:
        pass
    
    # Fuzzy match if allowed
    if allow_fuzzy and RAPIDFUZZ_AVAILABLE:
        matches = fz_process.extract(
            user_input, 
            columns, 
            scorer=fuzz.WRatio, 
            limit=5
        )
        if matches and matches[0][1] >= threshold:
            return matches[0][0]
    
    return None


def fuzzy_match(
    user_input: str,
    choices: List[str],
    threshold: int = 60,
    limit: int = 5
) -> List[Tuple[str, float]]:
    """
    Perform fuzzy matching and return top matches with scores.
    
    Args:
        user_input: Input string to match
        choices: List of possible choices
        threshold: Minimum match score (0-100)
        limit: Maximum number of results
    
    Returns:
        List of (choice, score) tuples sorted by score
    """
    if not RAPIDFUZZ_AVAILABLE:
        # Fallback: exact match only
        if user_input in choices:
            return [(user_input, 100.0)]
        return []
    
    matches = fz_process.extract(
        user_input,
        choices,
        scorer=fuzz.WRatio,
        limit=limit
    )
    return [(m, s) for m, s, _ in matches if s >= threshold]


def fuzzy_select_column(
    columns: List[str],
    prompt_text: str = "Select column",
    default: Optional[str] = None
) -> Optional[str]:
    """
    Interactive column selection with fuzzy matching.
    If ambiguous, shows top 5 matches and allows user selection.
    
    Args:
        columns: List of available column names
        prompt_text: Prompt message
        default: Default column name
    
    Returns:
        Selected column name, or None if cancelled
    """
    if not columns:
        console.print("[red]No columns available.[/red]")
        return None
    
    # Show available columns
    console.print("\n[bold]Available Columns:[/bold]")
    t = Table(box=box.SIMPLE, show_header=False)
    t.add_column("#", style="yellow", width=4, justify="center")
    t.add_column("Column", style="white")
    t.add_column("Excel", style="dim", width=5)
    
    for i, col in enumerate(columns):
        t.add_row(str(i + 1), col, col_letter(i))
    console.print(t)
    
    # Get user input
    if default:
        prompt = f"{prompt_text} [{default}]"
    else:
        prompt = prompt_text
    
    user_input = Prompt.ask(prompt, default=default or "").strip()
    
    if not user_input:
        if default and default in columns:
            return default
        if default and default not in columns:
            import warnings
            warnings.warn(f"Default column '{default}' not found among available columns.")
            return None
        return columns[0] if columns else None
    
    # Try direct resolution first
    resolved = resolve_column(user_input, columns, allow_fuzzy=False)
    if resolved:
        return resolved
    
    # Fuzzy match
    if RAPIDFUZZ_AVAILABLE:
        matches = fz_process.extract(
            user_input,
            columns,
            scorer=fuzz.WRatio,
            limit=5
        )
        
        if not matches:
            console.print(f"[red]No matches found for '{user_input}'.[/red]")
            return None
        
        # If score >= 90, auto-accept but show and confirm
        if matches[0][1] >= 90:
            console.print(f"[green]Auto-selected: {matches[0][0]} (score: {matches[0][1]:.0f})[/green]")
            if Confirm.ask("Confirm?", default=True):
                return matches[0][0]
        
        # Show matches for selection
        console.print(f"\n[yellow]Matches for '{user_input}':[/yellow]")
        t = Table(box=box.SIMPLE)
        t.add_column("#", style="yellow", width=4, justify="center")
        t.add_column("Column", style="white")
        t.add_column("Match", style="cyan", width=8, justify="right")
        
        for i, (col, score, _) in enumerate(matches):
            t.add_row(str(i + 1), col, f"{score:.0f}%")
        t.add_row("0", "[dim]None (cancel)[/dim]", "")
        console.print(t)
        
        choice = Prompt.ask("Pick #", default="1").strip()
        
        if choice == "0":
            return None
        
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(matches):
                return matches[idx][0]
        except ValueError:
            pass
    
    console.print(f"[red]Could not resolve '{user_input}'.[/red]")
    return None


# ╔══════════════════════════════════════════════════════════════════╗
# ║  VALUE PARSING                                                    ║
# ╚══════════════════════════════════════════════════════════════════╝

def parse_value(value_str: str) -> Any:
    """
    Parse a string value into appropriate Python type.
    Supports: int, float, ISO dates, comma-lists, ranges.
    
    Args:
        value_str: String value to parse
    
    Returns:
        Parsed value (int, float, datetime, list, or string)
    """
    if not value_str or not value_str.strip():
        return None
    
    value_str = value_str.strip()
    
    # Try integer
    try:
        return int(value_str)
    except ValueError:
        pass
    
    # Try float
    try:
        return float(value_str)
    except ValueError:
        pass
    
    # Try ISO date (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)
    date_patterns = [
        r'^\d{4}-\d{2}-\d{2}$',
        r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$',
        r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',
    ]
    for pattern in date_patterns:
        if re.match(pattern, value_str):
            try:
                return datetime.fromisoformat(value_str.replace(' ', 'T'))
            except (ValueError, TypeError):
                pass
    
    # Try comma-separated list
    if ',' in value_str:
        items = [parse_value(v.strip()) for v in value_str.split(',')]
        return items
    
    # Try range (e.g., "10-20")
    if '-' in value_str and not value_str.startswith('-'):
        parts = [p.strip() for p in value_str.split('-')]
        if len(parts) == 2:
            try:
                start = parse_value(parts[0])
                end = parse_value(parts[1])
                if isinstance(start, (int, float)) and isinstance(end, (int, float)):
                    return {"start": start, "end": end, "type": "range"}
            except (ValueError, TypeError):
                pass
    
    # Return as string
    return value_str


def parse_operator(operator_str: str) -> str:
    """
    Parse and normalize operator string.
    
    Args:
        operator_str: Operator string (e.g., "==", "equals", "EQUALS")
    
    Returns:
        Normalized operator
    """
    op = operator_str.strip().upper()
    
    # Map common aliases
    op_map = {
        "EQUALS": "==",
        "EQUAL": "==",
        "=": "==",
        "==": "==",
        "NOT_EQUALS": "!=",
        "NOT EQUALS": "!=",
        "NOT_EQUAL": "!=",
        "!=": "!=",
        "GREATER_THAN": ">",
        "GREATER": ">",
        ">": ">",
        "LESS_THAN": "<",
        "LESS": "<",
        "<": "<",
        "GREATER_EQUALS": ">=",
        "GREATER_THAN_OR_EQUAL": ">=",
        ">=": ">=",
        "LESS_EQUALS": "<=",
        "LESS_THAN_OR_EQUAL": "<=",
        "<=": "<=",
        "CONTAINS": "CONTAINS",
        "LIKE": "CONTAINS",
        "NOT_CONTAINS": "NOT_CONTAINS",
        "NOT CONTAINS": "NOT_CONTAINS",
        "STARTSWITH": "STARTSWITH",
        "STARTS WITH": "STARTSWITH",
        "STARTS": "STARTSWITH",
        "ENDSWITH": "ENDSWITH",
        "ENDS WITH": "ENDSWITH",
        "ENDS": "ENDSWITH",
        "IS_NULL": "IS_NULL",
        "ISNULL": "IS_NULL",
        "IS NULL": "IS_NULL",
        "NULL": "IS_NULL",
        "BLANK": "IS_NULL",
        "IS_NOT_NULL": "IS_NOT_NULL",
        "ISNOTNULL": "IS_NOT_NULL",
        "IS NOT NULL": "IS_NOT_NULL",
        "NOT NULL": "IS_NOT_NULL",
        "NOT BLANK": "IS_NOT_NULL",
        "IS_ONE_OF": "IS_ONE_OF",
        "IN": "IS_ONE_OF",
        "IS ONE OF": "IS_ONE_OF",
    }
    
    return op_map.get(op, op)


# ╔══════════════════════════════════════════════════════════════════╗
# ║  FUZZY VALUE MATCHING (for filtering)                            ║
# ╚══════════════════════════════════════════════════════════════════╝

def fuzzy_select_value(
    user_input: str,
    available_values: List[str],
    threshold: int = 60,
    limit: int = 6
) -> Optional[str]:
    """
    Interactive value selection with fuzzy matching.
    
    Args:
        user_input: User's input value
        available_values: List of possible values
        threshold: Minimum match score
        limit: Maximum results
    
    Returns:
        Selected value, or original input if confirmed
    """
    if not RAPIDFUZZ_AVAILABLE:
        return user_input
    
    # Exact match
    if user_input in available_values:
        return user_input
    
    # Fuzzy match
    matches = fz_process.extract(
        user_input,
        available_values,
        scorer=fuzz.WRatio,
        limit=limit
    )
    good = [(m, s) for m, s, _ in matches if s >= threshold]
    
    if not good:
        console.print(f"[yellow]⚠ No close match for '{user_input}'.[/yellow]")
        return user_input if Confirm.ask("Use as-is?", default=True) else None
    
    # Auto-accept if score >= 90
    if good[0][1] >= 90:
        console.print(f"[green]Auto-selected: {good[0][0]} (score: {good[0][1]:.0f})[/green]")
        if Confirm.ask("Confirm?", default=True):
            return good[0][0]
    
    # Show matches
    console.print(f"\n[yellow]❓ '{user_input}' not found. Best matches:[/yellow]")
    t = Table(box=box.SIMPLE)
    t.add_column("#", style="yellow", width=4, justify="center")
    t.add_column("Value", style="white", min_width=20)
    t.add_column("Match", style="cyan", width=8, justify="right")
    
    for i, (val, score) in enumerate(good):
        t.add_row(str(i + 1), val, f"{score:.0f}%")
    t.add_row("0", "[dim]Keep my original[/dim]", "")
    console.print(t)
    
    choice = Prompt.ask("Pick #", default="1").strip()
    
    if choice == "0":
        return user_input
    
    try:
        idx = int(choice) - 1
        if 0 <= idx < len(good):
            return good[idx][0]
    except ValueError:
        pass
    
    return user_input


# ╔══════════════════════════════════════════════════════════════════╗
# ║  FORMAT HELPERS                                                  ║
# ╚══════════════════════════════════════════════════════════════════╝

def format_value(v: Any, max_len: int = 60) -> str:
    """Format a value for display."""
    if v is None or (isinstance(v, float) and str(v) == 'nan'):
        return "[dim italic red]∅[/dim italic red]"
    s = str(v)
    if len(s) > max_len:
        s = s[:max_len - 3] + "..."
    return s


def format_number(n: int) -> str:
    """Format large numbers with commas."""
    return f"{n:,}"


# ╔══════════════════════════════════════════════════════════════════╗
# ║  OPERATOR DISPLAY HELPERS                                        ║
# ╚══════════════════════════════════════════════════════════════════╝

def get_operator_choices() -> List[Tuple[str, str, str]]:
    """
    Get available filter operators with descriptions.
    
    Returns:
        List of (choice, operator, description) tuples
    """
    return [
        ("1", "==", "EQUALS"),
        ("2", "!=", "NOT EQUALS"),
        ("3", ">", "GREATER THAN"),
        ("4", "<", "LESS THAN"),
        ("5", ">=", "GREATER OR EQUAL"),
        ("6", "<=", "LESS OR EQUAL"),
        ("7", "CONTAINS", "Contains text"),
        ("8", "STARTSWITH", "Starts with"),
        ("9", "ENDSWITH", "Ends with"),
        ("0", "IS_NULL", "Is blank"),
        ("A", "IS_ONE_OF", "Is one of (list)"),
    ]


def show_operator_help() -> None:
    """Show filter operator options."""
    from rich.panel import Panel
    
    ops = get_operator_choices()
    lines = [f"  [yellow]{c}[/yellow]  [cyan]{op:15}[/cyan]  {desc}" 
             for c, op, desc in ops]
    
    console.print(Panel(
        "\n".join(lines),
        title="📊 Filter Operators", 
        border_style="cyan",
        padding=(0, 1)
    ))
