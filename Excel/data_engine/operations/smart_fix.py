"""
Smart Fix module — scan and fix column type issues.
"""

import pandas as pd

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.rule import Rule
from rich.table import Table

from ..display import show_preview
from ..session import Session

console = Console()


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
                except Exception as e:
                    console.print(f"[yellow]Warning: date parsing check failed for {col}: {e}[/yellow]")
        
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
        issue_categories = sum(1 for v in issues.values() if v)
        console.print(Panel(f"[yellow]⚠ Found {total_issues} potential issues across {issue_categories} issue categories.[/yellow]\n\n"
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
    
    opt_map = {
        "date_like_strings": "Convert date-like strings to actual dates",
        "float_id_precision": "Convert float IDs to integers (avoid precision loss)",
        "high_nulls": "Drop columns with 90%+ nulls",
        "unique_values": "Mark columns as likely IDs (for your reference)",
        "mixed_types": "Standardize mixed-type columns",
    }
    for i, (issue_type, cols) in enumerate(options):
        text = opt_map.get(issue_type, issue_type)
        label = f"{i+1}. {text}"
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
                df[col] = pd.to_datetime(df[col], errors='coerce')
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
