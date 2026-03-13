"""
Filter module — all filtering operations.
Improved with Excel-like guided experience.
"""

import pandas as pd

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.rule import Rule

from ..display import show_columns, show_preview, show_unique_inline
from ..helpers import fuzzy_pick_value, fuzzy_pick_values_list, resolve, ask_cols
from ..session import Session

console = Console()


def _show_filter_help():
    """Show filter operator options in Excel-like format."""
    console.print(Panel(
        "[bold]Filter Operators - Choose one:[/bold]\n\n"
        "  [bold cyan]EQUALS[/bold cyan]        [dim]==[/dim]      Match exactly\n"
        "  [bold cyan]NOT EQUALS[/bold cyan]    [dim]!=[/dim]      Does not match\n"
        "  [bold cyan]GREATER THAN[/bold cyan]  [dim]>[/dim]       Numbers/text after\n"
        "  [bold cyan]LESS THAN[/bold cyan]     [dim]<[/dim]       Numbers/text before\n"
        "  [bold cyan]CONTAINS[/bold cyan]              Has this text anywhere\n"
        "  [bold cyan]NOT CONTAINS[/bold cyan]          Does not have text\n"
        "  [bold cyan]STARTS WITH[/bold cyan]           Begins with\n"
        "  [bold cyan]ENDS WITH[/bold cyan]             Ends with\n"
        "  [bold cyan]IS BLANK[/bold cyan]              Empty/null cells\n"
        "  [bold cyan]IS NOT BLANK[/bold cyan]          Has any value\n"
        "  [bold cyan]IS ONE OF[/bold cyan]       [dim]IN[/dim]   Any of these values\n"
        "\n[dim]Examples:[/dim]\n"
        "  [dim]Price == 100[/dim]\n"
        "  [dim]Name CONTAINS 'John'[/dim]\n"
        "  [dim]Status IS ONE OF Active,Pending",
        title="📊 Filter Options", border_style="cyan", padding=(0, 1)
    ))


def apply_single_condition(df: pd.DataFrame, col: str, op: str, val) -> pd.Series:
    """Apply a single filter condition and return boolean mask."""
    if op in ("IS NULL", "ISNULL", "NULL", "IS BLANK", "BLANK"):
        return df[col].isna() | (df[col].astype(str).str.strip() == "")
    
    if op in ("IS NOT NULL", "ISNOTNULL", "NOT NULL", "IS NOT BLANK", "NOT BLANK"):
        return df[col].notna() & (df[col].astype(str).str.strip() != "")
    
    if op in ("IN", "NOT IN", "IS ONE OF"):
        raw_list = [v.strip().strip("'\"") for v in str(val).split(",")]
        try:
            num_list = [float(v) for v in raw_list]
            mask = df[col].isin(num_list)
        except:
            mask = df[col].astype(str).str.strip().isin(raw_list)
        return mask if op in ("IN", "IS ONE OF") else ~mask
    
    if op in ("CONTAINS", "~"):
        return df[col].astype(str).str.contains(val, case=False, na=False)
    
    if op in ("NOT CONTAINS", "!~"):
        return ~df[col].astype(str).str.contains(val, case=False, na=False)
    
    if op in ("STARTSWITH", "STARTS WITH", "STARTS"):
        return df[col].astype(str).str.startswith(val, na=False)
    
    if op in ("ENDSWITH", "ENDS WITH", "ENDS"):
        return df[col].astype(str).str.endswith(val, na=False)
    
    # Standard comparison operators
    try:
        num_val = float(val)
        return pd.Series(False, index=df.index).where(False, (df[col] > num_val) if op == ">" else (df[col] < num_val) if op == "<" else (df[col] >= num_val) if op == ">=" else (df[col] <= num_val) if op == "<=" else (df[col] == num_val) if op == "==" else (df[col] != num_val))
    except ValueError:
        # String comparison
        col_series = df[col].astype(str)
        if op == "==":
            return col_series == str(val)
        elif op == "!=":
            return col_series != str(val)
        elif op == ">":
            return col_series > str(val)
        elif op == "<":
            return col_series < str(val)
        elif op == ">=":
            return col_series >= str(val)
        elif op == "<=":
            return col_series <= str(val)
        else:
            return pd.Series(False, index=df.index)


def op_filter(sess: Session) -> pd.DataFrame:
    """Basic filter operation - Excel-like guided experience."""
    df = sess.df
    console.print(Rule("[bold]🔍 Filter Rows (Keep Matching)[/bold]"))
    show_columns(df, compact=True)

    # Step 1: Choose column
    console.print("\n[bold]Step 1:[/bold] Which column do you want to filter on?")
    col = resolve(Prompt.ask("Column (letter A,B,C or name)", 
                             default=col_letter(0) if len(df.columns) > 0 else ""), df)
    if not col:
        console.print("[red]Column not found. Try again.[/red]"); return df

    # Step 2: Show values in that column so user knows what to type
    console.print()
    show_unique_inline(df, col)

    # Step 3: Choose operator
    console.print("\n[bold]Step 2:[/bold] How do you want to filter?")
    _show_filter_help()
    
    # Show common options as numbered choices for easy use
    console.print("\n[bold]Quick options:[/bold]")
    console.print("  [yellow]1[/yellow]  EQUALS (==)        [yellow]2[/yellow]  NOT EQUALS (!=)")
    console.print("  [yellow]3[/yellow]  GREATER THAN (>)   [yellow]4[/yellow]  LESS THAN (<)")
    console.print("  [yellow]5[/yellow]  CONTAINS         [yellow]6[/yellow]  IS BLANK")
    console.print("  [yellow]7[/yellow]  IS ONE OF (IN)    [yellow]8[/yellow]  Custom operator")
    
    choice = Prompt.ask("Choose filter type", choices=["1","2","3","4","5","6","7","8"]).strip()
    
    op_map = {
        "1": "==", "2": "!=", "3": ">", "4": "<",
        "5": "CONTAINS", "6": "IS BLANK", "7": "IS ONE OF"
    }
    
    if choice == "8":
        console.print("\n[dim]Available: == != > < >= <= CONTAINS NOT CONTAINS STARTSWITH ENDSWITH IS BLANK IS ONE OF[/dim]")
        op = Prompt.ask("Operator").strip().upper()
    else:
        op = op_map.get(choice, "==")
        console.print(f"[dim]Using: {op}[/dim]")

    # Step 4: Get value based on operator
    if op in ("IS BLANK", "ISNULL", "NULL", "BLANK"):
        new_df = df[df[col].isna() | (df[col].astype(str).str.strip()=="")]
        console.print(f"[green]✔ {len(new_df):,} rows where {col} IS BLANK[/green]")
        show_preview(new_df, n=5); return new_df

    if op in ("IS NOT BLANK", "ISNOTNULL", "NOT NULL"):
        new_df = df[df[col].notna() & (df[col].astype(str).str.strip()!="")]
        console.print(f"[green]✔ {len(new_df):,} rows where {col} IS NOT BLANK[/green]")
        show_preview(new_df, n=5); return new_df

    # Get value for operators that need one
    if op == "IS ONE OF":
        console.print("\n[bold]Step 3:[/bold] Enter values separated by commas")
        console.print("[dim]Example: Active, Pending, Draft[/dim]")
        val_raw = Prompt.ask("Values").strip()
    elif op == "CONTAINS":
        console.print("\n[bold]Step 3:[/bold] What text should be contained?")
        console.print("[dim]Example: john, error, @company.com[/dim]")
        val_raw = Prompt.ask("Contains text").strip()
    else:
        console.print("\n[bold]Step 3:[/bold] Enter the value to match")
        console.print("[dim]Example: 100, john@example.com, Active[/dim]")
        val_raw = Prompt.ask("Value").strip()

    try:
        if op in ("IS ONE OF", "IN"):
            raw_list = [v.strip().strip("'\"") for v in val_raw.split(",")]
            resolved_list = fuzzy_pick_values_list(raw_list, df, col)
            if not resolved_list:
                console.print("[red]No values resolved.[/red]"); return df
            try:
                num_list = [float(v) for v in resolved_list]
                mask = df[col].isin(num_list)
            except ValueError:
                mask = df[col].astype(str).str.strip().isin(resolved_list)
            new_df = df[mask]

        elif op in ("CONTAINS",):
            new_df = df[df[col].astype(str).str.contains(val_raw, case=False, na=False)]

        elif op in ("NOT CONTAINS", "!~"):
            new_df = df[~df[col].astype(str).str.contains(val_raw, case=False, na=False)]

        elif op in ("STARTSWITH", "STARTS WITH"):
            new_df = df[df[col].astype(str).str.startswith(val_raw, na=False)]

        elif op in ("ENDSWITH", "ENDS WITH"):
            new_df = df[df[col].astype(str).str.endswith(val_raw, na=False)]

        else:
            actual = val_raw
            if op in ("==","!="):
                actual = fuzzy_pick_value(val_raw, df, col) or val_raw
            try:
                num_v = float(actual)
                new_df = df.query(f"`{col}` {op} {num_v}")
            except ValueError:
                new_df = df.query(f"`{col}` {op} '{actual}'")

        removed = len(df)-len(new_df)
        console.print(
            f"\n[green]✔ Filter applied![/green] "
            f"[bold]{len(new_df):,}[/bold] rows kept ([yellow]{removed}[/yellow] removed)"
        )
        
        # Show summary
        if removed > 0:
            console.print(f"[dim]Showing {min(5, len(new_df))} rows of {len(new_df):,} total[/dim]")
        show_preview(new_df, n=5, title="Filtered Result")
        return new_df

    except Exception as e:
        console.print(f"[red]❌ Filter error: {e}[/red]")
        return df


def col_letter(idx: int) -> str:
    """Convert column index to Excel-style letter (0 -> A, 1 -> B, 26 -> AA)."""
    result, idx = "", idx + 1
    while idx > 0:
        idx, r = divmod(idx - 1, 26)
        result = chr(65 + r) + result
    return result


def op_multi_filter(sess: Session) -> pd.DataFrame:
    """
    Multi-condition filter - stack multiple conditions and apply together.
    Similar to Excel AutoFilter with AND/OR logic.
    """
    df = sess.df
    console.print(Rule("[bold]🔍 Multi-Condition Filter (Excel AutoFilter)[/bold]"))
    
    console.print(Panel(
        "[bold]Add conditions to filter your data.[/bold]\n"
        "You can combine with AND (both must match) or OR (either can match).\n\n"
        "Press 0 when done to apply filter.",
        title="Multi-Filter", border_style="cyan"
    ))
    
    conditions = []  # List of (column, operator, value, logic_type)
    
    while True:
        # Show current conditions as visual chips
        if conditions:
            console.print("\n[bold green]Current Filter:[/bold green]")
            for i, (col, op, val, logic) in enumerate(conditions):
                chip = f"[cyan]{col}[/cyan] [yellow]{op}[/yellow] [white]{val}[/white]"
                if logic == "AND":
                    chip += " [green]AND[/green]"
                elif logic == "OR":
                    chip += " [magenta]OR[/magenta]"
                console.print(f"  {i+1}. {chip}  [dim](type {i+1} to remove)[/dim]")
        else:
            console.print("\n[dim]No conditions yet. Add your first filter condition below.[/dim]")
        
        console.print("\n[bold]Options:[/bold]")
        console.print("  [bold yellow]1[/bold yellow]  Add new condition")
        console.print("  [bold yellow]2[/bold yellow]  Apply filter (done)")
        console.print("  [bold yellow]3[/bold yellow]  Clear all")
        console.print("  [bold yellow]0[/bold yellow]  Cancel (back)")
        
        action = Prompt.ask("\nWhat do you want to do?", 
                           choices=["1", "2", "3", "0"], default="1")
        
        if action == "0":
            return df
        
        if action == "3":
            conditions.clear()
            console.print("[dim]All conditions cleared.[/dim]")
            continue
        
        if action == "2":
            if not conditions:
                console.print("[yellow]No conditions to apply. Add at least one.[/yellow]")
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
            console.print(f"\n[green]✔ Filter applied![/green]")
            console.print(f"[bold]{len(result):,}[/bold] rows kept ([yellow]{removed}[/yellow] removed)")
            show_preview(result, n=8, title="Filtered Result")
            
            if Confirm.ask("\nApply this filter to your working table?", default=True):
                return result
            return df
        
        if action == "1":
            # Add a new condition - guided wizard
            console.print("\n[bold]--- Add Condition ---[/bold]")
            
            # Step 1: Choose column
            show_columns(df, compact=True)
            col = resolve(Prompt.ask("Column to filter on", 
                                    default=col_letter(0)), df)
            if not col:
                console.print("[red]Column not found.[/red]"); continue
            
            # Show values
            show_unique_inline(df, col)
            
            # Step 2: Choose operator
            console.print("\n[bold]Choose how to filter:[/bold]")
            console.print("  [yellow]1[/yellow]  EQUALS (==)      [yellow]2[/yellow]  NOT EQUALS (!=)")
            console.print("  [yellow]3[/yellow]  GREATER THAN    [yellow]4[/yellow]  LESS THAN")
            console.print("  [yellow]5[/yellow]  CONTAINS        [yellow]6[/yellow]  IS BLANK")
            console.print("  [yellow]7[/yellow]  IS ONE OF       [yellow]8[/yellow]  Custom")
            
            op_choice = Prompt.ask("Operator", choices=["1","2","3","4","5","6","7","8"]).strip()
            
            op_map = {
                "1": "==", "2": "!=", "3": ">", "4": "<",
                "5": "CONTAINS", "6": "IS BLANK", "7": "IS ONE OF"
            }
            
            if op_choice == "8":
                op = Prompt.ask("Enter operator", default="==").strip().upper()
            else:
                op = op_map.get(op_choice, "==")
            
            # Step 3: Get value
            if op in ("IS BLANK",):
                val = None
            elif op == "IS ONE OF":
                val = Prompt.ask("Values (comma-separated)", default="").strip()
            else:
                val = Prompt.ask("Value to match", default="").strip()
            
            # Step 4: AND/OR logic
            if conditions:
                logic = Prompt.ask("Combine with [A]ND or [O]R?", default="A").upper()
            else:
                logic = "AND"  # First condition
            
            conditions.append((col, op, val, logic))
            display_val = "BLANK" if op == "IS BLANK" else (val if val else "BLANK")
            console.print(f"[green]✔ Added: {col} {op} {display_val}[/green]")
    
    return df


def op_filter_by_color(sess: Session) -> pd.DataFrame:
    """Filter by flag column (like Excel's filter by color)."""
    df = sess.df
    console.print(Rule("[bold]🚩 Filter by Flag / Status[/bold]"))
    
    console.print(Panel(
        "Filter rows based on flag/checkbox columns.\n"
        "Useful for: Active status, Yes/No, True/False, 0/1 flags",
        title="Filter by Flag", border_style="yellow"
    ))
    
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
        console.print("[dim]Tip: Create a flag column using option 2 (Add column > Conditional).[/dim]")
        return df
    
    console.print("\n[bold]Found these flag columns:[/bold]")
    for i, col in enumerate(flag_cols):
        true_count = (df[col] == True).sum() if df[col].dtype == 'bool' else (df[col] == 1).sum() if df[col].dtype in ('int64', 'Int64', 'float64') else (df[col].str.lower() == 'yes').sum()
        false_count = len(df) - true_count - df[col].isna().sum()
        console.print(f"  [yellow]{i+1}[/yellow]  {col}  [dim](True: {true_count:,}, False: {false_count:,})[/dim]")
    
    choice = Prompt.ask("\nFilter by which column?", 
                        choices=[str(i+1) for i in range(len(flag_cols))],
                        default="1")
    col = flag_cols[int(choice)-1]
    
    # Determine values to keep - make it easy with numbered options
    console.print(f"\n[bold]Filter '{col}' to show:[/bold]")
    
    if df[col].dtype == 'bool':
        console.print("  [yellow]1[/yellow]  TRUE (checked)  [yellow]2[/yellow]  FALSE (unchecked)")
        choice = Prompt.ask("Keep", choices=["1","2"], default="1")
        result = df[df[col] == (choice == "1")]
    elif df[col].dtype in ('int64', 'Int64', 'float64'):
        console.print("  [yellow]1[/yellow]  1 (flagged)  [yellow]2[/yellow]  0 (not flagged)")
        choice = Prompt.ask("Keep", choices=["1","2"], default="1")
        flag_val = 1 if choice == "1" else 0
        result = df[df[col] == flag_val]
    else:
        console.print("  [yellow]1[/yellow]  Yes/True  [yellow]2[/yellow]  No/False")
        choice = Prompt.ask("Keep", choices=["1","2"], default="1")
        truthy = {'y', 'yes', 'true'}
        falsy = {'n', 'no', 'false'}
        keep_set = truthy if choice == "1" else falsy
        result = df[df[col].astype(str).str.lower().isin(keep_set)]
    
    removed = len(df) - len(result)
    console.print(f"\n[green]✔ Filter applied![/green] {len(result):,} rows kept ({removed} removed)")
    show_preview(result, n=5)
    return result
