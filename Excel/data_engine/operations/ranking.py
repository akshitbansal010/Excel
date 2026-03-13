"""
Ranking operations module.
"""
import pandas as pd
from rich.console import Console
from rich.prompt import Prompt, Confirm
from rich.rule import Rule

from ..helpers import resolve
from ..display import show_preview
from ..session import Session

console = Console()

def op_rank(sess: Session) -> pd.DataFrame:
    """Rank rows based on column values."""
    df = sess.df.copy(deep=True)  # Work on a copy to avoid mutating session state
    console.print(Rule("[bold]📊 Rank Rows[/bold]"))
    
    col = resolve(Prompt.ask("Column to rank by"), df)
    if not col:
        return df
        
    console.print("\n[bold]Ranking Methods:[/bold]")
    console.print("  [yellow]1[/yellow]  Standard (1, 2, 2, 4) - break ties by order / first")
    console.print("  [yellow]2[/yellow]  Dense (1, 2, 2, 3) - ties don't skip")
    console.print("  [yellow]3[/yellow]  Min (1, 2, 2, 4) - use lowest rank")
    console.print("  [yellow]4[/yellow]  Max (1, 3, 3, 4) - use highest rank")
    console.print("  [yellow]5[/yellow]  Average (1, 2.5, 2.5, 4)")
    console.print("  [yellow]6[/yellow]  Percentile (0.0 to 1.0)")
    
    choice = Prompt.ask("Method", choices=["1", "2", "3", "4", "5", "6"], default="2")
    
    method_map = {
        "1": "first",     # Standard (1,2,2,4): break ties by row order
        "2": "dense",     # Dense (1,2,2,3)
        "3": "min",       # Min (1,2,2,4)
        "4": "max",       # Max (1,3,3,4)
        "5": "average",   # Average (1,2.5,2.5,4)
        "6": "percentile" # Percentile (0.0 to 1.0)
    }
    method = method_map[choice]
    
    group_col = None
    if Confirm.ask("Rank within groups (e.g. rank per region)?", default=False):
        group_col = resolve(Prompt.ask("Group by column"), df)
    
    ascending = Confirm.ask("Ascending order? (Low value = Rank 1)", default=False)
    new_col = Prompt.ask("New column name", default=f"{col}_rank")
    
    try:
        # Prepare args
        rank_args = {'ascending': ascending}
        if method == "percentile":
            rank_args['pct'] = True
        else:
            rank_args['method'] = method

        if group_col:
            df[new_col] = df.groupby(group_col)[col].rank(**rank_args)
        else:
            df[new_col] = df[col].rank(**rank_args)
            
        console.print(f"[green]✔ Added rank column '{new_col}'[/green]")
        
        if Confirm.ask("Filter to top N rows?", default=False):
            try:
                if method == "percentile":
                    # Percentile rank: prompt for percentage threshold with validation
                    while True:
                        try:
                            pct = int(Prompt.ask("Top X% (0-100, default 10)", default="10"))
                            if not (0 <= pct <= 100):
                                console.print("[yellow]Please enter a value between 0 and 100.[/yellow]")
                                continue
                            break
                        except ValueError:
                            console.print("[red]Invalid input. Please enter a whole number.[/red]")
                    threshold = pct / 100.0
                    if ascending:
                        # Lower is better: filter to bottom X%
                        df = df[df[new_col] <= threshold]
                        console.print(f"[green]✔ Filtered to bottom {pct}%[/green]")
                    else:
                        # Higher is better: filter to top X%
                        df = df[df[new_col] >= (1.0 - threshold)]
                        console.print(f"[green]✔ Filtered to top {pct}%[/green]")
                else:
                    # Integer rank: filter by rank number
                    n = int(Prompt.ask("Top N (default 10)", default="10"))
                    if n <= 0:
                        console.print("[red]N must be > 0. Skipping filter.[/red]")
                    else:
                        df = df[df[new_col] <= n]
                        console.print(f"[green]✔ Filtered to top {n}[/green]")
            except ValueError:
                console.print("[red]Invalid value. Skipping filter.[/red]")
            except Exception as ve:
                console.print(f"[red]Unable to filter: {ve}[/red]")
            
    except Exception as e:
        console.print(f"[red]Rank error: {e}[/red]")
        return sess.df  # Return original on error
        
    show_preview(df)
    sess.df = df  # Write result back to session
    return df