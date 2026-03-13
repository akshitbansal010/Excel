"""
Ranking operations module.
"""
import pandas as pd
from rich.console import Console
from rich.prompt import Prompt, Confirm
from rich.rule import Rule

from ..helpers import resolve, show_preview
from ..session import Session

console = Console()

def op_rank(sess: Session) -> pd.DataFrame:
    """Rank rows based on column values."""
    df = sess.df
    console.print(Rule("[bold]📊 Rank Rows[/bold]"))
    
    col = resolve(Prompt.ask("Column to rank by"), df)
    if not col:
        return df
        
    console.print("\n[bold]Ranking Methods:[/bold]")
    console.print("  [yellow]1[/yellow]  Standard (1, 2, 2, 4) - ties skip rank")
    console.print("  [yellow]2[/yellow]  Dense (1, 2, 2, 3) - ties don't skip")
    console.print("  [yellow]3[/yellow]  Min (1, 2, 2, 4) - use lowest rank")
    console.print("  [yellow]4[/yellow]  Max (1, 3, 3, 4) - use highest rank")
    console.print("  [yellow]5[/yellow]  Average (1, 2.5, 2.5, 4)")
    console.print("  [yellow]6[/yellow]  Percentile (0.0 to 1.0)")
    
    choice = Prompt.ask("Method", choices=["1", "2", "3", "4", "5", "6"], default="2")
    
    method_map = {
        "1": "min",
        "2": "dense",
        "3": "min", 
        "4": "max",
        "5": "average",
        "6": "percentile"
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
            n = int(Prompt.ask("N", default="10"))
            df = df[df[new_col] <= n]
            console.print(f"[green]✔ Filtered to top {n}[/green]")
            
    except Exception as e:
        console.print(f"[red]Rank error: {e}[/red]")
        
    show_preview(df)
    return df