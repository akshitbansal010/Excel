"""
Join operations.
"""
import pandas as pd
from rich.console import Console
from rich.prompt import Prompt, Confirm
from rich.rule import Rule

from ..helpers import resolve, show_preview
from ..session import Session

console = Console()

def op_join_tables(sess: Session):
    """Join two tables."""
    console.print(Rule("[bold]🔗 Join Tables[/bold]"))
    
    tables = sess.list_tables()
    if len(tables) < 2:
        console.print("[yellow]Need at least 2 tables.[/yellow]")
        return

    for i, t in enumerate(tables):
        console.print(f"  {i+1}. {t}")
    
    while True:
        try:
            choice1 = int(Prompt.ask("Left table #", default="1"))
            if 1 <= choice1 <= len(tables):
                t1 = tables[choice1 - 1]
                break
            console.print(f"[red]Please enter a number between 1 and {len(tables)}[/red]")
        except ValueError:
            console.print("[red]Please enter a valid number[/red]")
    
    while True:
        try:
            choice2 = int(Prompt.ask("Right table #", default="2"))
            if 1 <= choice2 <= len(tables):
                t2 = tables[choice2 - 1]
                break
            console.print(f"[red]Please enter a number between 1 and {len(tables)}[/red]")
        except ValueError:
            console.print("[red]Please enter a valid number[/red]")
    
    df1 = sess.tables[t1]
    df2 = sess.tables[t2]
    
    k1 = resolve(Prompt.ask(f"Key in {t1}"), df1)
    k2 = resolve(Prompt.ask(f"Key in {t2}"), df2)
    
    how = Prompt.ask("Type", choices=["left", "right", "inner", "outer", "cross"], default="left")
    
    try:
        res = pd.merge(df1, df2, left_on=k1, right_on=k2, how=how, suffixes=('_L', '_R'))
        console.print(f"[green]✔ Result: {len(res):,} rows[/green]")
        show_preview(res)
        if Confirm.ask("Save?", default=True):
            sess.add(Prompt.ask("Name", default=f"{t1}_{how}_{t2}"), res)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")