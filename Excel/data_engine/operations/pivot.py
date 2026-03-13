"""
Pivot table operations.
"""
import pandas as pd
from rich.console import Console
from rich.prompt import Prompt, Confirm
from rich.rule import Rule
from rich.table import Table
from rich import box

from ..helpers import resolve, fmt_val
from ..session import Session

console = Console()

def op_pivot_table(sess: Session):
    """Create a pivot table like Excel."""
    df = sess.df
    console.print(Rule("[bold]📊 Pivot Table[/bold]"))
    
    rows_in = Prompt.ask("Rows (group by columns, comma-sep)").strip()
    if not rows_in: return
    index = [resolve(c.strip(), df) for c in rows_in.split(",") if c.strip()]
    index = [c for c in index if c]
    if not index: return

    cols_in = Prompt.ask("Columns (optional, comma-sep)", default="").strip()
    columns = [resolve(c.strip(), df) for c in cols_in.split(",") if c.strip()]
    columns = [c for c in columns if c]

    val_in = Prompt.ask("Values (column to aggregate)").strip()
    values = resolve(val_in, df)
    if not values: return

    agg_map = {"1":"sum", "2":"mean", "3":"count", "4":"min", "5":"max", "6":"nunique"}
    agg_c = Prompt.ask("Function: [1]sum [2]mean [3]count [4]min [5]max [6]unique", choices=list(agg_map.keys()), default="1")
    aggfunc = agg_map[agg_c]

    try:
        pivot = pd.pivot_table(df, values=values, index=index, columns=columns, aggfunc=aggfunc, fill_value=0)
        console.print(f"\n[bold]Pivot: {aggfunc}({values})[/bold]")
        
        display_df = pivot.reset_index()
        t = Table(box=box.SIMPLE)
        for c in display_df.columns:
            t.add_column(str(c))
        for _, row in display_df.head(20).iterrows():
            t.add_row(*[fmt_val(v) for v in row])
        console.print(t)

        if Confirm.ask("Save as new table?", default=True):
            name = Prompt.ask("Table name", default="pivot_result")
            sess.add(name, pivot.reset_index())
            
    except Exception as e:
        console.print(f"[red]Pivot error: {e}[/red]")