"""
Session I/O module — save and export operations.
"""

import os
from datetime import datetime

from rich.console import Console
from rich.prompt import Prompt
from rich.rule import Rule

from ..database import db_save
from ..session import Session

console = Console()


def op_save(sess: Session):
    """Save to database."""
    console.print(Rule("[bold]Save to Database[/bold]"))
    tables = sess.list_tables()
    for i, n in enumerate(tables):
        console.print(f"  [yellow]{i+1}[/yellow]  {n}  "
                      f"({len(sess.tables[n]):,}r × {len(sess.tables[n].columns)}c)")

    try:
        default_idx = str(tables.index(sess.active) + 1)
    except ValueError:
        default_idx = "1"

    c = Prompt.ask("Which table to save?", default=default_idx)
    try:
        tname = tables[int(c) - 1]
    except (ValueError, IndexError):
        tname = sess.active

    db_basename = os.path.basename(sess.db_path) if sess.db_path else "database.db"
    out_db = Prompt.ask("Save to DB path", default=f"work_{db_basename}")
    out_table = Prompt.ask("Table name in DB", default=tname)
    exists    = Prompt.ask("If table exists: [R]eplace / [A]ppend", default="R").upper()

    try:
        db_save(sess.tables[tname], out_db, out_table,
                if_exists="replace" if exists=="R" else "append")
        console.print("[dim]Your source database was NOT modified.[/dim]")
    except Exception as e:
        console.print(f"[red]❌ Save error: {e}[/red]")


def op_export(sess: Session):
    """Export to CSV or Excel."""
    console.print(Rule("[bold]Export[/bold]"))
    tables = sess.list_tables()
    for i, n in enumerate(tables): console.print(f"  [yellow]{i+1}[/yellow]  {n}")

    try:
        default_idx = str(tables.index(sess.active) + 1)
    except ValueError:
        default_idx = "1"

    c = Prompt.ask("Which table?", default=default_idx)
    try:
        tname = tables[int(c) - 1]
    except (ValueError, IndexError):
        tname = sess.active

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
