#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║            DataEngine Pro  v2.0  —  Your Data Cockpit            ║
║   Excel-power. Python speed. DB-connected. Zero BS.              ║
╚══════════════════════════════════════════════════════════════════╝

pip install rich pandas numpy openpyxl rapidfuzz

Run:
    python dataengine_pro.py
    python data_engine.py --db my_database.db
"""

# ── Auto-install deps ──────────────────────────────────────────────────────────
import subprocess, sys

def _ensure(pkg, import_as=None):
    name = import_as or pkg
    try:
        __import__(name)
    except ImportError:
        print(f"Installing {pkg}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "--quiet"])

for _p, _i in [("rich","rich"),("rapidfuzz","rapidfuzz"),("openpyxl","openpyxl")]:
    _ensure(_p, _i)

# ── Imports ────────────────────────────────────────────────────────────────────
import os
import sys
import argparse
import pandas as pd

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich import box

# Import core modules from the package
from data_engine.session import Session
from data_engine import operations as ops
from data_engine.display import (
    show_banner, show_menu, status_bar, show_columns,
    show_preview, show_unique_full, show_null_report
)
from data_engine.database import db_tables, db_load
from data_engine.helpers import resolve, ask_cols, find_dbs

console = Console()

# ╔══════════════════════════════════════════════════════════════════╗
# ║  STARTUP                                                         ║
# ╚══════════════════════════════════════════════════════════════════╝

def startup(cli_db: str = None) -> Session:
    show_banner()

    # ── Pick DB ───────────────────────────────────────────────────
    if cli_db:
        db_path = cli_db
    else:
        dbs = find_dbs()
        if dbs:
            t = Table(box=box.SIMPLE, show_header=False)
            t.add_column("Ref", style="yellow", width=4)
            t.add_column("File", style="white")
            console.print("\n[bold]Databases found:[/bold]")
            for i, db in enumerate(dbs):
                kb = os.path.getsize(db)/1024
                t.add_row(str(i+1), f"{db}  [dim]({kb:,.0f} KB)[/dim]")
            console.print(t)
            console.print("  [dim]N = custom path[/dim]")
            c = Prompt.ask("Pick DB", default="1")
            if c.upper() == "N":
                db_path = Prompt.ask("Path to .db file")
            else:
                try:    db_path = dbs[int(c)-1]
                except: db_path = c
        else:
            console.print("[yellow]No .db files found here.[/yellow]")
            db_path = Prompt.ask("Full path to SQLite database")

    if not os.path.exists(db_path):
        console.print(f"[red]❌ Not found: {db_path}[/red]"); sys.exit(1)

    # ── Pick Table ────────────────────────────────────────────────
    tables = db_tables(db_path)
    if not tables:
        console.print("[red]❌ No tables in database.[/red]"); sys.exit(1)

    t = Table(box=box.SIMPLE, show_header=False)
    t.add_column("Ref", style="yellow", width=4)
    t.add_column("Table", style="white")
    console.print(f"\n[bold]Tables in [cyan]{db_path}[/cyan]:[/bold]")
    for i, tbl in enumerate(tables):
        t.add_row(str(i+1), tbl)
    console.print(t)

    c = Prompt.ask("Pick table", default="1")
    try:    table_name = tables[int(c)-1]
    except: table_name = c

    console.print(f"\n[dim]Loading [bold]{table_name}[/bold]…[/dim]")
    df = db_load(db_path, table_name)
    df.columns = [col.strip() for col in df.columns]
    console.print(f"[bold green]✔ Loaded {len(df):,} rows × {len(df.columns)} columns.[/bold green]")

    sess = Session(db_path)
    sess.add(table_name, df)           # always keep original untouched

    # ── Work mode ─────────────────────────────────────────────────
    console.print(Panel(
        "[bold]How do you want to work?[/bold]\n\n"
        "  [bold yellow]1[/bold yellow]  Create a [bold]working copy[/bold] "
                        "(recommended — original stays locked)\n"
        "  [bold yellow]2[/bold yellow]  Work directly on loaded data\n"
        "  [bold yellow]3[/bold yellow]  Create a [bold]slim table[/bold] "
                        "— pick only the columns you need",
        border_style="cyan"
    ))
    mode = Prompt.ask("Mode", choices=["1","2","3"], default="1")

    if mode == "1":
        copy_name = Prompt.ask("Working table name", default=f"{table_name}_work")
        sess.add(copy_name, df.copy())
        console.print(f"[green]✔ Working on '[bold]{copy_name}[/bold]'. "
                      f"Original '[dim]{table_name}[/dim]' is locked.[/green]")

    elif mode == "3":
        show_columns(df)
        cols = ask_cols("Columns to include (comma-sep letters or names)", df)
        if not cols: cols = list(df.columns)
        slim_name = Prompt.ask("Slim table name", default=f"{table_name}_slim")
        sess.add(slim_name, df[cols].copy())
        console.print(f"[green]✔ Slim table '[bold]{slim_name}[/bold]' "
                      f"with {len(cols)} columns.[/green]")

    show_columns(sess.df)
    show_preview(sess.df, n=5, title=f"▸ {sess.active}")
    return sess


# ╔══════════════════════════════════════════════════════════════════╗
# ║  MAIN LOOP                                                       ║
# ╚══════════════════════════════════════════════════════════════════╝

def main():
    parser = argparse.ArgumentParser(description="DataEngine Pro v2")
    parser.add_argument("--db", default=None, help="Path to SQLite .db file")
    args   = parser.parse_args()

    sess = startup(cli_db=args.db)
    show_menu()
    status_bar(sess)

    while True:
        try:
            choice = Prompt.ask(
                f"\n[bold cyan]DataEngine[/bold cyan] "
                f"[dim]({sess.active} │ {len(sess.df):,}r × {len(sess.df.columns)}c)[/dim]"
            ).strip().upper()
        except (KeyboardInterrupt, EOFError):
            console.print("\n[dim]Ctrl+C — type [bold]0[/bold] to exit.[/dim]")
            continue

        if not choice: continue

        try:
            # VIEW
            if   choice == "H": show_menu()
            elif choice == "C": show_columns(sess.df)
            elif choice == "P": ops.op_preview(sess)
            elif choice == "U":
                show_columns(sess.df, compact=True)
                ci  = Prompt.ask("Column")
                col = resolve(ci, sess.df)
                if col: show_unique_full(sess.df, col)
                else:   console.print(f"[red]Not found: {ci}[/red]")
            elif choice == "N": show_null_report(sess.df)
            elif choice == "S": ops.op_search(sess)
            elif choice == "I": ops.op_stats(sess)

            # TRANSFORM
            elif choice == "1": sess.push_undo(); sess.df = ops.op_filter(sess)
            elif choice == "2": sess.push_undo(); sess.df = ops.op_add_column(sess)
            elif choice == "3": ops.op_aggregate(sess)
            elif choice == "4": sess.push_undo(); sess.df = ops.op_sort(sess)
            elif choice == "5": sess.push_undo(); sess.df = ops.op_handle_nulls(sess)
            elif choice == "6": sess.push_undo(); sess.df = ops.op_rename_drop(sess)
            elif choice == "7": sess.push_undo(); sess.df = ops.op_dedupe(sess)
            elif choice == "8": ops.op_pivot(sess)
            elif choice == "9": sess.push_undo(); sess.df = ops.op_change_type(sess)
            elif choice == "J": sess.push_undo(); ops.op_join(sess)

            # TABLE MANAGER
            elif choice == "T": ops.op_table_manager(sess)
            elif choice == "K": ops.op_switch_table(sess)

            # SESSION
            elif choice == "Z":
                if sess.undo():
                    console.print(f"[yellow]↩ Undone → {len(sess.df):,}r × {len(sess.df.columns)}c[/yellow]")
                else:
                    console.print("[dim]Nothing to undo.[/dim]")

            elif choice == "R":
                origin = sess.list_tables()[0]
                if Confirm.ask(f"Reset [bold]{sess.active}[/bold] to original '{origin}'?", default=False):
                    sess.df = sess.tables[origin].copy()
                    sess.history[sess.active].clear()
                    console.print(f"[yellow]↩ Reset — {len(sess.df):,} rows.[/yellow]")

            elif choice == "W": ops.op_save(sess)
            elif choice == "E": ops.op_export(sess)

            elif choice == "0":
                if Confirm.ask("[bold]Exit DataEngine Pro?[/bold]", default=True):
                    console.print(Panel(
                        "[bold green]Session ended.[/bold green]\n"
                        "[dim]All unsaved changes discarded.[/dim]",
                        border_style="cyan"
                    ))
                    break

            else:
                console.print(f"[dim]Unknown '{choice}'. Type [bold]H[/bold] for menu.[/dim]")

        except Exception as e:
            console.print(f"[bold red]❌ Unexpected error:[/bold red] {e}")
            console.print("[dim]Your data is intact. Try again or type Z to undo.[/dim]")

        status_bar(sess)


if __name__ == "__main__":
    main()