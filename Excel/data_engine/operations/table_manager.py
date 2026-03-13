"""
Table Manager module — operations for managing tables.
"""

import os
import pandas as pd

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.rule import Rule
from rich.table import Table

from ..database import db_load, db_save, db_tables
from ..display import show_columns, show_preview
from ..helpers import ask_cols
from ..session import Session

console = Console()


def op_table_manager(sess: Session):
    """Table Manager — create, clone, slim, stack, import, delete tables."""
    while True:
        console.print(Rule("[bold]Table Manager[/bold]"))
        t = Table(title="Session Tables", box=box.ROUNDED, title_style="bold cyan")
        t.add_column("●",     width=3, justify="center")
        t.add_column("Name",  style="bold white", min_width=20)
        t.add_column("Rows",  justify="right", style="cyan")
        t.add_column("Cols",  justify="right", style="magenta")
        t.add_column("Columns (preview)", style="dim", min_width=30)

        for name, tdf in sess.tables.items():
            marker   = "[bold green]●[/bold green]" if name == sess.active else ""
            col_prev = ", ".join(tdf.columns[:6].tolist())
            if len(tdf.columns) > 6: col_prev += f" … +{len(tdf.columns)-6}"
            t.add_row(marker, name, f"{len(tdf):,}", str(len(tdf.columns)), col_prev)
        console.print(t)

        console.print(Panel(
            "  [bold]1[/bold]  Load existing table from DB into session\n"
            "  [bold]2[/bold]  Clone active table (in-session copy)\n"
            "  [bold]3[/bold]  Slim — keep only chosen columns → new table\n"
            "  [bold]4[/bold]  Stack / Append two tables vertically\n"
            "  [bold]5[/bold]  Delete a session table\n"
            "  [bold]6[/bold]  Preview a table\n"
            "  [bold]7[/bold]  Create BRAND NEW empty table → save to DB\n"
            "  [bold]8[/bold]  Save any session table → DB (create/overwrite)\n"
            "  [bold]9[/bold]  List all tables currently in DB\n"
            "  [bold]X[/bold]  Import Excel (.xlsx) → Session\n"
            "  [bold]0[/bold]  Back",
            title="Actions", border_style="dim"
        ))
        action = Prompt.ask("Action", choices=["0","1","2","3","4","5","6","7","8","9","X","x"]).upper()

        if action == "0": break

        elif action == "1":
            tbls = db_tables(sess.db_path)
            if not tbls:
                console.print("[red]No tables in DB.[/red]"); continue
            for i, n in enumerate(tbls): console.print(f"  [yellow]{i+1}[/yellow]  {n}")
            c = Prompt.ask("Pick table")
            try:    tname = tbls[int(c)-1]
            except (ValueError, IndexError): tname = c
            try:
                new_df = db_load(sess.db_path, tname)
                new_df.columns = [col.strip() for col in new_df.columns]
                alias = Prompt.ask("Name in session", default=tname)
                sess.add(alias, new_df)
                console.print(f"[green]✔ '{alias}' loaded ({len(new_df):,} rows × {len(new_df.columns)} cols).[/green]")
                show_columns(new_df, compact=True)
            except Exception as e:
                console.print(f"[red]❌ Load error: {e}[/red]")

        elif action == "2":
            clone_name = Prompt.ask("Clone name", default=f"{sess.active}_clone")
            sess.add(clone_name, sess.df.copy())
            console.print(f"[green]✔ Cloned as '{clone_name}'.[/green]")

        elif action == "3":
            # Pick source table
            all_t = sess.list_tables()
            for i, n in enumerate(all_t): console.print(f"  [yellow]{i+1}[/yellow]  {n}")
            c = Prompt.ask("Source table #", default=str(all_t.index(sess.active)+1))
            try:    src_name = all_t[int(c)-1]
            except: src_name = sess.active
            src_df = sess.tables[src_name]

            console.print(f"\n[bold]{src_name}[/bold] has {len(src_df.columns)} columns:")
            show_columns(src_df)
            cols = ask_cols("Pick columns to keep (comma-sep letters or names)", src_df)
            if not cols:
                console.print("[red]No valid columns.[/red]"); continue
            slim_name = Prompt.ask("New table name", default=f"{src_name}_slim")
            sess.add(slim_name, src_df[cols].copy())
            console.print(f"[green]✔ '{slim_name}' created with {len(cols)} columns from {src_name}.[/green]")
            show_preview(sess.tables[slim_name], n=4, title=slim_name)

        elif action == "4":
            all_t = sess.list_tables()
            for i, n in enumerate(all_t): console.print(f"  [yellow]{i+1}[/yellow]  {n}  ({len(sess.tables[n]):,}r)")
            a = Prompt.ask("First table #"); b = Prompt.ask("Second table #")
            try:
                t1 = sess.tables[all_t[int(a)-1]]
                t2 = sess.tables[all_t[int(b)-1]]
                # Warn if column mismatch
                missing_in_t2 = set(t1.columns) - set(t2.columns)
                missing_in_t1 = set(t2.columns) - set(t1.columns)
                if missing_in_t2:
                    console.print(f"[yellow]⚠ Cols in table1 not in table2 → will be null: {missing_in_t2}[/yellow]")
                if missing_in_t1:
                    console.print(f"[yellow]⚠ Cols in table2 not in table1 → will be null: {missing_in_t1}[/yellow]")
                stacked = pd.concat([t1, t2], ignore_index=True)
                sname = Prompt.ask("Name for stacked table", default="stacked")
                sess.add(sname, stacked)
                console.print(f"[green]✔ {len(t1):,} + {len(t2):,} = {len(stacked):,} rows → '{sname}'.[/green]")
            except (ValueError, IndexError):
                console.print("[red]Invalid selection.[/red]")
            except Exception as e:
                console.print(f"[red]❌ Stack error: {e}[/red]")

        elif action == "5":
            all_t = [n for n in sess.list_tables() if n != sess.active]
            if not all_t:
                console.print("[yellow]Can't delete the active table. Switch first.[/yellow]"); continue
            for i, n in enumerate(all_t): console.print(f"  [yellow]{i+1}[/yellow]  {n}")
            c = Prompt.ask("Delete table #")
            try:
                to_del = all_t[int(c)-1]
                if Confirm.ask(f"Delete '{to_del}' from session?", default=False):
                    sess.tables.pop(to_del, None)
                    sess.history.pop(to_del, None)
                    console.print(f"[green]✔ '{to_del}' removed from session. (DB not affected)[/green]")
            except (ValueError, IndexError):
                console.print("[red]Invalid.[/red]")

        elif action == "6":
            all_t = sess.list_tables()
            for i, n in enumerate(all_t): console.print(f"  [yellow]{i+1}[/yellow]  {n}")
            c = Prompt.ask("Preview table #")
            try:
                tname = all_t[int(c)-1]
                show_columns(sess.tables[tname], compact=True)
                show_preview(sess.tables[tname], n=8, title=tname)
            except (ValueError, IndexError):
                console.print("[red]Invalid.[/red]")

        elif action == "7":
            # ── Create a brand new empty table and save to DB ──────────────
            console.print(Rule("[bold]Create New Table in DB[/bold]"))
            console.print(
                "[dim]Define column names and types. This creates an empty table "
                "in your DB that you can populate later.[/dim]\n"
            )
            new_tname = Prompt.ask("New table name (as it will appear in DB)")

            console.print("\n[dim]Types: [bold]str  int  float  bool  date[/bold][/dim]")
            columns_spec: list = []
            console.print("[dim]Add columns one by one. Blank name to finish.[/dim]")
            while True:
                cname = Prompt.ask("  Column name (blank to finish)").strip()
                if not cname: break
                ctype = Prompt.ask(f"  '{cname}' type", default="str").strip().lower()
                columns_spec.append((cname, ctype))

            if not columns_spec:
                console.print("[red]No columns defined — cancelled.[/red]"); continue

            # Show summary
            t = Table(title=f"New table: {new_tname}", box=box.ROUNDED)
            t.add_column("Column", style="white")
            t.add_column("Type",   style="cyan")
            for cname, ctype in columns_spec:
                t.add_row(cname, ctype)
            console.print(t)

            if not Confirm.ask("Create this table?", default=True): continue

            # Build empty DataFrame with correct dtypes
            dtype_map = {
                "str": "object", "int": "Int64", "float": "float64",
                "bool": "boolean", "date": "datetime64[ns]",
            }
            empty_df = pd.DataFrame({
                cname: pd.Series(dtype=dtype_map.get(ctype, "object"))
                for cname, ctype in columns_spec
            })

            # Optionally pre-fill with some rows
            n_rows = Prompt.ask(
                "Pre-fill with N empty rows? (0 = empty table)", default="0"
            ).strip()
            try:
                n_rows = int(n_rows)
                if n_rows > 0:
                    empty_df = pd.DataFrame({
                        cname: pd.Series([pd.NA] * n_rows, dtype=dtype_map.get(ctype, "object"))
                        for cname, ctype in columns_spec
                    })
            except ValueError:
                pass

            # Save to DB
            out_db = Prompt.ask("Save to DB path", default=sess.db_path)
            try:
                db_save(empty_df, out_db, new_tname, if_exists="fail" if
                    Confirm.ask("Fail if table already exists?", default=True) else "replace")
                # Also add to session
                sess.add(new_tname, empty_df)
                console.print(
                    f"[green]✔ Table '[bold]{new_tname}[/bold]' created in DB and added to session.[/green]"
                )
            except Exception as e:
                console.print(f"[red]❌ Error: {e}[/red]")

        elif action == "8":
            # ── Save any session table → DB (explicit create/overwrite) ──
            console.print(Rule("[bold]Save Session Table → DB[/bold]"))
            all_t = sess.list_tables()
            for i, n in enumerate(all_t):
                console.print(f"  [yellow]{i+1}[/yellow]  {n}  ({len(sess.tables[n]):,}r × {len(sess.tables[n].columns)}c)")
            c = Prompt.ask("Which table to save?", default=str(all_t.index(sess.active)+1))
            try:    save_tname = all_t[int(c)-1]
            except: save_tname = sess.active

            out_db    = Prompt.ask("Save to DB path", default=sess.db_path)
            out_tname = Prompt.ask("Table name in DB", default=save_tname)

            # Check if table already exists in DB
            existing = db_tables(out_db) if os.path.exists(out_db) else []
            if out_tname in existing:
                console.print(f"[yellow]⚠ Table '{out_tname}' already exists in {out_db}.[/yellow]")
                action2 = Prompt.ask(
                    "  [R]eplace  [A]ppend  [C]ancel",
                    choices=["R","A","C","r","a","c"], default="R"
                ).upper()
                if action2 == "C": continue
                if_exists = "replace" if action2 == "R" else "append"
            else:
                if_exists = "replace"

            try:
                db_save(sess.tables[save_tname], out_db, out_tname, if_exists=if_exists)
                console.print(f"[dim]Source DB '{sess.db_path}' was NOT modified.[/dim]")
            except Exception as e:
                console.print(f"[red]❌ Save error: {e}[/red]")

        elif action == "9":
            # ── List all tables in DB ──────────────────────────────────────
            console.print(Rule("[bold]Tables in DB[/bold]"))
            tbls = db_tables(sess.db_path)
            t = Table(title=f"DB: {sess.db_path}", box=box.ROUNDED)
            t.add_column("#",     style="yellow", width=5, justify="center")
            t.add_column("Table", style="white",  min_width=25)
            t.add_column("In Session?", style="cyan", justify="center")
            for i, n in enumerate(tbls):
                in_sess = "[green]✔[/green]" if n in sess.tables else "[dim]–[/dim]"
                t.add_row(str(i+1), n, in_sess)
            console.print(t)

        elif action == "X":
            console.print(Rule("[bold]Import Excel[/bold]"))
            path = Prompt.ask("Path to .xlsx file").strip().strip("\"")
            if os.path.exists(path):
                try:
                    with pd.ExcelFile(path) as xl:
                        sheets = xl.sheet_names
                        if len(sheets) == 1:
                            sheet = sheets[0]
                        else:
                            console.print(f"Sheets: [cyan]{', '.join(sheets)}[/cyan]")
                            sheet = Prompt.ask("Sheet to load", default=sheets[0])
                        df_xl = xl.parse(sheet)
                    df_xl.columns = [str(c).strip() for c in df_xl.columns]
                    
                    tname = Prompt.ask("Table name in session", default=sheet)
                    sess.add(tname, df_xl)
                    console.print(f"[green]✔ Imported '{sheet}' ({len(df_xl):,} rows).[/green]")
                    
                    if Confirm.ask(f"Save '{tname}' to DB ({os.path.basename(sess.db_path)})?", default=False):
                        db_save(df_xl, sess.db_path, tname)
                except Exception as e:
                    console.print(f"[red]❌ Import error: {e}[/red]")
            else:
                console.print(f"[red]File not found: {path}[/red]")


def op_switch_table(sess: Session):
    """Switch active table."""
    tables = sess.list_tables()
    if len(tables) == 1:
        console.print("[dim]Only one table in session.[/dim]"); return
    for i, n in enumerate(tables):
        mark = " [bold cyan]← active[/bold cyan]" if n==sess.active else ""
        console.print(f"  [yellow]{i+1}[/yellow]  {n}{mark}")
    c = Prompt.ask("Switch to table #")
    try:
        sess.active = tables[int(c)-1]
        console.print(f"[green]✔ Active: '[bold]{sess.active}[/bold]' "
                      f"— {len(sess.df):,}r × {len(sess.df.columns)}c[/green]")
        show_columns(sess.df, compact=True)
    except (ValueError, IndexError):
        console.print("[red]Invalid.[/red]")
