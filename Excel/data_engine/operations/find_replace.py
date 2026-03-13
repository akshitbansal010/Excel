"""
Find & Replace module — classic Ctrl+H functionality.
Improved with Excel-like guided experience.
"""

import pandas as pd

from rich import box
from rich.console import Console
from rich.prompt import Confirm, Prompt
from rich.rule import Rule
from rich.table import Table
from rich.panel import Panel

from ..display import show_columns, show_preview
from ..helpers import resolve
from ..session import Session

console = Console()


def op_find_replace(sess: Session) -> pd.DataFrame:
    """
    Classic Ctrl+H Find & Replace functionality.
    Excel-like guided experience with preview before commit.
    """
    df = sess.df
    console.print(Rule("[bold]🔍 Find & Replace (Ctrl+H)[/bold]"))
    
    console.print(Panel(
        "Find text or values and replace with new ones.\n"
        "Like Excel's Find & Replace (Ctrl+H).",
        title="Find & Replace", border_style="cyan"
    ))
    
    # Step 1: Choose scope
    console.print("\n[bold]Step 1: Where to search?[/bold]")
    console.print("  [yellow]1[/yellow]  One column")
    console.print("  [yellow]2[/yellow]  All columns")
    scope = Prompt.ask("Scope", choices=["1", "2"], default="1")
    
    if scope == "1":
        show_columns(df, compact=True)
        console.print("\n[bold]Step 2: Choose column[/bold]")
        col = resolve(Prompt.ask("Column to search in"), df)
        if not col:
            console.print("[red]Column not found.[/red]"); return df
    else:
        col = None
        console.print("Searching [bold]all columns[/bold]")
    
    # Step 3: What to find
    console.print("\n[bold]Step 3: What to find?[/bold]")
    find_str = Prompt.ask("Find what", default="").strip()
    if not find_str:
        console.print("[yellow]Nothing to find.[/yellow]"); return df
    
    # Step 4: Replace with what
    replace_str = Prompt.ask("Step 4: Replace with", default="").strip()
    
    # Step 5: Match mode
    console.print("\n[bold]Step 5: How to match?[/bold]")
    console.print("  [yellow]E[/yellow]  Exact match")
    console.print("  [yellow]C[/yellow]  Contains (text anywhere)")
    console.print("  [yellow]R[/yellow]  Regex (advanced pattern)")
    
    match_mode = Prompt.ask("Match mode", choices=["E", "C", "R"], default="E").upper()
    case_sensitive = Confirm.ask("Case sensitive? (A vs a)", default=False)
    
    # Build mask
    if col:
        target_df = df[[col]]
    else:
        target_df = df
    
    mask = pd.DataFrame(False, index=target_df.index, columns=target_df.columns)
    if match_mode == "E":
        for c in target_df.columns:
            if case_sensitive:
                mask[c] = target_df[c].astype(str) == find_str
            else:
                mask[c] = target_df[c].astype(str).str.lower() == find_str.lower()
    elif match_mode == "C":
        for c in target_df.columns:
            if case_sensitive:
                mask[c] = target_df[c].astype(str).str.contains(find_str, regex=False, na=False)
            else:
                mask[c] = target_df[c].astype(str).str.contains(find_str, case=False, regex=False, na=False)
    else:  # Regex
        try:
            for c in target_df.columns:
                mask[c] = target_df[c].astype(str).str.contains(find_str, case=case_sensitive, regex=True, na=False)
        except Exception as e:
            console.print(f"[red]Invalid regex: {e}[/red]"); return df
    
    # Count matches
    if col:
        match_count = mask[col].sum()
    else:
        match_count = int(mask.values.sum())
    
    console.print(f"\n[cyan]Found {match_count:,} cells containing '{find_str}'.[/cyan]")
    
    if match_count == 0:
        console.print("[yellow]No matches found.[/yellow]")
        return df
    
    # Step 6: Preview what will change
    console.print("\n[bold]Preview (first 5 changes):[/bold]")
    
    if col:
        preview_rows = df[mask[col]].head(5)
    else:
        preview_mask = mask.any(axis=1)
        preview_rows = df[preview_mask].head(5)
    
    t = Table(box=box.SIMPLE)
    if col:
        t.add_column("Row", style="dim", width=5)
        t.add_column("Current", style="red", min_width=20)
        t.add_column("Will become", style="green", min_width=20)
        for idx, row in preview_rows.iterrows():
            t.add_row(str(idx+1), str(row[col])[:30], replace_str)
    else:
        t.add_column("Row", style="dim", width=5)
        t.add_column("Changes", style="white", min_width=40)
        for idx in preview_rows.index:
            changed_cols = [c for c in df.columns if mask.loc[idx, c]]
            change_str = ", ".join([f"{c}: {str(df.loc[idx, c])[:15]}→{replace_str[:15]}" 
                                   for c in changed_cols[:3]])
            t.add_row(str(idx+1), change_str)
    console.print(t)
    
    # Step 7: Replace mode
    console.print("\n[bold]Step 5: Replace options[/bold]")
    console.print("  [yellow]P[/yellow]  Preview first (recommended)")
    console.print("  [yellow]A[/yellow]  Replace all")
    console.print("  [yellow]F[/yellow]  Replace first only")
    
    replace_mode = Prompt.ask("Replace", choices=["P", "A", "F"], default="P").upper()
    
    if replace_mode == "P":
        console.print(f"\n[yellow]⚠ {match_count:,} cells will be changed to '{replace_str}'.[/yellow]")
        if Confirm.ask("Proceed with replacement?", default=True):
            replace_mode = "A"
        else:
            console.print("[dim]Cancelled.[/dim]")
            return df
    
    # Apply replacement
    if replace_mode == "F":
        # Replace first only
        if col:
            first_match_idx = mask[mask[col]].index[0]
            df.at[first_match_idx, col] = replace_str
        else:
            first_match_idx = mask.any(axis=1).idxmax()
            for c in df.columns:
                if mask.loc[first_match_idx, c]:
                    df.at[first_match_idx, c] = replace_str
                    break
        console.print(f"[green]✔ Replaced 1 cell with '{replace_str}'.[/green]")
    else:
        # Replace all
        if col:
            df.loc[mask[col], col] = replace_str
        else:
            for c in df.columns:
                df.loc[mask[c], c] = replace_str
        console.print(f"[green]✔ Replaced {match_count:,} cells with '{replace_str}'.[/green]")
    
    show_preview(df, n=5, title="After Replace")
    return df
