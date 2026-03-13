"""
Analysis module — advanced data analysis operations.
"""

import numpy as np
import pandas as pd

from rich import box
from rich.console import Console
from rich.prompt import Confirm, Prompt
from rich.rule import Rule
from rich.table import Table

from ..display import show_columns, show_preview
from ..helpers import resolve
from ..session import Session

console = Console()


def detect_column_type(df: pd.DataFrame, col: str) -> str:
    """Detect if a column looks like ID, date, flag, etc."""
    # Check for flag (0/1, True/False, Yes/No)
    if df[col].dtype == 'bool':
        return "Flag (Boolean)"
    if df[col].dtype in ('int64', 'Int64', 'float64'):
        unique = df[col].dropna().unique()
        if set(unique).issubset({0, 1}):
            return "Flag (0/1)"
    if df[col].dtype == 'object':
        unique = df[col].dropna().str.lower().unique()
        if set(unique).issubset({'yes', 'no', 'y', 'n', 'true', 'false'}):
            return "Flag (Yes/No)"
    
    # Check for ID (all unique)
    if df[col].nunique(dropna=False) == len(df):
        return "ID (Unique)"
    
    # Check for date
    if df[col].dtype == 'datetime64':
        return "Date"
    if df[col].dtype == 'object':
        sample = df[col].dropna().head(10)
        try:
            parsed = pd.to_datetime(sample, errors='coerce')
            if len(sample) > 0 and parsed.notna().sum() / len(sample) > 0.7:
                return "Date-like string"
        except Exception:
            pass
    
    # Check for email pattern
    if df[col].dtype == 'object':
        if df[col].astype(str).str.contains(r'^[\w.-]+@[\w.-]+\.\w+', regex=True, na=False).any():
            return "Email"
    
    unique_count = df[col].nunique()
    if unique_count <= 10:
        return f"Category ({unique_count} values)"
    
    # Check if numeric stored as string
    if df[col].dtype == 'object':
        try:
            numeric_series = pd.to_numeric(df[col], errors='coerce')
            denom = df[col].notna().sum()
            if denom > 0 and numeric_series.notna().sum() / denom > 0.8:
                return "Numeric (stored as text)"
        except Exception:
            pass
    
    return "Free Text"


def op_profile(sess: Session):
    """One-command full data quality profile for entire table."""
    df = sess.df
    console.print(Rule("[bold]📊 Quick Profile Report[/bold]"))
    
    # Performance Warning
    if len(df) > 1_000_000:
        console.print("[bold red]⚠ Large Dataset Detected (>1M rows)[/bold red]")
        console.print("[dim]Consider using SQL operations or aggregations for better performance.[/dim]")

    console.print(f"\n[bold cyan]Table:[/bold cyan] {sess.active}")
    console.print(f"[bold cyan]Rows:[/bold cyan] {len(df):,}")
    console.print(f"[bold cyan]Columns:[/bold cyan] {len(df.columns)}")
    
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    date_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
    obj_cols = df.select_dtypes(include=['object']).columns.tolist()
    
    console.print(f"\n[dim]Numeric: {len(numeric_cols)} | Date: {len(date_cols)} | Text: {len(obj_cols)}[/dim]")
    
    # For each column, generate profile
    for col in df.columns:
        console.print(f"\n[bold yellow]─── {col} ───[/bold yellow]")
        
        dtype = str(df[col].dtype)
        null_count = df[col].isna().sum()
        null_pct = null_count / len(df) * 100 if len(df) > 0 else 0
        unique_count = df[col].nunique(dropna=False)
        
        console.print(f"  Type: {dtype} | Null: {null_count:,} ({null_pct:.1f}%) | Unique: {unique_count:,}")
        
        # Detect column type
        col_type = detect_column_type(df, col)
        console.print(f"  [bold]Detected:[/bold] {col_type}")
        
        # Show top 5 values
        vc = df[col].dropna().value_counts().head(5)
        if len(vc) > 0:
            top_str = " | ".join([f"{v}: {c}" for v, c in vc.items()])
            console.print(f"  Top values: [dim]{top_str[:80]}...[/dim]" if len(top_str) > 80 else f"  Top values: [dim]{top_str}[/dim]")
        
        # Numeric-specific stats
        if col in numeric_cols:
            try:
                col_data = pd.to_numeric(df[col], errors='coerce')
                mean_val = col_data.mean()
                median_val = col_data.median()
                std_val = col_data.std()
                min_val = col_data.min()
                max_val = col_data.max()
                
                console.print(f"  Stats: mean={mean_val:.2f} | median={median_val:.2f} | std={std_val:.2f}")
                console.print(f"  Range: [{min_val:.2f}, {max_val:.2f}]")
                
                # Outlier detection (3 std method)
                outliers = col_data[abs(col_data - mean_val) > 3 * std_val]
                if len(outliers) > 0:
                    console.print(f"  [yellow]⚠ Outliers (3σ): {len(outliers)}[/yellow]")
            except:
                pass
        
        # Date-specific
        if col in date_cols or col_type == "Date-like string":
            try:
                dt_col = pd.to_datetime(df[col], errors='coerce')
                min_dt = dt_col.min()
                max_dt = dt_col.max()
                console.print(f"  Date range: {min_dt} to {max_dt}")
            except:
                pass
    
    # Option to export
    if Confirm.ask("Export profile to Excel?", default=False):
        try:
            # Build profile dataframe
            profile_data = []
            for col in df.columns:
                denom = len(df)
            row = {
                    "Column": col,
                    "Type": str(df[col].dtype),
                    "Null Count": df[col].isna().sum(),
                    "Null %": (df[col].isna().sum() / denom * 100) if denom > 0 else 0,
                    "Unique Count": df[col].nunique(dropna=False),
                    "Detected Type": detect_column_type(df, col)
                }
                if col in numeric_cols:
                    try:
                        col_data = pd.to_numeric(df[col], errors='coerce')
                        row["Mean"] = col_data.mean()
                        row["Median"] = col_data.median()
                        row["Std Dev"] = col_data.std()
                        row["Min"] = col_data.min()
                        row["Max"] = col_data.max()
                    except:
                        pass
                profile_data.append(row)
            
            profile_df = pd.DataFrame(profile_data)
            path = Prompt.ask("File path", default=f"profile_{sess.active}.xlsx")
            profile_df.to_excel(path, index=False)
            console.print(f"[green]✔ Profile exported to {path}[/green]")
        except Exception as e:
            console.print(f"[red]Export error: {e}[/red]")


def op_outlier_detection(sess: Session) -> pd.DataFrame:
    """Detect statistical outliers in numeric columns."""
    df = sess.df
    console.print(Rule("[bold]Outlier Detection[/bold]"))
    
    # Get numeric columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if not numeric_cols:
        console.print("[red]No numeric columns found.[/red]")
        return df
    
    show_columns(df, compact=True)
    col = resolve(Prompt.ask("Column to check for outliers"), df)
    if not col or col not in numeric_cols:
        console.print("[red]Invalid or non-numeric column.[/red]"); return df
    
    console.print("\n[bold]Detection Methods:[/bold]")
    console.print("  [yellow]1[/yellow]  IQR Method (beyond 1.5× IQR)")
    console.print("  [yellow]2[/yellow]  Z-Score Method (beyond 2-3 std)")
    console.print("  [yellow]3[/yellow]  Manual Threshold")
    method = Prompt.ask("Method", choices=["1", "2", "3"])
    
    col_data = pd.to_numeric(df[col], errors='coerce')
    outlier_mask = pd.Series(False, index=df.index)
    
    if method == "1":
        # IQR Method
        Q1 = col_data.quantile(0.25)
        Q3 = col_data.quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        outlier_mask = (col_data < lower) | (col_data > upper)
        console.print(f"[dim]IQR bounds: [{lower:.2f}, {upper:.2f}][/dim]")
    
    elif method == "2":
        # Z-Score Method
        while True:
            raw_th = Prompt.ask("Z-score threshold", default="3").strip()
            try:
                threshold = float(raw_th)
                break
            except ValueError:
                console.print("[yellow]Invalid threshold. Please enter a numeric value.[/yellow]")
        mean = col_data.mean()
        std = col_data.std()
        if std == 0:
            console.print("[red]Standard deviation is 0, cannot compute z-scores.[/red]"); return df
        z_scores = abs((col_data - mean) / std)
        outlier_mask = z_scores > threshold
        console.print(f"[dim]Mean: {mean:.2f}, Std: {std:.2f}, Threshold: {threshold}σ[/dim]")
    
    else:
        # Manual threshold
        lower = Prompt.ask("Lower bound (blank = none)").strip()
        upper = Prompt.ask("Upper bound (blank = none)").strip()
        if lower:
            try:
                lower_val = float(lower)
                outlier_mask = outlier_mask | (col_data < lower_val)
            except ValueError:
                console.print("[yellow]Invalid lower bound; ignoring this bound.[/yellow]")
        if upper:
            try:
                upper_val = float(upper)
                outlier_mask = outlier_mask | (col_data > upper_val)
            except ValueError:
                console.print("[yellow]Invalid upper bound; ignoring this bound.[/yellow]")
    
    outlier_count = outlier_mask.sum()
    if len(df) > 0:
        console.print(f"\n[yellow]Found {outlier_count:,} outliers ({outlier_count/len(df)*100:.1f}% of data).[/yellow]")
    else:
        console.print("[yellow]No data to analyze.[/yellow]")
    
    if outlier_count == 0:
        return df
    
    # Show flagged rows
    outlier_rows = df[outlier_mask]
    show_preview(outlier_rows, n=10, title="Outlier Rows")
    
    # Option to filter
    if Confirm.ask("Keep only outliers?", default=False):
        return outlier_rows
    elif Confirm.ask("Remove outliers?", default=False):
        return df[~outlier_mask]
    
    return df


def op_correlation_matrix(sess: Session):
    """Show correlation matrix for numeric columns."""
    df = sess.df
    console.print(Rule("[bold]Correlation Matrix[/bold]"))
    
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if len(numeric_cols) < 2:
        console.print("[red]Need at least 2 numeric columns.[/red]"); return
    
    console.print(f"[dim]Analyzing {len(numeric_cols)} numeric columns...[/dim]")
    
    corr = df[numeric_cols].corr()
    
    # Format as table
    t = Table(title="Correlation Matrix", box=box.ROUNDED, show_lines=True)
    t.add_column("", style="dim", width=15)
    for c in corr.columns:
        t.add_column(c[:12], justify="right")
    
    for row in corr.index:
        row_data = [row[:12]]
        for v in corr.loc[row]:
            if pd.isna(v):
                row_data.append("-")
            else:
                # Color code
                if v > 0.7:
                    row_data.append(f"[green]{v:.2f}[/green]")
                elif v < -0.7:
                    row_data.append(f"[red]{v:.2f}[/red]")
                else:
                    row_data.append(f"{v:.2f}")
        t.add_row(*row_data)
    
    console.print(t)


def op_crosstab(sess: Session):
    """Cross-tabulation like Excel pivot counting."""
    df = sess.df
    console.print(Rule("[bold]Cross-Tabulation[/bold]"))
    show_columns(df, compact=True)
    
    row_col_in = Prompt.ask("Row column").strip()
    row_col = resolve(row_col_in, df)
    if not row_col:
        console.print("[red]Column not found.[/red]"); return
    
    col_col_in = Prompt.ask("Column column").strip()
    col_col = resolve(col_col_in, df)
    if not col_col:
        console.print("[red]Column not found.[/red]"); return
    
    try:
        ct = pd.crosstab(df[row_col], df[col_col])
        
        t = Table(title=f"{row_col} × {col_col}", box=box.ROUNDED, show_lines=True)
        t.add_column(row_col, style="bold")
        for c in ct.columns:
            t.add_column(str(c)[:20], justify="right")
        t.add_column("Total", justify="right", style="bold cyan")
        
        for idx in ct.index:
            row_vals = [str(idx)[:20]]
            total = 0
            for v in ct.loc[idx]:
                row_vals.append(str(v))
                total += v
            row_vals.append(str(total))
            t.add_row(*row_vals)
        
        # Add totals row
        totals = ["[bold]Total[/bold]"]
        col_totals = ct.sum()
        for v in col_totals:
            totals.append(str(v))
        totals.append(str(col_totals.sum()))
        t.add_row(*totals)
        
        console.print(t)
        
        if Confirm.ask("Save as new table?", default=False):
            tname = Prompt.ask("Table name", default="crosstab")
            sess.add(tname, ct.reset_index())
            console.print(f"[green]✔ '{tname}' added to session.[/green]")
    
    except Exception as e:
        console.print(f"[red]❌ Error: {e}[/red]")


def op_segment_column(sess: Session) -> pd.DataFrame:
    """Convert continuous numeric to categorical bins."""
    df = sess.df
    console.print(Rule("[bold]Segment / Bin Numeric Column[/bold]"))
    
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if not numeric_cols:
        console.print("[red]No numeric columns found.[/red]"); return df
    
    show_columns(df, compact=True)
    col = resolve(Prompt.ask("Column to segment"), df)
    if not col:
        console.print("[red]Column not found.[/red]"); return df
    
    console.print("\n[bold]Segmentation Methods:[/bold]")
    console.print("  [yellow]1[/yellow]  Equal-width bins (e.g., 0-10, 10-20)")
    console.print("  [yellow]2[/yellow]  Equal-frequency bins (quantiles)")
    console.print("  [yellow]3[/yellow]  Custom bin edges")
    method = Prompt.ask("Method", choices=["1", "2", "3"])
    
    new_col = Prompt.ask("Name for new segment column")
    if not new_col:
        console.print("[red]Column name required.[/red]"); return df
    
    try:
        col_data = pd.to_numeric(df[col], errors='coerce')
        
        if method == "1":
            bins = int(Prompt.ask("Number of bins", default="5"))
            df[new_col] = pd.cut(col_data, bins=bins, include_lowest=True)
        
        elif method == "2":
            q = int(Prompt.ask("Number of quantiles", default="4"))
            df[new_col] = pd.qcut(col_data, q=q, duplicates='drop')
        
        else:
            edges_str = Prompt.ask("Bin edges (comma-sep, e.g., 0,10,50,100)").strip()
            edges = [float(e.strip()) for e in edges_str.split(",")]
            df[new_col] = pd.cut(col_data, bins=edges, include_lowest=True)
        
        console.print(f"[green]✔ Created '{new_col}' with {df[new_col].nunique()} segments.[/green]")
        show_preview(df, n=5)
    
    except Exception as e:
        console.print(f"[red]❌ Error: {e}[/red]")
    
    return df


def op_time_series(sess: Session) -> pd.DataFrame:
    """Unlock time-based analysis for date columns."""
    df = sess.df
    console.print(Rule("[bold]Time Series Analysis[/bold]"))
    
    # Find date columns
    date_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
    
    # Also check for date-like strings
    for col in df.select_dtypes(include=['object']).columns:
        try:
            sample = df[col].dropna().head(20)
            parsed = pd.to_datetime(sample, errors='coerce')
            if parsed.notna().sum() / len(sample) > 0.7:
                date_cols.append(col)
        except:
            pass
    
    if not date_cols:
        console.print("[red]No date columns found.[/red]"); return df
    
    console.print("[bold]Date columns found:[/bold]")
    for i, c in enumerate(date_cols):
        console.print(f"  [yellow]{i+1}[/yellow]  {c}")
    
    choice = Prompt.ask("Select date column", choices=[str(i+1) for i in range(len(date_cols))])
    date_col = date_cols[int(choice)-1]
    
    # Convert if string
    if df[date_col].dtype != 'datetime64':
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    
    console.print("\n[bold]Time Operations:[/bold]")
    console.print("  [yellow]1[/yellow]  Extract year/month/day/weekday")
    console.print("  [yellow]2[/yellow]  Group by time period (daily, monthly, etc.)")
    console.print("  [yellow]3[/yellow]  Calculate time differences")
    
    op = Prompt.ask("Operation", choices=["1", "2", "3"])
    
    if op == "1":
        # Extract components
        new_col = Prompt.ask("Name for new column")
        extract = Prompt.ask("Extract: [Y]ear / [M]onth / [D]ay / [W]eekday / [Q]uarter", 
                            choices=["Y", "M", "D", "W", "Q"]).upper()
        
        try:
            dt = df[date_col]
            if extract == "Y":
                df[new_col] = dt.dt.year
            elif extract == "M":
                df[new_col] = dt.dt.month
            elif extract == "D":
                df[new_col] = dt.dt.day
            elif extract == "W":
                df[new_col] = dt.dt.dayofweek
            else:
                df[new_col] = dt.dt.quarter
            console.print(f"[green]✔ Extracted {extract}.[/green]")
        except Exception as e:
            console.print(f"[red]❌ Error: {e}[/red]")
    
    elif op == "2":
        # Group by period
        period = Prompt.ask("Group by: [D]ay / [W]eek / [M]onth / [Q]uarter / [Y]ear",
                           choices=["D", "W", "M", "Q", "Y"]).upper()
        
        show_columns(df, compact=True)
        val_col = resolve(Prompt.ask("Value column to aggregate"), df)
        if not val_col:
            console.print("[red]Column not found.[/red]"); return df
        
        agg = Prompt.ask("Aggregation: sum / mean / count", default="sum")
        
        try:
            dt = df[date_col]
            if period == "D":
                grouper = dt.dt.date
            elif period == "W":
                grouper = dt.dt.to_period("W")
            elif period == "M":
                grouper = dt.dt.to_period("M")
            elif period == "Q":
                grouper = dt.dt.to_period("Q")
            else:
                grouper = dt.dt.year
            
            if agg == "sum":
                result = df.groupby(grouper)[val_col].sum()
            elif agg == "mean":
                result = df.groupby(grouper)[val_col].mean()
            else:
                result = df.groupby(grouper)[val_col].count()
            
            t = Table(title=f"Time Series: {val_col} by {period}", box=box.ROUNDED)
            t.add_column("Period", style="bold")
            t.add_column("Value", justify="right", style="cyan")
            for idx, val in result.items():
                t.add_row(str(idx), f"{val:,.2f}" if isinstance(val, float) else str(val))
            console.print(t)
            
            if Confirm.ask("Save as table?", default=False):
                tname = Prompt.ask("Table name", default="timeseries")
                sess.add(tname, result.reset_index(name=val_col))
                console.print(f"[green]✔ '{tname}' added.[/green]")
        
        except Exception as e:
            console.print(f"[red]❌ Error: {e}[/red]")
    
    elif op == "3":
        # Time differences
        console.print("[dim]Calculate days between two date columns.[/dim]")
        
        show_columns(df, compact=True)
        date2_in = Prompt.ask("Second date column (or TODAY)").strip()
        
        if date2_in.upper() == "TODAY":
            df['_ref_date'] = pd.Timestamp.now()
            date2_col = '_ref_date'
        else:
            date2_col = resolve(date2_in, df)
            if not date2_col:
                console.print("[red]Column not found.[/red]"); return df
            if df[date2_col].dtype != 'datetime64':
                df[date2_col] = pd.to_datetime(df[date2_col], errors='coerce')
        
        new_col = Prompt.ask("Name for days-difference column")
        
        try:
            df[new_col] = (df[date_col] - df[date2_col]).dt.days
            console.print(f"[green]✔ Created '{new_col}' (days difference).[/green]")
            show_preview(df, n=5)
        except Exception as e:
            console.print(f"[red]❌ Error: {e}[/red]")
        
        if '_ref_date' in df.columns:
            df = df.drop(columns=['_ref_date'])
    
    return df


def op_string_analysis(sess: Session):
    """Analyze text columns for common issues."""
    df = sess.df
    console.print(Rule("[bold]String Analysis[/bold]"))
    
    text_cols = df.select_dtypes(include=['object']).columns.tolist()
    if not text_cols:
        console.print("[red]No text columns found.[/red]"); return
    
    show_columns(df, compact=True)
    col = resolve(Prompt.ask("Column to analyze"), df)
    if not col:
        console.print("[red]Column not found.[/red]"); return
    
    console.print(f"\n[bold]Analysis for: {col}[/bold]")
    
    # String length stats
    lengths = df[col].astype(str).str.len()
    console.print(f"\n[dim]Length stats:[/dim]")
    console.print(f"  Min: {lengths.min()} | Max: {lengths.max()} | Mean: {lengths.mean():.1f}")
    
    # Check for common issues
    issues = []
    
    # Leading/trailing spaces
    leading = df[col].astype(str).str.startswith(' ').sum()
    trailing = df[col].astype(str).str.endswith(' ').sum()
    if leading > 0:
        issues.append(f"Leading spaces: {leading}")
    if trailing > 0:
        issues.append(f"Trailing spaces: {trailing}")
    
    # Empty strings
    empty = (df[col].astype(str).str.strip() == '').sum()
    if empty > 0:
        issues.append(f"Empty strings: {empty}")
    
    # Mixed case variations
    sample = df[col].dropna().head(100).astype(str)
    lower_count = sample.str.lower().nunique()
    orig_count = sample.nunique()
    if lower_count < orig_count:
        issues.append(f"Case inconsistencies: {orig_count} unique vs {lower_count} lowercase")
    
    # Numeric-as-text
    numeric_as_text = sample.str.match(r'^-?\d+\.?\d*$').sum()
    if numeric_as_text > len(sample) * 0.5:
        issues.append(f"Likely numeric stored as text: {numeric_as_text}")
    
    if issues:
        console.print("\n[yellow]⚠ Issues found:[/yellow]")
        for issue in issues:
            console.print(f"  • {issue}")
    else:
        console.print("\n[green]✔ No obvious issues found.[/green]")
    
    # Character frequency
    console.print("\n[bold]Top characters used:[/bold]")
    all_chars = ''.join(sample.astype(str).tolist())
    from collections import Counter
    char_counts = Counter(all_chars)
    for char, count in char_counts.most_common(10):
        if char != ' ':
            console.print(f"  '{char}': {count}")

    console.print("\n[dim]String analysis complete.[/dim]")
