#!/usr/bin/env python3
"""
Demo script for excelpy - automates a typical interactive session.

This script demonstrates:
1. Loading CSV data
2. Previewing data
3. Filtering
4. Sorting
5. Ranking
6. Aggregating
7. Saving results
"""

import os
import sys
import tempfile

# Add excelpy to path if running from source
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from excelpy import (
    load_table,
    show_preview,
    sort_table,
    rank_table,
    aggregate_table,
    save_table,
    __version__,
)
from excelpy.engine import is_polars_available


def create_demo_data() -> str:
    """Create demo CSV file with sample data."""
    content = """name,age,city,department,salary,score
Alice,30,NY,Sales,75000,85
Bob,25,LA,Engineering,65000,72
Charlie,35,NY,Marketing,90000,91
Diana,28,SF,Engineering,70000,78
Eve,32,NY,Sales,80000,88
Frank,29,LA,Marketing,72000,80
Grace,27,SF,Engineering,68000,75
Henry,33,NY,Sales,85000,89
Ivy,31,LA,Marketing,78000,82
Jack,26,SF,Engineering,62000,70
Kate,34,NY,Marketing,95000,94
Leo,29,LA,Sales,73000,81
Mike,30,SF,Engineering,71000,79
Nora,28,NY,Marketing,82000,86
Oscar,27,LA,Engineering,66000,74"""
    
    # Create temp file
    fd, path = tempfile.mkstemp(suffix='.csv', prefix='demo_')
    with os.fdopen(fd, 'w') as f:
        f.write(content)
    
    return path


def run_demo():
    """Run the demo session."""
    print("\n" + "="*60)
    print("  excelpy Demo - Automating a Typical Session")
    print("="*60)
    print(f"\nVersion: {__version__}")
    print(f"Polars available: {is_polars_available()}")
    
    # Create demo data
    print("\n[1] Creating demo data...")
    csv_path = create_demo_data()
    print(f"    Created: {csv_path}")
    
    try:
        # Load data
        print("\n[2] Loading CSV file...")
        df = load_table(csv_path)
        print(f"    Loaded: {len(df)} rows, {len(df.columns)} columns")
        print(f"    Engine: {df.engine}")
        
        # Preview
        print("\n[3] Previewing data...")
        show_preview(df, n=5, title="Initial Data")
        
        # Show columns
        print("\n[4] Available columns:")
        for i, col in enumerate(df.columns):
            from excelpy.helpers import col_letter
            print(f"    {col_letter(i)}: {col}")
        
        # Filter demo
        print("\n[5] Filtering: department == 'Sales'")
        from excelpy.core import _apply_filter
        filtered = _apply_filter(df, "department", "==", "Sales")
        print(f"    Filtered: {len(filtered)} rows (from {len(df)})")
        show_preview(filtered, n=5, title="Sales Department")
        
        # Sort demo
        print("\n[6] Sorting by salary (descending)")
        sorted_df = df.sort("salary", ascending=False)
        show_preview(sorted_df, n=5, title="Sorted by Salary")
        
        # Ranking demo
        print("\n[7] Ranking by score (min tie method)")
        from excelpy.core import _apply_rank
        ranked = _apply_rank(df, ["score"], "rank", "min", ascending=False)
        ranked_native = ranked.native
        print("\n    Top 5 by score:")
        if df.engine == "polars":
            for row in ranked_native.head(5).to_dicts():
                print(f"    Rank {row.get('rank')}: {row.get('name')} - Score: {row.get('score')}")
        else:
            print(ranked_native[["name", "score", "rank"]].head(5).to_string())
        
        # Aggregation demo
        print("\n[8] Aggregating: average salary by department")
        from excelpy.core import _apply_aggregate
        agg = _apply_aggregate(df, "department", ["salary"], "mean")
        show_preview(agg, n=10, title="Avg Salary by Department")
        
        # Save demo
        print("\n[9] Saving results...")
        from pathlib import Path
        p = Path(csv_path)
        output_path = str(p.with_name(p.stem + "_processed" + p.suffix))
        save_table(filtered, output_path, mode="replace")
        print(f"    Saved to: {output_path}")
        
        # Cleanup
        os.unlink(output_path)
        
        print("\n" + "="*60)
        print("  Demo Complete!")
        print("="*60)
        
    finally:
        # Cleanup
        os.unlink(csv_path)
        print(f"\nCleaned up demo file: {csv_path}")


def run_interactive_demo():
    """Show how to use interactive mode (non-executable demo)."""
    print("\n" + "="*60)
    print("  Interactive Mode Demo (Scripted)")
    print("="*60)
    
    # This is what an interactive session would look like
    demo_commands = """
# In interactive mode, you would run:

$ excelpy load data.csv
✓ Loaded data.csv
  Rows: 15 | Columns: 6 | Engine: polars

$ excelpy preview
# Shows first 10 rows

$ excelpy filter
# Launches 3-step filter wizard:
# Step 1: Select column (e.g., "department")
# Step 2: Select operator (e.g., "==")
# Step 3: Enter value (e.g., "Sales")

$ excelpy sort -c salary --descending
# Sorts by salary descending

$ excelpy rank -c score -n rank --group department --top 3
# Ranks by score within each department, keeps top 3

$ excelpy aggregate -g department -c salary -f mean
# Calculates mean salary per department

$ excelpy save results.csv
# Saves current data to CSV
"""
    print(demo_commands)


if __name__ == "__main__":
    # Run automated demo
    run_demo()
    
    # Show interactive instructions
    run_interactive_demo()
