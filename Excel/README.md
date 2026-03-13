# excelpy

<p align="center">
  <strong>Interactive CLI and library for treating CSV/DB tables like Excel</strong>
</p>

<p align="center">
  <a href="https://pypi.org/project/excelpy/">
    <img src="https://img.shields.io/pypi/v/excelpy.svg" alt="PyPI Version">
  </a>
  <a href="https://pypi.org/project/excelpy/">
    <img src="https://img.shields.io/pypi/pyversions/excelpy.svg" alt="Python Versions">
  </a>
  <a href="https://github.com/excelpy/excelpy/actions">
    <img src="https://github.com/excelpy/excelpy/actions/workflows/test.yml/badge.svg" alt="Tests">
  </a>
  <a href="https://pypi.org/project/excelpy/">
    <img src="https://img.shields.io/pypi/l/excelpy.svg" alt="License">
  </a>
</p>

---

## Features

- **Load Data**: CSV files (auto-detect delimiter) or SQLite databases
- **Interactive Filtering**: 3-step wizard (Column → Operator → Value)
- **Fuzzy Matching**: Partial column names with rapidfuzz
- **Excel-style References**: Use A, B, C column letters
- **Ranking**: Rank by columns with multiple tie methods
- **Aggregation**: Sum, average, count, min, max by group
- **Export**: CSV, SQLite with append/replace/fail modes
- **Rich UI**: Beautiful tables and progress indicators

## Installation

```bash
# Core installation
pip install excelpy

# With all dependencies
pip install excelpy[all]

# With pandas support
pip install excelpy[pandas]

# With SQLAlchemy support
pip install excelpy[sqlalchemy]
```

## Quick Start

### CLI Usage

```bash
# Load a CSV file
excelpy load data.csv

# Load with specific engine
excelpy load data.csv --engine pandas

# Preview data
excelpy preview
excelpy preview --rows 20 --columns name,age

# Filter data (interactive)
excelpy filter

# Sort by column
excelpy sort --columns age --descending

# Rank by value
excelpy rank --columns salary --name rank --group department

# Aggregate by group
excelpy aggregate --group city --columns salary --function mean

# Save results
excelpy save output.csv

# Show data info
excelpy info
```

### Library Usage

```python
from excelpy import load_table, show_preview, ask_condition_and_filter
from excelpy import sort_table, rank_table, aggregate_table
from excelpy.engine import DataFrameWrapper

# Load data
df = load_table("data.csv")

# Show preview
show_preview(df, n=10)

# Filter (interactive - 3 step: Column → Operator → Value)
result = ask_condition_and_filter(df)

# Sort
sorted_df = sort_table(df, ["name"], ascending=True)

# Rank
ranked_df = rank_table(df, ["score"], rank_col_name="position", tie_method="dense")

# Aggregate
agg_df = aggregate_table(df, group_col="category", value_cols=["sales"], func="sum")

# Save
df.save("output.csv", mode="replace")
```

## Interactive Workflow

The CLI guides you through each operation:

```
$ excelpy load sales.csv
✓ Loaded sales.csv
  Rows: 10,000 | Columns: 15 | Engine: polars

$ excelpy filter

═══════════════════════════════════════ Filter Rows ═══════════════════════════════════════

[bold cyan]Step 1:[/bold cyan] Select column to filter on
Available Columns:
  #   Column    Excel
  1   date      A
  2   region    B
  3   product   C
  4   sales     D
  5   profit    E

Column [date]: region

Values in 'region':
  North
  South
  East
  West

[bold cyan]Step 2:[/bold cyan] Select operator
  1  EQUALS
  2  NOT EQUALS
  3  GREATER THAN
  ...

Operator [1]: 

[bold cyan]Step 3:[/bold cyan] Enter value to compare
Value [North]: North

✓ Filter applied!
  2,500 rows kept (7,500 removed)

Select columns to display (all):
```

## Fuzzy Matching

excelpy uses [rapidfuzz](https://github.com/maxbachmann/RapidFuzz) for intelligent matching:

- Partial column names resolve automatically
- If score ≥ 90, auto-accepts but still confirms
- Shows top 5 matches when ambiguous

```
Column [prod]: prd
❓ 'prd' not found. Best matches:
 #  Column      Match
 1  product     95%
 2  prod_id     85%
 3  prod_name   82%
```

## Excel-style References

Use letter references like Excel:

```
A     → First column
B     → Second column  
AA    → 27th column
1     → First column (1-based)
name  → Column name
```

## Requirements

- Python 3.9+
- typer (CLI framework)
- rich (terminal UI)
- rapidfuzz (fuzzy matching)
- polars (default engine, optional)
- pandas (fallback engine, optional)
- sqlalchemy (SQL support, optional)

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
