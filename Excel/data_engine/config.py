"""
Configuration constants, banners, and menu text for DataEngine Pro.
Improved Excel-like organization.
"""

# ╔══════════════════════════════════════════════════════════════════╗
# ║  BANNER                                                            ║
# ╚══════════════════════════════════════════════════════════════════╝

BANNER = """[bold cyan]
  ██████╗  █████╗ ████████╗ █████╗     ███████╗███╗   ██╗ ██████╗ ██╗██╗   ██╗███████╗
  ██╔══██╗██╔══██╗╚══██╔══╝██╔══██║    ██╔════╝████╗  ██║██╔════╝ ██║██║   ██║██╔════╝
  ██║  ██║███████║   ██║   ███████║    █████╗  ██╔██╗ ██║██║  ███╗██║██║   ██║█████╗  
  ██║  ██║██╔══██║   ██║   ██╔══██║    ██╔══╝  ██║╚██╗██║██║   ██║██║██║   ██║██╔══╝  
  ██████╔╝██║  ██║   ██║   ██║  ██║    ███████╗██║ ╚████║╚██████╔╝██║╚██████╔╝███████╗
  ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝    ╚══════╝╚═╝  ╚═══╝ ╚═════╝ ╚═╝ ╚═════╝ ╚══════╝
                               [yellow]P R O  v 2 . 0[/yellow]
[/bold cyan]"""


HELP_MENU = """[bold cyan]📊 DATA VIEW[/bold cyan]
   [bold]C[/bold]  Columns      — View all columns with types & samples
   [bold]P[/bold]  Preview      — Show rows with column picker
   [bold]U[/bold]  Unique       — See all unique values in a column
   [bold]N[/bold]  Null Report  — Find missing/empty values
   [bold]S[/bold]  Search       — Find text anywhere in data
   [bold]I[/bold]  Statistics   — Numeric summaries (sum, avg, min, max)

[bold cyan]✏️ EDIT & TRANSFORM[/bold cyan]
   [bold]1[/bold]  Filter       — Keep rows matching criteria (Excel AutoFilter)
   [bold]2[/bold]  Add Column   — Create new column (formula, IF, map values)
   [bold]3[/bold]  Aggregate   — Count, Sum, Average by category
   [bold]4[/bold]  Sort        — Sort by one or more columns
   [bold]5[/bold]  Clean Nulls  — Fill, delete, or replace missing values
   [bold]6[/bold]  Rename/Drop  — Rename or delete columns
   [bold]7[/bold]  Remove Dupes — Delete duplicate rows
   [bold]9[/bold]  Change Type  — Convert text↔number↔date

[bold cyan]🔍 FIND & REPLACE[/bold cyan]
   [bold]G[/bold]  Find & Replace — Like Ctrl+H in Excel
   [bold]F[/bold]  Smart Fix    — Auto-detect and fix data issues
   [bold]M[/bold]  Multi-Filter — Multiple AND/OR conditions

[bold cyan]📈 ANALYSIS[/bold cyan]
   [bold]D[/bold]  Profile      — Full data quality report
   [bold]X[/bold]  Outliers     — Find unusual values
   [bold]V[/bold]  Correlation  — How columns relate
   [bold]Q[/bold]  Cross-Tab    — Frequency table
   [bold]][/bold]  Segment      — Group numbers into ranges
   [bold]=[/bold]  String Analysis — Text patterns

[bold cyan]📁 TABLE MANAGER[/bold cyan]
   [bold]T[/bold]  Manage       — Create, clone, stack, import, delete tables
   [bold]K[/bold]  Switch       — Change active table

[bold cyan]💾 SAVE & EXPORT[/bold cyan]
   [bold]W[/bold]  Save to DB   — Save changes to database
   [bold]E[/bold]  Export       — Export to CSV or Excel

[bold cyan]↩️ UNDO / RESET[/bold cyan]
   [bold]Z[/bold]  Undo         — Revert last change
   [bold]R[/bold]  Reset        — Reset to original data

[bold cyan]❓ HELP[/bold cyan]
   [bold]H[/bold]  Show Menu    — Display this help
   [bold]0[/bold]  Exit         — Quit program"""


# ╔══════════════════════════════════════════════════════════════════╗
# ║  DEPENDENCIES                                                      ║
# ╚══════════════════════════════════════════════════════════════════╝

REQUIRED_PACKAGES = [
    ("rich", "rich"),
    ("rapidfuzz", "rapidfuzz"),
    ("openpyxl", "openpyxl"),
]

# ╔══════════════════════════════════════════════════════════════════╗
# ║  APP INFO                                                          ║
# ╚══════════════════════════════════════════════════════════════════╝

APP_NAME = "DataEngine Pro"
APP_VERSION = "2.0"
APP_DESCRIPTION = "Excel-power. Python speed. DB-connected."

# ╔══════════════════════════════════════════════════════════════════╗
# ║  CONSTANTS                                                         ║
# ╚══════════════════════════════════════════════════════════════════╝

MAX_UNDO_HISTORY = 20
DEFAULT_PREVIEW_ROWS = 8

# Large data handling
LARGE_DATASET_THRESHOLD = 100000  # 100k rows
CHUNK_SIZE = 10000

# Display limits
MAX_UNIQUE_DISPLAY = 60
MAX_PREVIEW_COLUMNS = 20
