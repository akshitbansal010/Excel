"""
Configuration constants, banners, and menu text for DataEngine Pro.
"""

# ╔══════════════════════════════════════════════════════════════════╗
# ║  BANNER                                                            ║
# ╚══════════════════════════════════════════════════════════════════╝

BANNER = """[bold cyan]
 ██████╗  █████╗ ████████╗ █████╗     ███████╗███╗   ██╗ ██████╗ ██╗██╗   ██╗███████╗
 ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗    ██╔════╝████╗  ██║██╔════╝ ██║██║   ██║██╔════╝
 ██║  ██║███████║   ██║   ███████║    █████╗  ██╔██╗ ██║██║  ███╗██║██║   ██║█████╗  
 ██║  ██║██╔══██║   ██║   ██╔══██║    ██╔══╝  ██║╚██╗██║██║   ██║██║██║   ██║██╔══╝  
 ██████╔╝██║  ██║   ██║   ██║  ██║    ███████╗██║ ╚████║╚██████╔╝██║╚██████╔╝███████╗
 ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝    ╚══════╝╚═╝  ╚═══╝ ╚═════╝ ╚═╝ ╚═════╝ ╚══════╝
                              [yellow]P R O  v 2 . 0[/yellow]
[/bold cyan]"""

HELP_MENU = """[bold yellow]DATA VIEW[/bold yellow]
  [bold]C[/bold]  Column inspector         [bold]P[/bold]  Preview (choose columns)
  [bold]U[/bold]  Unique values            [bold]N[/bold]  Null / missing report
  [bold]S[/bold]  Search                   [bold]I[/bold]  Statistics (describe)

[bold yellow]TRANSFORM[/bold yellow]
  [bold]1[/bold]  Filter rows              [bold]2[/bold]  Add flag / formula column
  [bold]3[/bold]  Count-If / Sum-If        [bold]4[/bold]  Sort
  [bold]5[/bold]  Clean / Handle nulls     [bold]6[/bold]  Rename / drop columns
  [bold]7[/bold]  Remove duplicates        [bold]8[/bold]  Pivot / Group summary
  [bold]9[/bold]  Change column type       [bold]J[/bold]  VLOOKUP / Join tables

[bold yellow]NEW: PHASE 1 - EXCEL PARITY[/bold yellow]
  [bold]F[/bold]  Smart Fix (1.1)          [bold]M[/bold]  Multi-filter (1.2)
  [bold]G[/bold]  Find & Replace (1.3)    [bold]O[/bold]  Focus/Pin View (1.4)
  [bold]L[/bold]  Row Edit (1.5)          [bold]B[/bold]  Calc Columns (1.6)
  [bold]Y[/bold]  Filter by Flag

[bold yellow]TABLE MANAGER[/bold yellow]
  [bold]T[/bold]  Table Manager  (create • clone • slim • stack • import • delete)
  [bold]K[/bold]  Switch active table

[bold yellow]SESSION[/bold yellow]
  [bold]Z[/bold]  Undo                     [bold]R[/bold]  Reset to original
  [bold]W[/bold]  Save to DB               [bold]E[/bold]  Export CSV / Excel
  [bold]H[/bold]  This menu                [bold]0[/bold]  Exit"""

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
APP_DESCRIPTION = "Excel-power. Python speed. DB-connected. Zero BS."

# ╔══════════════════════════════════════════════════════════════════╗
# ║  CONSTANTS                                                         ║
# ╚══════════════════════════════════════════════════════════════════╝

MAX_UNDO_HISTORY = 20
DEFAULT_PREVIEW_ROWS = 8
