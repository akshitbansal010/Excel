"""
Configuration constants for DataEngine Pro Streamlit app.
"""

# Large dataset handling
LARGE_DATASET_THRESHOLD = 100000  # 100k rows - use pagination
DEFAULT_PAGE_SIZE = 100
MAX_PREVIEW_ROWS = 500
CHUNK_SIZE = 10000

# Cache configuration
CACHE_TTL = 300  # 5 minutes

# Display limits
MAX_UNIQUE_DISPLAY = 60
MAX_PREVIEW_COLUMNS = 20

# App info
APP_NAME = "DataEngine Pro"
APP_VERSION = "2.0"
APP_DESCRIPTION = "Excel Power, Python Speed"

# Filter operators
NUMERIC_OPERATORS = ["==", "!=", ">", "<", ">=", "<=", "is_null", "is_not_null", "in"]
TEXT_OPERATORS = ["==", "!=", "contains", "startswith", "endswith", "is_null", "is_not_null", "is_blank", "in"]

# Undo history limit
MAX_UNDO_HISTORY = 20
