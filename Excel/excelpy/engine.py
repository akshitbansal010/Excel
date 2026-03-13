"""
Engine abstraction layer for excelpy.
Supports polars (default) with pandas fallback.
"""

from typing import Optional, Any, Union
from dataclasses import dataclass
import warnings

# Try to import polars
POLARS_AVAILABLE = False
try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    pl = None

# Try to import pandas
PANDAS_AVAILABLE = False
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    pd = None

# Try to import sqlalchemy
SQLALCHEMY_AVAILABLE = False
try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    Engine = None
    create_engine = None
    text = None


@dataclass
class EngineInfo:
    """Information about the current engine."""
    name: str  # "polars" or "pandas"
    is_polars: bool
    is_pandas: bool


def is_polars_available() -> bool:
    """Check if polars is available."""
    return POLARS_AVAILABLE


def is_pandas_available() -> bool:
    """Check if pandas is available."""
    return PANDAS_AVAILABLE


def is_sqlalchemy_available() -> bool:
    """Check if SQLAlchemy is available."""
    return SQLALCHEMY_AVAILABLE


def get_engine_info(force_engine: Optional[str] = None) -> EngineInfo:
    """
    Get information about the current engine.
    
    Args:
        force_engine: Force a specific engine ("polars" or "pandas")
    
    Returns:
        EngineInfo with engine details
    """
    engine = determine_engine(force_engine)
    return EngineInfo(
        name=engine,
        is_polars=engine == "polars",
        is_pandas=engine == "pandas"
    )


def determine_engine(force_engine: Optional[str] = None) -> str:
    """
    Determine which engine to use.
    
    Args:
        force_engine: Force a specific engine ("polars" or "pandas")
    
    Returns:
        Engine name: "polars" or "pandas"
    
    Raises:
        ImportError: If neither polars nor pandas is available
    """
    if force_engine:
        if force_engine.lower() == "polars":
            if not POLARS_AVAILABLE:
                raise ImportError("Polars is not installed. Install with: pip install polars")
            return "polars"
        elif force_engine.lower() == "pandas":
            if not PANDAS_AVAILABLE:
                raise ImportError("Pandas is not installed. Install with: pip install pandas")
            return "pandas"
        else:
            raise ValueError(f"Unknown engine: {force_engine}. Use 'polars' or 'pandas'")
    
    # Default priority: polars > pandas
    if POLARS_AVAILABLE:
        return "polars"
    elif PANDAS_AVAILABLE:
        return "pandas"
    else:
        raise ImportError(
            "Neither polars nor pandas is installed. "
            "Install one with: pip install polars  # recommended"
        )


class DataFrameWrapper:
    """
    Wrapper class that provides a unified interface for both polars and pandas DataFrames.
    """
    
    def __init__(self, df: Any, engine: str):
        """
        Initialize wrapper with a DataFrame.
        
        Args:
            df: polars DataFrame or pandas DataFrame
            engine: Engine name ("polars" or "pandas")
        """
        self._df = df
        self._engine = engine
    
    @property
    def native(self) -> Any:
        """Get the underlying DataFrame."""
        return self._df
    
    @property
    def engine(self) -> str:
        """Get the engine name."""
        return self._engine
    
    @property
    def columns(self) -> list:
        """Get column names."""
        if self._engine == "polars":
            return self._df.columns
        else:
            return list(self._df.columns)
    
    @property
    def shape(self) -> tuple:
        """Get (rows, cols) shape."""
        if self._engine == "polars":
            return (self._df.height, len(self._df.columns))
        else:
            return self._df.shape
    
    @property
    def dtypes(self) -> dict:
        """Get column dtypes."""
        if self._engine == "polars":
            return {c: str(self._df[c].dtype) for c in self._df.columns}
        else:
            return {c: str(dtype) for c, dtype in self._df.dtypes.items()}
    
    def __len__(self) -> int:
        """Get number of rows."""
        if self._engine == "polars":
            return self._df.height
        return len(self._df)
    
    def __getitem__(self, key) -> Any:
        """Support bracket notation for column access."""
        if isinstance(key, str):
            return self._df[key] if self._engine == "polars" else self._df[key]
        if isinstance(key, list) or isinstance(key, slice):
            return DataFrameWrapper(self._df[key], self._engine)
        return self._df[key]
    
    def head(self, n: int = 5) -> Any:
        """Get first n rows."""
        if self._engine == "polars":
            return self._df.head(n)
        return self._df.head(n)
    
    def tail(self, n: int = 5) -> Any:
        """Get last n rows."""
        if self._engine == "polars":
            return self._df.tail(n)
        return self._df.tail(n)
    
    def filter(self, condition: Any) -> "DataFrameWrapper":
        """Filter rows."""
        if self._engine == "polars":
            return DataFrameWrapper(self._df.filter(condition), self._engine)
        return DataFrameWrapper(self._df[condition], self._engine)
    
    def select(self, columns: list) -> "DataFrameWrapper":
        """Select columns."""
        if self._engine == "polars":
            return DataFrameWrapper(self._df.select(columns), self._engine)
        return DataFrameWrapper(self._df[columns], self._engine)
    
    def sort(self, by: Union[str, list], ascending: bool = True) -> "DataFrameWrapper":
        """Sort by column(s)."""
        if self._engine == "polars":
            return DataFrameWrapper(self._df.sort(by, descending=not ascending), self._engine)
        return DataFrameWrapper(self._df.sort_values(by, ascending=ascending), self._engine)
    
    def group_by(self, by: Union[str, list]) -> Any:
        """Group by column(s)."""
        if self._engine == "polars":
            return self._df.group_by(by)
        return self._df.groupby(by)
    
    def is_null(self) -> Any:
        """Check for null values."""
        if self._engine == "polars":
            return self._df.is_null()
        return self._df.isna()
    
    def is_not_null(self) -> Any:
        """Check for non-null values."""
        if self._engine == "polars":
            return self._df.is_not_null()
        return self._df.notna()
    
    def fill_null(self, value: Any) -> "DataFrameWrapper":
        """Fill null values."""
        if self._engine == "polars":
            return DataFrameWrapper(self._df.fill_null(value), self._engine)
        return DataFrameWrapper(self._df.fillna(value), self._engine)
    
    def drop_nulls(self, subset: Optional[list] = None) -> "DataFrameWrapper":
        """Drop null values."""
        if self._engine == "polars":
            return DataFrameWrapper(self._df.drop_nulls(subset=subset), self._engine)
        return DataFrameWrapper(self._df.dropna(subset=subset), self._engine)
    
    def with_column(self, expr: Any, name: Optional[str] = None) -> "DataFrameWrapper":
        """Add a new column."""
        if self._engine == "polars":
            return DataFrameWrapper(self._df.with_columns([expr]), self._engine)
        # For pandas, need different handling
        if name:
            df_copy = self._df.copy()
            df_copy[name] = expr if not callable(expr) else expr(df_copy)
            return DataFrameWrapper(df_copy, self._engine)
        return self
    
    def to_pandas(self) -> "pd.DataFrame":
        """Convert to pandas DataFrame."""
        if self._engine == "polars":
            return self._df.to_pandas()
        return self._df.copy()
    
    def to_polars(self) -> "pl.DataFrame":
        """Convert to polars DataFrame."""
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is not installed. Install with 'pip install polars'.")
        if self._engine == "polars":
            return self._df
        return pl.DataFrame(self._df)
    
    def to_dict(self, orient: str = "records") -> list:
        """Convert to list of dicts."""
        if self._engine == "polars":
            return self._df.to_dicts()
        return self._df.to_dict(orient=orient)
    
    def to_csv(self, path: str, **kwargs) -> None:
        """Save to CSV."""
        if self._engine == "polars":
            self._df.write_csv(path, **kwargs)
        else:
            self._df.to_csv(path, index=False, **kwargs)
    
    def to_sql(self, table_name: str, con: Any, if_exists: str = "replace") -> None:
        """Save to SQL table."""
        if self._engine == "polars":
            df = self._df.to_pandas()
        else:
            df = self._df
        df.to_sql(table_name, con, if_exists=if_exists, index=False)
    
    def unique(self, subset: Optional[list] = None) -> "DataFrameWrapper":
        """Get unique rows."""
        if self._engine == "polars":
            return DataFrameWrapper(self._df.unique(subset=subset), self._engine)
        return DataFrameWrapper(self._df.drop_duplicates(subset=subset), self._engine)
    
    def n_unique(self, column: str) -> int:
        """Count unique values in a column."""
        if self._engine == "polars":
            return self._df[column].n_unique()
        return self._df[column].nunique()
    
    def value_counts(self, column: str) -> "DataFrameWrapper":
        """Get value counts."""
        if self._engine == "polars":
            return DataFrameWrapper(
                self._df[column].value_counts().sort("counts", descending=True),
                self._engine
            )
        vc = self._df[column].value_counts().reset_index()
        vc.columns = [column, "counts"]
        return DataFrameWrapper(vc.sort_values("counts", ascending=False), self._engine)
    
    def describe(self) -> Any:
        """Get describe statistics."""
        if self._engine == "polars":
            return self._df.describe()
        return self._df.describe()
    
    def null_count(self) -> dict:
        """Get null counts per column."""
        if self._engine == "polars":
            return {c: self._df[c].null_count() for c in self._df.columns}
        return self._df.isna().sum().to_dict()
    
    def copy(self) -> "DataFrameWrapper":
        """Create a copy."""
        if self._engine == "polars":
            return DataFrameWrapper(self._df.clone(), self._engine)
        return DataFrameWrapper(self._df.copy(), self._engine)
    
    def iterrows(self):
        """Iterate over rows."""
        if self._engine == "polars":
            for idx, row in enumerate(self._df.iter_rows()):
                yield idx, row
        else:
            for idx, row in self._df.iterrows():
                yield idx, row
    
    def __repr__(self) -> str:
        return f"DataFrameWrapper(engine={self._engine}, shape={self.shape})"


def get_engine(force_engine: Optional[str] = None) -> DataFrameWrapper:
    """
    Get a DataFrameWrapper for the current engine.
    
    Args:
        force_engine: Force a specific engine ("polars" or "pandas")
    
    Returns:
        DataFrameWrapper
    
    Raises:
        ImportError: If neither polars nor pandas is available
    """
    engine = determine_engine(force_engine)
    if engine == "polars":
        df = pl.DataFrame()
    else:
        df = pd.DataFrame()
    return DataFrameWrapper(df, engine)


# CSV loading functions
def read_csv(path: str, force_engine: Optional[str] = None, **kwargs) -> DataFrameWrapper:
    """
    Read a CSV file into a DataFrameWrapper.
    
    Args:
        path: Path to CSV file
        force_engine: Force a specific engine ("polars" or "pandas")
        **kwargs: Additional arguments passed to CSV reader
    
    Returns:
        DataFrameWrapper
    """
    engine = determine_engine(force_engine)
    
    # Auto-detect delimiter if not specified
    if "separator" in kwargs:
        kwargs["sep"] = kwargs.pop("separator")
    elif "delimiter" in kwargs:
        kwargs["sep"] = kwargs.pop("delimiter")
    else:
        # Try to detect delimiter
        try:
            with open(path, 'r') as f:
                first_line = f.readline()
            # Common delimiters
            for sep in [',', ';', '\t', '|']:
                if sep in first_line:
                    kwargs["sep"] = sep
                    break
        except:
            pass
    
    if engine == "polars":
        try:
            df = pl.read_csv(path, **kwargs)
        except Exception as e:
            # Fallback to pandas for complex cases
            if PANDAS_AVAILABLE:
                df = pd.read_csv(path, **kwargs)
                engine = "pandas"
            else:
                raise e
    else:
        df = pd.read_csv(path, **kwargs)
    
    return DataFrameWrapper(df, engine)


def read_sql(query: str, connection_string: str, force_engine: Optional[str] = None) -> DataFrameWrapper:
    """
    Read data from SQL query.
    
    Args:
        query: SQL query to execute
        connection_string: SQLAlchemy connection string
        force_engine: Force a specific engine
    
    Returns:
        DataFrameWrapper
    """
    if not SQLALCHEMY_AVAILABLE:
        raise ImportError("SQLAlchemy is required for SQL support. Install with: pip install sqlalchemy")
    
    engine = determine_engine(force_engine)
    sa_engine = create_engine(connection_string)
    try:
        with sa_engine.connect() as con:
            if engine == "polars":
                df = pl.read_sql(query, connection_string)
            else:
                df = pd.read_sql(query, con)
    finally:
        sa_engine.dispose()
    return DataFrameWrapper(df, engine)


def read_sqlite(path: str, table_name: str, force_engine: Optional[str] = None) -> DataFrameWrapper:
    """
    Read data from SQLite table.
    
    Args:
        path: Path to SQLite database
        table_name: Name of table to read
        force_engine: Force a specific engine
    
    Returns:
        DataFrameWrapper
    """
    import re
    if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', table_name):
        raise ValueError(f"Invalid table name: {table_name}")
    conn_str = f"sqlite:///{path}"
    query = f"SELECT * FROM \"{table_name}\""
    return read_sql(query, conn_str, force_engine)


# Alias
load_table = read_csv
