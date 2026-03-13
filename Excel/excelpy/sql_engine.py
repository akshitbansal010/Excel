"""
SQL Engine using DuckDB for executing queries on pandas DataFrames.
"""
import duckdb
import pandas as pd

class SQLEngine:
    def __init__(self):
        # Initialize in-memory DuckDB connection
        self.con = duckdb.connect(database=':memory:')

    def query(self, df: pd.DataFrame, query_str: str) -> pd.DataFrame:
        """
        Execute SQL query on a pandas DataFrame.
        The DataFrame is registered as 'df' for the query.
        """
        try:
            self.con.register('df', df)
            result = self.con.execute(query_str).df()
            self.con.unregister('df')
            return result
        except Exception as e:
            raise ValueError(f"SQL Execution Error: {e}")

def run_sql_query(df: pd.DataFrame, query: str) -> pd.DataFrame:
    """Helper to run a query on a dataframe."""
    engine = SQLEngine()
    return engine.query(df, query)