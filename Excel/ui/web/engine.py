"""
DataEngine Pro - Streamlit Engine Wrapper
==========================================

This module bridges the Streamlit UI with the existing data_engine operations.
It provides a clean interface for the UI to use all the existing modules.

Usage:
    from ui.web.engine import Engine
    engine = Engine(st.session_state)
    engine.filter(...)
    engine.sort(...)
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, Any, List, Tuple
import traceback


class Engine:
    """
    Engine wrapper that connects Streamlit session to data_engine operations.
    Provides a simple API for all data operations while using the existing modules.
    """
    
    def __init__(self, session_state):
        """
        Initialize the engine with Streamlit session state.
        
        Args:
            session_state: Streamlit session_state object containing session_tables
        """
        self._session = session_state
        
        # Initialize session tables if not exist
        if 'session_tables' not in self._session:
            self._session.session_tables = {}
        if 'active_table' not in self._session:
            self._session.active_table = ""
        if 'operation_history' not in self._session:
            self._session.operation_history = []
    
    # =========================================================================
    # DATA LOADING
    # =========================================================================
    
    def load_csv(self, file, chunk_size: int = 50000) -> Tuple[bool, str]:
        """Load CSV file into session."""
        try:
            import os
            
            # Get file size
            file.seek(0, os.SEEK_END)
            file_size = file.tell()
            file.seek(0)
            
            # For large files, use chunked reading
            if file_size > 50 * 1024 * 1024:  # > 50MB
                chunks = []
                for chunk in pd.read_csv(file, chunksize=chunk_size):
                    chunks.append(chunk)
                df = pd.concat(chunks, ignore_index=True)
            else:
                df = pd.read_csv(file)
            
            # Create table name
            table_name = "".join(c for c in file.name.replace('.csv', '') 
                               if c.isalnum() or c in ('_', '-'))
            
            self._session.session_tables[table_name] = df
            self._session.active_table = table_name
            self._log_operation(f"Loaded CSV: {table_name} ({len(df):,} rows)")
            
            return True, f"Loaded {table_name} ({len(df):,} rows, {len(df.columns)} columns)"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def load_excel(self, file) -> Tuple[bool, str]:
        """Load Excel file into session."""
        try:
            import os
            
            file.seek(0, os.SEEK_END)
            file_size = file.tell()
            file.seek(0)
            
            table_names = []
            
            if file_size > 50 * 1024 * 1024:  # > 50MB
                # Large file - just first sheet
                df = pd.read_excel(file, sheet_name=0)
                table_name = "".join(c for c in file.name.replace('.xlsx', '').replace('.xls', '') 
                                   if c.isalnum() or c in ('_', '-'))
                self._session.session_tables[table_name] = df
                self._session.active_table = table_name
                table_names.append(table_name)
            else:
                # Read all sheets
                excel_file = pd.ExcelFile(file)
                for sheet_name in excel_file.sheet_names:
                    df = pd.read_excel(excel_file, sheet_name=sheet_name)
                    base_name = "".join(c for c in file.name.replace('.xlsx', '').replace('.xls', '') 
                                       if c.isalnum() or c in ('_', '-'))
                    table_name = f"{base_name}_{sheet_name}" if len(excel_file.sheet_names) > 1 else base_name
                    table_name = "".join(c for c in table_name if c.isalnum() or c in ('_', '-'))
                    self._session.session_tables[table_name] = df
                    table_names.append(table_name)
                
                self._session.active_table = table_names[0]
            
            total_rows = sum(len(self._session.session_tables[t]) for t in table_names)
            self._log_operation(f"Loaded Excel: {', '.join(table_names)} ({total_rows:,} rows)")
            
            return True, f"Loaded {len(table_names)} sheet(s): {', '.join(table_names)}"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    # =========================================================================
    # TABLE MANAGEMENT
    # =========================================================================
    
    @property
    def tables(self) -> List[str]:
        """Get list of all table names."""
        return list(self._session.session_tables.keys())
    
    @property
    def active_table(self) -> str:
        """Get active table name."""
        return self._session.active_table
    
    @active_table.setter
    def active_table(self, name: str):
        """Set active table."""
        if name in self._session.session_tables:
            self._session.active_table = name
    
    def get_table(self, name: Optional[str] = None) -> pd.DataFrame:
        """Get DataFrame by name (or active table if None)."""
        table_name = name or self._session.active_table
        if table_name not in self._session.session_tables:
            raise ValueError(f"Table '{table_name}' not found")
        return self._session.session_tables[table_name]
    
    def get_table_info(self, name: Optional[str] = None) -> Dict[str, Any]:
        """Get information about a table."""
        table_name = name or self._session.active_table
        if table_name not in self._session.session_tables:
            return {}
        
        df = self._session.session_tables[table_name]
        
        # Get column info
        col_info = []
        for col in df.columns:
            col_info.append({
                "name": col,
                "dtype": str(df[col].dtype),
                "null_count": int(df[col].isna().sum()),
                "unique_count": int(df[col].nunique()),
                "sample": df[col].dropna().head(3).tolist()
            })
        
        return {
            "name": table_name,
            "rows": len(df),
            "cols": len(df.columns),
            "columns": col_info,
            "memory_kb": df.memory_usage(deep=True).sum() / 1024
        }
    
    def add_table(self, name: str, df: pd.DataFrame) -> None:
        """Add a new table to session."""
        self._session.session_tables[name] = df
        self._session.active_table = name
        self._log_operation(f"Created table: {name} ({len(df):,} rows)")
    
    def delete_table(self, name: str) -> bool:
        """Delete a table from session."""
        if name in self._session.session_tables:
            del self._session.session_tables[name]
            if self._session.active_table == name:
                self._session.active_table = list(self._session.session_tables.keys())[0] if self._session.session_tables else ""
            self._log_operation(f"Deleted table: {name}")
            return True
        return False
    
    def duplicate_table(self, source: str, dest: str) -> Tuple[bool, str]:
        """Duplicate a table."""
        if source not in self._session.session_tables:
            return False, f"Source table '{source}' not found"
        
        self._session.session_tables[dest] = self._session.session_tables[source].copy()
        self._session.active_table = dest
        self._log_operation(f"Duplicated {source} -> {dest}")
        return True, f"Created '{dest}'"
    
    # =========================================================================
    # FILTERING
    # =========================================================================
    
    def filter(self, table_name: Optional[str] = None,
               column: str = None, operator: str = "==", 
               value: Any = None, save_as: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """
        Apply filter to a table.
        
        Args:
            table_name: Table to filter (default: active)
            column: Column to filter on
            operator: Operator (==, !=, >, <, >=, <=, contains, startswith, endswith, is_blank, is_not_blank)
            value: Value to filter by
            save_as: Name for new filtered table
        
        Returns:
            Tuple of (filtered DataFrame, status message)
        """
        df = self.get_table(table_name).copy()
        
        if column not in df.columns:
            return df, f"Column '{column}' not found"
        
        # Handle blank conditions
        if operator in ("is_blank", "IS BLANK", "BLANK", "ISNULL"):
            result = df[df[column].isna() | (df[column].astype(str).str.strip() == "")]
            msg = f"Filtered to {len(result):,} rows where {column} is blank"
        elif operator in ("is_not_blank", "IS NOT BLANK", "NOT BLANK", "ISNOTNULL"):
            result = df[df[column].notna() & (df[column].astype(str).str.strip() != "")]
            msg = f"Filtered to {len(result):,} rows where {column} is not blank"
        elif operator == "contains":
            result = df[df[column].astype(str).str.contains(str(value), case=False, na=False)]
            msg = f"Filtered to {len(result):,} rows containing '{value}'"
        elif operator == "startswith":
            result = df[df[column].astype(str).str.startswith(str(value), na=False)]
            msg = f"Filtered to {len(result):,} rows starting with '{value}'"
        elif operator == "endswith":
            result = df[df[column].astype(str).str.endswith(str(value), na=False)]
            msg = f"Filtered to {len(result):,} rows ending with '{value}'"
        elif operator in ("==", "equals"):
            # Try numeric first
            try:
                num_val = float(value)
                result = df[df[column] == num_val]
            except ValueError:
                result = df[df[column].astype(str).str.lower() == str(value).lower()]
            msg = f"Filtered to {len(result):,} rows where {column} = '{value}'"
        elif operator == "!=":
            try:
                num_val = float(value)
                result = df[df[column] != num_val]
            except ValueError:
                result = df[df[column].astype(str).str.lower() != str(value).lower()]
            msg = f"Filtered to {len(result):,} rows where {column} != '{value}'"
        elif operator == ">":
            try:
                num_val = float(value)
                result = df[df[column] > num_val]
                msg = f"Filtered to {len(result):,} rows where {column} > {value}"
            except ValueError:
                return df, "Cannot apply > to non-numeric column"
        elif operator == "<":
            try:
                num_val = float(value)
                result = df[df[column] < num_val]
                msg = f"Filtered to {len(result):,} rows where {column} < {value}"
            except ValueError:
                return df, "Cannot apply < to non-numeric column"
        elif operator == ">=":
            try:
                num_val = float(value)
                result = df[df[column] >= num_val]
                msg = f"Filtered to {len(result):,} rows where {column} >= {value}"
            except ValueError:
                return df, "Cannot apply >= to non-numeric column"
        elif operator == "<=":
            try:
                num_val = float(value)
                result = df[df[column] <= num_val]
                msg = f"Filtered to {len(result):,} rows where {column} <= {value}"
            except ValueError:
                return df, "Cannot apply <= to non-numeric column"
        else:
            return df, f"Unknown operator: {operator}"
        
        # Save if requested
        if save_as:
            self._session.session_tables[save_as] = result
            self._session.active_table = save_as
            self._log_operation(f"Filter: {save_as} <- {table_name} ({column} {operator} {value})")
            msg = f"{msg}. Saved as '{save_as}'"
        
        return result, msg
    
    def multi_filter(self, table_name: Optional[str] = None,
                    conditions: List[Dict] = None,
                    logic: str = "AND",
                    save_as: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """
        Apply multiple filter conditions.
        
        Args:
            table_name: Table to filter
            conditions: List of {"column": col, "operator": op, "value": val}
            logic: "AND" or "OR"
            save_as: Name for result table
        """
        df = self.get_table(table_name).copy()
        
        if not conditions:
            return df, "No conditions provided"
        
        mask = pd.Series([True] * len(df), index=df.index)
        
        for cond in conditions:
            col = cond.get("column")
            op = cond.get("operator")
            val = cond.get("value")
            
            if col not in df.columns:
                continue
            
            # Get condition mask
            if op in ("is_blank", "IS BLANK"):
                cond_mask = df[col].isna() | (df[col].astype(str).str.strip() == "")
            elif op in ("is_not_blank", "IS NOT BLANK"):
                cond_mask = df[col].notna() & (df[col].astype(str).str.strip() != "")
            elif op == "contains":
                cond_mask = df[col].astype(str).str.contains(str(val), case=False, na=False)
            else:
                try:
                    num_val = float(val)
                    if op == "==":
                        cond_mask = df[col] == num_val
                    elif op == "!=":
                        cond_mask = df[col] != num_val
                    elif op == ">":
                        cond_mask = df[col] > num_val
                    elif op == "<":
                        cond_mask = df[col] < num_val
                    elif op == ">=":
                        cond_mask = df[col] >= num_val
                    elif op == "<=":
                        cond_mask = df[col] <= num_val
                    else:
                        cond_mask = pd.Series([False] * len(df))
                except ValueError:
                    cond_mask = df[col].astype(str).str.lower() == str(val).lower()
            
            if logic == "AND":
                mask = mask & cond_mask
            else:
                mask = mask | cond_mask
        
        result = df[mask]
        
        if save_as:
            self._session.session_tables[save_as] = result
            self._session.active_table = save_as
        
        return result, f"Multi-filter: {len(result):,} rows ({logic} of {len(conditions)} conditions)"
    
    # =========================================================================
    # SORTING
    # =========================================================================
    
    def sort(self, table_name: Optional[str] = None,
             by: List[str] = None,
             ascending: bool = True,
             save_as: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """Sort table by column(s)."""
        df = self.get_table(table_name).copy()
        
        if not by:
            return df, "No columns specified for sorting"
        
        # Validate columns
        valid_cols = [c for c in by if c in df.columns]
        if not valid_cols:
            return df, "No valid columns found"
        
        try:
            result = df.sort_values(by=valid_cols, ascending=ascending)
            direction = "↑" if ascending else "↓"
            msg = f"Sorted by {', '.join(valid_cols)} {direction}"
            
            if save_as:
                self._session.session_tables[save_as] = result
                self._session.active_table = save_as
                msg += f". Saved as '{save_as}'"
            
            return result, msg
        except Exception as e:
            return df, f"Sort error: {str(e)}"
    
    # =========================================================================
    # AGGREGATION
    # =========================================================================
    
    def aggregate(self, table_name: Optional[str] = None,
                 group_by: List[str] = None,
                 agg_column: str = None,
                 agg_func: str = "sum",
                 save_as: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """Aggregate/group data."""
        df = self.get_table(table_name).copy()
        
        if not group_by:
            # Simple aggregation on single column
            if agg_column and agg_column in df.columns:
                if agg_func == "count":
                    result = pd.DataFrame({agg_column: [len(df)]})
                elif agg_func == "sum":
                    result = pd.DataFrame({agg_column: [df[agg_column].sum()]})
                elif agg_func == "mean":
                    result = pd.DataFrame({agg_column: [df[agg_column].mean()]})
                elif agg_func == "min":
                    result = pd.DataFrame({agg_column: [df[agg_column].min()]})
                elif agg_func == "max":
                    result = pd.DataFrame({agg_column: [df[agg_column].max()]})
                else:
                    return df, f"Unknown aggregation: {agg_func}"
                
                msg = f"{agg_func.upper()} of {agg_column}: {result[agg_column].iloc[0]}"
            else:
                return df, "No aggregation column specified"
        else:
            # Group by
            if not agg_column:
                # Just count
                result = df.groupby(group_by, dropna=False).size().reset_index(name="Count")
            else:
                result = df.groupby(group_by, dropna=False)[agg_column].agg(agg_func).reset_index()
                result.columns = list(group_by) + [f"{agg_func}_{agg_column}"]
            
            result = result.sort_values(result.columns[-1], ascending=False)
            msg = f"Aggregated: {len(result):,} groups"
        
        if save_as:
            self._session.session_tables[save_as] = result
            self._session.active_table = save_as
            msg += f". Saved as '{save_as}'"
        
        return result, msg
    
    def pivot(self, table_name: Optional[str] = None,
              index: List[str] = None,
              values: str = None,
              aggfunc: str = "sum",
              save_as: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """Create pivot table."""
        df = self.get_table(table_name).copy()
        
        if not index:
            return df, "No index columns specified"
        
        try:
            if values:
                result = df.pivot_table(index=index, values=values, 
                                       aggfunc=aggfunc, dropna=False).reset_index()
            else:
                # Just count
                result = df.pivot_table(index=index, aggfunc='count', dropna=False).reset_index()
            
            msg = f"Pivot table: {len(result):,} rows"
            
            if save_as:
                self._session.session_tables[save_as] = result
                self._session.active_table = save_as
                msg += f". Saved as '{save_as}'"
            
            return result, msg
        except Exception as e:
            return df, f"Pivot error: {str(e)}"
    
    # =========================================================================
    # TRANSFORMATIONS
    # =========================================================================
    
    def add_column(self, table_name: Optional[str] = None,
                   new_column: str = None,
                   formula: str = None,
                   save_as: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """Add new column with formula."""
        df = self.get_table(table_name).copy()
        
        if not new_column:
            return df, "No column name specified"
        
        if not formula:
            return df, "No formula specified"
        
        try:
            # Use pandas eval for formulas
            safe_scope = {col: df[col] for col in df.columns}
            df[new_column] = pd.eval(formula, local_dict=safe_scope, engine='numexpr')
            msg = f"Added column '{new_column}' with formula"
            
            if save_as:
                self._session.session_tables[save_as] = df
                self._session.active_table = save_as
                msg += f". Saved as '{save_as}'"
            
            return df, msg
        except Exception as e:
            return df, f"Formula error: {str(e)}"
    
    def add_conditional_column(self, table_name: Optional[str] = None,
                               new_column: str = None,
                               condition: str = None,
                               true_value: Any = None,
                               false_value: Any = None,
                               save_as: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """Add conditional (IF) column."""
        df = self.get_table(table_name).copy()
        
        if not new_column or not condition:
            return df, "Missing parameters"
        
        try:
            mask = df.eval(condition)
            df[new_column] = np.where(mask, true_value, false_value)
            msg = f"Added conditional column '{new_column}'"
            
            if save_as:
                self._session.session_tables[save_as] = df
                self._session.active_table = save_as
                msg += f". Saved as '{save_as}'"
            
            return df, msg
        except Exception as e:
            return df, f"Error: {str(e)}"
    
    def transform_column(self, table_name: Optional[str] = None,
                        column: str = None,
                        transform: str = None,
                        save_as: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """Transform column values."""
        df = self.get_table(table_name).copy()
        
        if not column or column not in df.columns:
            return df, f"Column '{column}' not found"
        
        transform = transform.lower() if transform else "none"
        
        try:
            if transform == "upper":
                df[column] = df[column].astype(str).str.upper()
            elif transform == "lower":
                df[column] = df[column].astype(str).str.lower()
            elif transform == "title":
                df[column] = df[column].astype(str).str.title()
            elif transform == "strip":
                df[column] = df[column].astype(str).str.strip()
            elif transform == "int":
                df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
            elif transform == "float":
                df[column] = pd.to_numeric(df[column], errors="coerce")
            elif transform == "clean":
                # Remove currency symbols, commas, etc
                df[column] = df[column].astype(str).str.replace(r'[$,\s%]', '', regex=True)
                df[column] = pd.to_numeric(df[column], errors="coerce")
            elif transform == "date":
                df[column] = pd.to_datetime(df[column], errors="coerce")
            else:
                return df, f"Unknown transform: {transform}"
            
            msg = f"Transformed '{column}' with {transform}"
            
            if save_as:
                self._session.session_tables[save_as] = df
                self._session.active_table = save_as
            
            return df, msg
        except Exception as e:
            return df, f"Transform error: {str(e)}"
    
    def map_values(self, table_name: Optional[str] = None,
                  column: str = None,
                  mappings: Dict[str, str] = None,
                  default: str = None,
                  save_as: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """Map values (VLOOKUP-like)."""
        df = self.get_table(table_name).copy()
        
        if not column or column not in df.columns:
            return df, f"Column '{column}' not found"
        
        if not mappings:
            return df, "No mappings provided"
        
        try:
            df[column] = df[column].astype(str).map(
                lambda v: mappings.get(str(v).strip(), default if default else np.nan)
            )
            msg = f"Mapped {len(mappings)} values in '{column}'"
            
            if save_as:
                self._session.session_tables[save_as] = df
                self._session.active_table = save_as
            
            return df, msg
        except Exception as e:
            return df, f"Map error: {str(e)}"
    
    # =========================================================================
    # FIND & REPLACE
    # =========================================================================
    
    def find_replace(self, table_name: Optional[str] = None,
                    column: str = None,
                    find: str = None,
                    replace: str = "",
                    case_sensitive: bool = False,
                    whole_cell: bool = True,
                    save_as: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """Find and replace values."""
        df = self.get_table(table_name).copy()
        
        if not find:
            return df, "No search term specified"
        
        try:
            if column and column in df.columns:
                # Single column
                if whole_cell:
                    if case_sensitive:
                        mask = df[column] == find
                    else:
                        mask = df[column].astype(str).str.lower() == find.lower()
                else:
                    if case_sensitive:
                        mask = df[column].astype(str).str.contains(find, regex=False, na=False)
                    else:
                        mask = df[column].astype(str).str.contains(find, case=False, regex=False, na=False)
                
                count = mask.sum()
                df.loc[mask, column] = replace
                msg = f"Replaced {count} occurrences in '{column}'"
            else:
                # All columns
                count = 0
                for col in df.columns:
                    if whole_cell:
                        if case_sensitive:
                            mask = df[col] == find
                        else:
                            mask = df[col].astype(str).str.lower() == find.lower()
                    else:
                        if case_sensitive:
                            mask = df[col].astype(str).str.contains(find, regex=False, na=False)
                        else:
                            mask = df[col].astype(str).str.contains(find, case=False, regex=False, na=False)
                    
                    df.loc[mask, col] = replace
                    count += mask.sum()
                
                msg = f"Replaced {count} occurrences across all columns"
            
            if save_as:
                self._session.session_tables[save_as] = df
                self._session.active_table = save_as
            
            return df, msg
        except Exception as e:
            return df, f"Error: {str(e)}"
    
    # =========================================================================
    # DATA CLEANING
    # =========================================================================
    
    def handle_nulls(self, table_name: Optional[str] = None,
                    columns: List[str] = None,
                    action: str = "fill",
                    fill_value: Any = None,
                    save_as: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """Handle missing values."""
        df = self.get_table(table_name).copy()
        
        if not columns:
            columns = list(df.columns)
        
        valid_cols = [c for c in columns if c in df.columns]
        if not valid_cols:
            return df, "No valid columns"
        
        try:
            if action == "delete":
                # Delete rows with nulls
                before = len(df)
                df = df.dropna(subset=valid_cols)
                msg = f"Deleted {before - len(df):,} rows with missing values"
            elif action == "fill":
                if fill_value is None:
                    fill_value = 0
                for col in valid_cols:
                    df[col] = df[col].fillna(fill_value)
                msg = f"Filled {len(valid_cols)} columns with '{fill_value}'"
            elif action == "fill_mean":
                for col in valid_cols:
                    if pd.api.types.is_numeric_dtype(df[col]):
                        df[col] = df[col].fillna(df[col].mean())
                msg = f"Filled numeric columns with mean"
            elif action == "fill_median":
                for col in valid_cols:
                    if pd.api.types.is_numeric_dtype(df[col]):
                        df[col] = df[col].fillna(df[col].median())
                msg = f"Filled numeric columns with median"
            elif action == "fill_ffill":
                for col in valid_cols:
                    df[col] = df[col].ffill()
                msg = f"Forward-filled {len(valid_cols)} columns"
            elif action == "fill_bfill":
                for col in valid_cols:
                    df[col] = df[col].bfill()
                msg = f"Backward-filled {len(valid_cols)} columns"
            else:
                return df, f"Unknown action: {action}"
            
            if save_as:
                self._session.session_tables[save_as] = df
                self._session.active_table = save_as
            
            return df, msg
        except Exception as e:
            return df, f"Error: {str(e)}"
    
    def deduplicate(self, table_name: Optional[str] = None,
                   subset: List[str] = None,
                   save_as: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """Remove duplicate rows."""
        df = self.get_table(table_name).copy()
        
        before = len(df)
        if subset:
            df = df.drop_duplicates(subset=subset)
        else:
            df = df.drop_duplicates()
        
        removed = before - len(df)
        msg = f"Removed {removed:,} duplicate rows ({len(df):,} unique remain)"
        
        if save_as:
            self._session.session_tables[save_as] = df
            self._session.active_table = save_as
        
        return df, msg
    
    def change_type(self, table_name: Optional[str] = None,
                   column: str = None,
                   new_type: str = "str",
                   save_as: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """Change column data type."""
        df = self.get_table(table_name).copy()
        
        if not column or column not in df.columns:
            return df, f"Column '{column}' not found"
        
        try:
            old_type = str(df[column].dtype)
            
            if new_type == "str":
                df[column] = df[column].astype(str)
            elif new_type == "int":
                df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
            elif new_type == "float":
                df[column] = pd.to_numeric(df[column], errors="coerce")
            elif new_type == "bool":
                df[column] = df[column].astype(str).str.lower().map(
                    lambda x: True if x in ('true', '1', 'yes', 'y', 't', 'on') 
                    else (False if x in ('false', '0', 'no', 'n', 'f', 'off') else np.nan)
                )
            elif new_type == "date":
                df[column] = pd.to_datetime(df[column], errors="coerce")
            else:
                return df, f"Unknown type: {new_type}"
            
            msg = f"Changed '{column}' from {old_type} to {new_type}"
            
            if save_as:
                self._session.session_tables[save_as] = df
                self._session.active_table = save_as
            
            return df, msg
        except Exception as e:
            return df, f"Error: {str(e)}"
    
    def rename_column(self, table_name: Optional[str] = None,
                     old_name: str = None,
                     new_name: str = None,
                     save_as: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """Rename a column."""
        df = self.get_table(table_name).copy()
        
        if old_name not in df.columns:
            return df, f"Column '{old_name}' not found"
        
        df = df.rename(columns={old_name: new_name})
        msg = f"Renamed '{old_name}' to '{new_name}'"
        
        if save_as:
            self._session.session_tables[save_as] = df
            self._session.active_table = save_as
        
        return df, msg
    
    def delete_columns(self, table_name: Optional[str] = None,
                      columns: List[str] = None,
                      save_as: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """Delete columns."""
        df = self.get_table(table_name).copy()
        
        if not columns:
            return df, "No columns specified"
        
        valid_cols = [c for c in columns if c in df.columns]
        if not valid_cols:
            return df, "No valid columns to delete"
        
        df = df.drop(columns=valid_cols)
        msg = f"Deleted columns: {', '.join(valid_cols)}"
        
        if save_as:
            self._session.session_tables[save_as] = df
            self._session.active_table = save_as
        
        return df, msg
    
    # =========================================================================
    # JOINS
    # =========================================================================
    
    def join(self, left_table: str = None,
            right_table: str = None,
            left_key: str = None,
            right_key: str = None,
            how: str = "left",
            save_as: str = None) -> Tuple[pd.DataFrame, str]:
        """Join two tables."""
        if not left_table or not right_table:
            return pd.DataFrame(), "Both tables must be specified"
        
        if left_table not in self._session.session_tables:
            return pd.DataFrame(), f"Table '{left_table}' not found"
        if right_table not in self._session.session_tables:
            return pd.DataFrame(), f"Table '{right_table}' not found"
        
        df1 = self._session.session_tables[left_table]
        df2 = self._session.session_tables[right_table]
        
        if left_key not in df1.columns:
            return pd.DataFrame(), f"Key '{left_key}' not in '{left_table}'"
        if right_key not in df2.columns:
            return pd.DataFrame(), f"Key '{right_key}' not in '{right_table}'"
        
        try:
            # Get columns to add (exclude key)
            add_cols = [c for c in df2.columns if c != right_key]
            
            result = df1.merge(df2[[right_key] + add_cols], 
                            left_on=left_key, right_on=right_key, 
                            how=how, suffixes=('', '_y'))
            
            msg = f"Joined: {len(result):,} rows × {len(result.columns)} columns"
            
            if save_as:
                self._session.session_tables[save_as] = result
                self._session.active_table = save_as
            
            return result, msg
        except Exception as e:
            return pd.DataFrame(), f"Join error: {str(e)}"
    
    # =========================================================================
    # RANKING
    # =========================================================================
    
    def rank(self, table_name: Optional[str] = None,
            column: str = None,
            by: str = None,
            ascending: bool = False,
            method: str = "dense",
            save_as: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """Add ranking column."""
        df = self.get_table(table_name).copy()
        
        if not column:
            return df, "No column specified"
        
        if column not in df.columns:
            return df, f"Column '{column}' not found"
        
        try:
            rank_col = f"{column}_rank" if not by else by
            
            if by:
                # Rank within groups
                df[rank_col] = df.groupby(by)[column].rank(
                    ascending=ascending, 
                    method=method
                )
            else:
                df[rank_col] = df[column].rank(
                    ascending=ascending,
                    method=method
                )
            
            msg = f"Added ranking column '{rank_col}'"
            
            if save_as:
                self._session.session_tables[save_as] = df
                self._session.active_table = save_as
            
            return df, msg
        except Exception as e:
            return df, f"Error: {str(e)}"
    
    # =========================================================================
    # ANALYSIS
    # =========================================================================
    
    def profile(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """Get data profile for a table."""
        df = self.get_table(table_name)
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        date_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
        obj_cols = df.select_dtypes(include=['object']).columns.tolist()
        
        profile = {
            "table_name": self._session.active_table,
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "numeric_columns": len(numeric_cols),
            "date_columns": len(date_cols),
            "text_columns": len(obj_cols),
            "memory_kb": df.memory_usage(deep=True).sum() / 1024,
            "null_counts": df.isna().sum().to_dict(),
            "column_types": {col: str(df[col].dtype) for col in df.columns}
        }
        
        # Add numeric stats
        if numeric_cols:
            profile["numeric_stats"] = {}
            for col in numeric_cols[:10]:  # Limit to first 10
                profile["numeric_stats"][col] = {
                    "mean": float(df[col].mean()) if not df[col].isna().all() else None,
                    "median": float(df[col].median()) if not df[col].isna().all() else None,
                    "std": float(df[col].std()) if not df[col].isna().all() else None,
                    "min": float(df[col].min()) if not df[col].isna().all() else None,
                    "max": float(df[col].max()) if not df[col].isna().all() else None
                }
        
        # Top values for each column
        profile["top_values"] = {}
        for col in df.columns[:10]:
            vc = df[col].dropna().value_counts().head(5)
            profile["top_values"][col] = [{"value": str(v), "count": int(c)} for v, c in vc.items()]
        
        return profile
    
    def correlation(self, table_name: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """Get correlation matrix."""
        df = self.get_table(table_name)
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if len(numeric_cols) < 2:
            return pd.DataFrame(), "Need at least 2 numeric columns"
        
        corr = df[numeric_cols].corr()
        return corr, f"Correlation matrix: {len(numeric_cols)} columns"
    
    def outliers(self, table_name: Optional[str] = None,
                column: str = None,
                method: str = "iqr",
                threshold: float = 1.5) -> Tuple[pd.DataFrame, str]:
        """Detect outliers."""
        df = self.get_table(table_name).copy()
        
        if not column or column not in df.columns:
            return df, f"Column '{column}' not found"
        
        if not pd.api.types.is_numeric_dtype(df[column]):
            return df, f"Column '{column}' is not numeric"
        
        try:
            col_data = pd.to_numeric(df[column], errors="coerce")
            
            if method == "iqr":
                Q1 = col_data.quantile(0.25)
                Q3 = col_data.quantile(0.75)
                IQR = Q3 - Q1
                lower = Q1 - threshold * IQR
                upper = Q3 + threshold * IQR
                mask = (col_data < lower) | (col_data > upper)
            elif method == "zscore":
                mean = col_data.mean()
                std = col_data.std()
                z_scores = abs((col_data - mean) / std)
                mask = z_scores > threshold
            else:
                return df, f"Unknown method: {method}"
            
            outliers = df[mask]
            msg = f"Found {len(outliers):,} outliers ({len(outliers)/len(df)*100:.1f}% of data)"
            
            return outliers, msg
        except Exception as e:
            return df, f"Error: {str(e)}"
    
    def segment(self, table_name: Optional[str] = None,
               column: str = None,
               bins: int = 5,
               labels: List[str] = None,
               save_as: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """Segment numeric column into bins."""
        df = self.get_table(table_name).copy()
        
        if not column or column not in df.columns:
            return df, f"Column '{column}' not found"
        
        if not pd.api.types.is_numeric_dtype(df[column]):
            return df, f"Column '{column}' is not numeric"
        
        try:
            col_data = pd.to_numeric(df[column], errors="coerce")
            
            if labels:
                df[f"{column}_segment"] = pd.cut(col_data, bins=bins, labels=labels)
            else:
                df[f"{column}_segment"] = pd.cut(col_data, bins=bins)
            
            msg = f"Created {len(df[f"{column}_segment"].unique())} segments"
            
            if save_as:
                self._session.session_tables[save_as] = df
                self._session.active_table = save_as
            
            return df, msg
        except Exception as e:
            return df, f"Error: {str(e)}"
    
    def time_extract(self, table_name: Optional[str] = None,
                    column: str = None,
                    extract: str = "year",
                    new_column: str = None,
                    save_as: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """Extract date components."""
        df = self.get_table(table_name).copy()
        
        if not column or column not in df.columns:
            return df, f"Column '{column}' not found"
        
        try:
            # Convert to datetime if needed
            if df[column].dtype != 'datetime64':
                df[column] = pd.to_datetime(df[column], errors="coerce")
            
            extract = extract.lower()
            new_col = new_column or f"{column}_{extract}"
            
            if extract == "year":
                df[new_col] = df[column].dt.year
            elif extract == "month":
                df[new_col] = df[column].dt.month
            elif extract == "day":
                df[new_col] = df[column].dt.day
            elif extract == "weekday":
                df[new_col] = df[column].dt.dayofweek
            elif extract == "quarter":
                df[new_col] = df[column].dt.quarter
            elif extract == "hour":
                df[new_col] = df[column].dt.hour
            else:
                return df, f"Unknown extract: {extract}"
            
            msg = f"Extracted {extract} from '{column}'"
            
            if save_as:
                self._session.session_tables[save_as] = df
                self._session.active_table = save_as
            
            return df, msg
        except Exception as e:
            return df, f"Error: {str(e)}"
    
    def crosstab(self, table_name: Optional[str] = None,
                row: str = None,
                col: str = None,
                save_as: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """Create cross-tabulation."""
        df = self.get_table(table_name)
        
        if not row or not col:
            return pd.DataFrame(), "Both row and column must be specified"
        
        if row not in df.columns or col not in df.columns:
            return pd.DataFrame(), "Invalid columns"
        
        try:
            ct = pd.crosstab(df[row], df[col])
            
            msg = f"Crosstab: {len(ct)} × {len(ct.columns)}"
            
            if save_as:
                self._session.session_tables[save_as] = ct.reset_index()
                self._session.active_table = save_as
            
            return ct, msg
        except Exception as e:
            return pd.DataFrame(), f"Error: {str(e)}"
    
    # =========================================================================
    # SMART FIX
    # =========================================================================
    
    def smart_fix(self, table_name: Optional[str] = None,
                  issue_type: str = None,
                  save_as: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """Apply smart fixes to common data issues."""
        df = self.get_table(table_name).copy()
        
        msg = ""
        
        try:
            if issue_type == "date_strings":
                # Convert date-like strings
                for col in df.columns:
                    if df[col].dtype == 'object':
                        sample = df[col].dropna().head(20).astype(str)
                        if len(sample) > 0:
                            try:
                                parsed = pd.to_datetime(sample, errors='coerce')
                                if parsed.notna().sum() / len(sample) >= 0.7:
                                    df[col] = pd.to_datetime(df[col], errors='coerce')
                                    msg += f"Converted {col} to date. "
                            except (ValueError, TypeError, pd.errors.ParserError):
                                pass
            
            elif issue_type == "numeric_text":
                # Convert numeric-as-text
                for col in df.columns:
                    if df[col].dtype == 'object':
                        try:
                            numeric = pd.to_numeric(df[col], errors='coerce')
                            if numeric.notna().sum() / df[col].notna().sum() > 0.8:
                                df[col] = numeric
                                msg += f"Converted {col} to numeric. "
                        except (ValueError, TypeError, ZeroDivisionError):
                            pass
            
            elif issue_type == "trim":
                # Trim whitespace
                for col in df.select_dtypes(include=['object']).columns:
                    df[col] = df[col].astype(str).str.strip()
                msg = "Trimmed whitespace from text columns"
            
            else:
                return df, f"Unknown issue type: {issue_type}"
            
            if not msg:
                msg = f"No fixes applied for {issue_type}"
            
            if save_as:
                self._session.session_tables[save_as] = df
                self._session.active_table = save_as
            
            return df, msg
        except Exception as e:
            return df, f"Error: {str(e)}"
    
    def scan_issues(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """Scan for data issues."""
        df = self.get_table(table_name)
        
        issues = {
            "date_like_strings": [],
            "numeric_as_text": [],
            "high_nulls": [],
            "mixed_types": [],
            "leading_trailing_spaces": []
        }
        
        for col in df.columns:
            # Check high nulls
            null_pct = df[col].isna().sum() / len(df) if len(df) > 0 else 0
            if null_pct >= 0.9:
                issues["high_nulls"].append(col)
            
            # Check date-like strings
            if df[col].dtype == 'object':
                sample = df[col].dropna().head(20).astype(str)
                if len(sample) > 0:
                    try:
                        parsed = pd.to_datetime(sample, errors='coerce')
                        if parsed.notna().sum() / len(sample) >= 0.7:
                            issues["date_like_strings"].append(col)
                    except (ValueError, TypeError, pd.errors.ParserError):
                        pass
                
                # Check numeric-as-text
                try:
                    numeric = pd.to_numeric(df[col], errors='coerce')
                    if numeric.notna().sum() / df[col].notna().sum() > 0.8:
                        issues["numeric_as_text"].append(col)
                except (ValueError, TypeError, ZeroDivisionError):
                    pass
                
                # Check whitespace
                leading = df[col].astype(str).str.startswith(' ').sum()
                trailing = df[col].astype(str).str.endswith(' ').sum()
                if leading > 0 or trailing > 0:
                    issues["leading_trailing_spaces"].append(col)
        
        return issues
    
    # =========================================================================
    # SEARCH
    # =========================================================================
    
    def search(self, table_name: Optional[str] = None,
              term: str = None,
              column: str = None) -> Tuple[pd.DataFrame, str]:
        """Search for term in data."""
        df = self.get_table(table_name)
        
        if not term:
            return pd.DataFrame(), "No search term"
        
        try:
            if column and column in df.columns:
                # Search in specific column
                mask = df[column].astype(str).str.contains(term, case=False, na=False)
                result = df[mask]
            else:
                # Search all columns
                mask = pd.Series(False, index=df.index)
                for col in df.columns:
                    mask |= df[col].astype(str).str.contains(term, case=False, na=False)
                result = df[mask]
            
            msg = f"Found {len(result):,} rows matching '{term}'"
            return result, msg
        except Exception as e:
            return pd.DataFrame(), f"Error: {str(e)}"
    
    # =========================================================================
    # EXPORT
    # =========================================================================
    
    def export_csv(self, table_name: Optional[str] = None) -> Tuple[str, str]:
        """Export table to CSV."""
        df = self.get_table(table_name)
        return df.to_csv(index=False), "text/csv"
    
    def export_excel(self, table_name: Optional[str] = None) -> Tuple[bytes, str]:
        """Export table to Excel."""
        import io
        df = self.get_table(table_name)
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name=self._session.active_table)
        return buffer.getvalue(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    
    def export_json(self, table_name: Optional[str] = None) -> Tuple[str, str]:
        """Export table to JSON."""
        df = self.get_table(table_name)
        return df.to_json(orient="records", indent=2), "application/json"
    
    # =========================================================================
    # UTILITY
    # =========================================================================
    
    def _log_operation(self, operation: str):
        """Log operation to history."""
        if 'operation_history' not in self._session:
            self._session.operation_history = []
        
        self._session.operation_history.append({
            'operation': operation,
            'table': self._session.active_table
        })
        
        # Keep only last 50
        if len(self._session.operation_history) > 50:
            self._session.operation_history = self._session.operation_history[-50:]
    
    def get_history(self) -> List[Dict]:
        """Get operation history."""
        return self._session.operation_history if 'operation_history' in self._session else []
    
    def preview(self, table_name: Optional[str] = None,
               rows: int = 10,
               columns: List[str] = None) -> pd.DataFrame:
        """Get preview of table."""
        df = self.get_table(table_name)
        
        if columns:
            valid_cols = [c for c in columns if c in df.columns]
            if valid_cols:
                df = df[valid_cols]
        
        return df.head(rows)
