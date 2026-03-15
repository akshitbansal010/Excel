"""
DataEngine Pro - Column Resolver
=================================

Fuzzy column name resolution with support for:
- Partial name matching
- Excel letter notation (A, B, AA, AB)
- Numeric indices
- Ambiguous match handling

Usage:
    from integration.column_resolver import ColumnResolver
    
    resolver = ColumnResolver(df)
    resolved = resolver.resolve("na")  # Finds "Name", "nationality", etc.
    resolved = resolver.resolve("A")    # Finds first column
    resolved = resolver.resolve("AA")   # Finds 27th column
"""

import pandas as pd
from typing import List, Dict, Any, Optional, Tuple, Union

# Try to import rapidfuzz for better fuzzy matching
try:
    from rapidfuzz import fuzz, process
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False
    fuzz = None
    process = None


class ColumnResolver:
    """
    Resolves partial or fuzzy column names to actual DataFrame column names.
    
    Supports:
    - Exact match
    - Case-insensitive match
    - Partial/substring match
    - Fuzzy match (with rapidfuzz)
    - Excel letter notation (A, B, AA = columns 1, 2, 27)
    - Numeric indices (0, 1, 2...)
    """
    
    def __init__(self, df: pd.DataFrame, threshold: float = 70):
        """
        Initialize resolver with a DataFrame.
        
        Args:
            df: The DataFrame to resolve columns against
            threshold: Minimum fuzzy match score (0-100) for suggestions
        """
        self.df = df
        self.columns = list(df.columns)
        self.column_count = len(self.columns)
        self.threshold = threshold
        
        # Build index maps
        self._name_to_index = {name: i for i, name in enumerate(self.columns)}
        self._name_lower = {name.lower(): name for name in self.columns}
    
    def _excel_letter_to_index(self, letter: str) -> Optional[int]:
        """
        Convert Excel column letter to 0-based index.
        
        Args:
            letter: Excel column letter (A, B, AA, AB, etc.)
            
        Returns:
            0-based column index or None if invalid
        """
        if not letter:
            return None
        
        letter = letter.upper().strip()
        if not letter or not letter.isalpha():
            return None
        
        index = 0
        for char in letter:
            index = index * 26 + (ord(char) - ord('A') + 1)
        
        return index - 1  # Convert to 0-based
    
    def _index_to_excel_letter(self, index: int) -> str:
        """Convert 0-based index to Excel column letter."""
        if index < 0:
            return ""
        
        result = ""
        index += 1
        while index > 0:
            index -= 1
            result = chr(ord('A') + index % 26) + result
            index //= 26
        
        return result
    
    def resolve(self, query: str, allow_partial: bool = True) -> Optional[str]:
        """
        Resolve a column query to an actual column name.
        
        Args:
            query: Column name, Excel letter, or numeric index
            allow_partial: Allow partial/fuzzy matches
            
        Returns:
            Resolved column name or None if not found
        """
        if not query:
            return None
        
        query = str(query).strip()
        
        # 1. Exact match
        if query in self.columns:
            return query
        
        # 2. Case-insensitive exact match
        query_lower = query.lower()
        if query_lower in self._name_lower:
            return self._name_lower[query_lower]
        
        # 3. Numeric index
        if query.isdigit():
            idx = int(query)
            if 0 <= idx < self.column_count:
                return self.columns[idx]
        
        # 4. Excel letter notation
        excel_idx = self._excel_letter_to_index(query)
        if excel_idx is not None and 0 <= excel_idx < self.column_count:
            return self.columns[excel_idx]
        
        # 5. Partial match (substring)
        if allow_partial:
            matches = self._partial_match(query_lower)
            if len(matches) == 1:
                return matches[0]
            elif len(matches) > 1:
                # Multiple matches - return None to indicate ambiguity
                return None
        
        # 6. Fuzzy match (if rapidfuzz available)
        if allow_partial and RAPIDFUZZ_AVAILABLE:
            fuzzy_match = self._fuzzy_match(query)
            if fuzzy_match:
                return fuzzy_match
        
        return None
    
    def _partial_match(self, query: str) -> List[str]:
        """Find columns that contain the query as a substring."""
        query_lower = query.lower()
        matches = []
        for col in self.columns:
            if query_lower in col.lower():
                matches.append(col)
        return matches
    
    def _fuzzy_match(self, query: str, limit: int = 1) -> Optional[str]:
        """Find best fuzzy match."""
        if not RAPIDFUZZ_AVAILABLE or not self.columns:
            return None
        
        # Use rapidfuzz to find best match
        result = process.extractOne(
            query,
            self.columns,
            scorer=fuzz.WRatio
        )
        
        if result and result[1] >= self.threshold:
            return result[0]
        
        return None
    
    def resolve_many(self, queries: List[str]) -> List[Optional[str]]:
        """
        Resolve multiple column queries.
        
        Args:
            queries: List of column queries
            
        Returns:
            List of resolved column names (None for unresolved)
        """
        return [self.resolve(q) for q in queries]
    
    def suggest(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get suggestions for a column query.
        
        Args:
            query: Column query
            limit: Maximum number of suggestions
            
        Returns:
            List of suggestions with score information
        """
        if not query:
            return []
        
        query = str(query).strip()
        suggestions = []
        
        # 1. Exact match
        if query in self.columns:
            suggestions.append({
                "column": query,
                "score": 100,
                "type": "exact"
            })
        
        # 2. Case-insensitive
        query_lower = query.lower()
        if query_lower in self._name_lower:
            name = self._name_lower[query_lower]
            if name != query:
                suggestions.append({
                    "column": name,
                    "score": 95,
                    "type": "case_insensitive"
                })
        
        # 3. Numeric index
        if query.isdigit():
            idx = int(query)
            if 0 <= idx < self.column_count:
                suggestions.append({
                    "column": self.columns[idx],
                    "score": 90,
                    "type": "index",
                    "index": idx,
                    "excel_letter": self._index_to_excel_letter(idx)
                })
        
        # 4. Excel letter
        excel_idx = self._excel_letter_to_index(query)
        if excel_idx is not None and 0 <= excel_idx < self.column_count:
            suggestions.append({
                "column": self.columns[excel_idx],
                "score": 90,
                "type": "excel_letter",
                "index": excel_idx
            })
        
        # 5. Partial matches
        partial_matches = self._partial_match(query_lower)
        for col in partial_matches[:limit]:
            if not any(s["column"] == col for s in suggestions):
                suggestions.append({
                    "column": col,
                    "score": 80,
                    "type": "partial"
                })
        
        # 6. Fuzzy matches (if rapidfuzz)
        if RAPIDFUZZ_AVAILABLE:
            fuzzy_results = process.extract(
                query,
                self.columns,
                scorer=fuzz.WRatio,
                limit=limit
            )
            for result in fuzzy_results:
                col, score = result[0], result[1]
                if not any(s["column"] == col for s in suggestions):
                    suggestions.append({
                        "column": col,
                        "score": score,
                        "type": "fuzzy"
                    })
        
        # Sort by score and limit
        suggestions = sorted(suggestions, key=lambda x: x["score"], reverse=True)[:limit]
        
        return suggestions
    
    def is_valid(self, query: str) -> bool:
        """Check if a query resolves to a valid column."""
        return self.resolve(query) is not None
    
    def get_column_info(self, column: str) -> Dict[str, Any]:
        """Get detailed information about a column."""
        resolved = self.resolve(column)
        if not resolved:
            return {"valid": False}
        
        col_idx = self._name_to_index.get(resolved)
        col_data = self.df[resolved]
        
        return {
            "valid": True,
            "resolved_name": resolved,
            "original_query": column,
            "index": col_idx,
            "excel_letter": self._index_to_excel_letter(col_idx) if col_idx is not None else None,
            "dtype": str(col_data.dtype),
            "null_count": int(col_data.isna().sum()),
            "unique_count": int(col_data.nunique()),
            "sample_values": col_data.dropna().head(5).tolist()
        }
    
    def get_all_columns(self) -> List[Dict[str, Any]]:
        """Get information about all columns."""
        return [
            {
                "name": col,
                "index": i,
                "excel_letter": self._index_to_excel_letter(i),
                "dtype": str(self.df[col].dtype)
            }
            for i, col in enumerate(self.columns)
        ]


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def resolve_column(query: str, df: pd.DataFrame) -> Optional[str]:
    """
    Convenience function to resolve a single column query.
    
    Args:
        query: Column name, Excel letter, or index
        df: DataFrame to resolve against
        
    Returns:
        Resolved column name or None
    """
    resolver = ColumnResolver(df)
    return resolver.resolve(query)


def resolve_columns(queries: List[str], df: pd.DataFrame) -> List[Optional[str]]:
    """
    Convenience function to resolve multiple column queries.
    
    Args:
        queries: List of column queries
        df: DataFrame to resolve against
        
    Returns:
        List of resolved column names
    """
    resolver = ColumnResolver(df)
    return resolver.resolve_many(queries)


def suggest_columns(query: str, df: pd.DataFrame, limit: int = 5) -> List[Dict[str, Any]]:
    """
    Convenience function to get column suggestions.
    
    Args:
        query: Column query
        df: DataFrame to resolve against
        limit: Maximum suggestions
        
    Returns:
        List of suggestions with scores
    """
    resolver = ColumnResolver(df)
    return resolver.suggest(query, limit)
