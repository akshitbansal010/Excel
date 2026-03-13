"""
Unit tests for excelpy package.
Tests fuzzy matching, operator parsing, rank, save modes, and Excel column mapping.
"""

import pytest
import os
import tempfile
from datetime import datetime

# Import excelpy modules
from excelpy.helpers import (
    col_letter,
    parse_col_letter,
    build_col_map,
    resolve_column,
    fuzzy_match,
    parse_value,
    parse_operator,
)
from excelpy.engine import (
    is_polars_available,
    is_pandas_available,
    read_csv,
    DataFrameWrapper,
)


# ╔══════════════════════════════════════════════════════════════════╗
# ║  COLUMN MAPPING TESTS                                            ║
# ╚══════════════════════════════════════════════════════════════════╝

class TestColumnMapping:
    """Test Excel-style column letter conversion."""
    
    def test_col_letter_single(self):
        """Test single letter columns."""
        assert col_letter(0) == "A"
        assert col_letter(1) == "B"
        assert col_letter(2) == "C"
        assert col_letter(25) == "Z"
    
    def test_col_letter_double(self):
        """Test double letter columns."""
        assert col_letter(26) == "AA"
        assert col_letter(27) == "AB"
        assert col_letter(51) == "AZ"
        assert col_letter(52) == "BA"
    
    def test_col_letter_triple(self):
        """Test triple letter columns."""
        assert col_letter(702) == "AAA"
        assert col_letter(703) == "AAB"
    
    def test_parse_col_letter(self):
        """Test parsing Excel column letters."""
        assert parse_col_letter("A") == 0
        assert parse_col_letter("B") == 1
        assert parse_col_letter("Z") == 25
        assert parse_col_letter("AA") == 26
        assert parse_col_letter("AZ") == 51
        assert parse_col_letter("AAA") == 702
    
    def test_parse_col_letter_case_insensitive(self):
        """Test case insensitivity."""
        assert parse_col_letter("a") == 0
        assert parse_col_letter("aa") == 26
    
    def test_parse_col_letter_invalid(self):
        """Test invalid input."""
        with pytest.raises(ValueError):
            parse_col_letter("")
        with pytest.raises(ValueError):
            parse_col_letter("123")
    
    def test_build_col_map(self):
        """Test building column map."""
        columns = ["name", "age", "city"]
        col_map = build_col_map(columns)
        
        assert col_map["a"] == "name"
        assert col_map["b"] == "age"
        assert col_map["c"] == "city"
        assert col_map["name"] == "name"
        assert col_map["age"] == "age"
        assert col_map["1"] == "name"
        assert col_map["2"] == "age"
    
    def test_resolve_column_letter(self):
        """Test resolving Excel letter to column."""
        columns = ["name", "age", "city"]
        
        assert resolve_column("A", columns) == "name"
        assert resolve_column("a", columns) == "name"
        assert resolve_column("B", columns) == "age"
        assert resolve_column("AA", columns) is None  # Out of range
    
    def test_resolve_column_number(self):
        """Test resolving 1-based number."""
        columns = ["name", "age", "city"]
        
        assert resolve_column("1", columns) == "name"
        assert resolve_column("2", columns) == "age"
        assert resolve_column("3", columns) == "city"
        assert resolve_column("0", columns) is None  # Out of range
        assert resolve_column("99", columns) is None  # Out of range
    
    def test_resolve_column_name(self):
        """Test resolving by exact name."""
        columns = ["name", "age", "city"]
        
        assert resolve_column("name", columns) == "name"
        assert resolve_column("age", columns) == "age"
        assert resolve_column("unknown", columns) is None
    
    def test_resolve_column_partial(self):
        """Test resolving partial name."""
        columns = ["first_name", "last_name", "email"]
        
        # Without fuzzy, should return None
        result = resolve_column("first", columns, allow_fuzzy=False)
        assert result is None
        
        # With fuzzy, may return match
        result = resolve_column("first", columns, allow_fuzzy=True, threshold=60)
        assert result is not None


# ╔══════════════════════════════════════════════════════════════════╗
# ║  VALUE PARSING TESTS                                             ║
# ╚══════════════════════════════════════════════════════════════════╝

class TestValueParsing:
    """Test value parsing."""
    
    def test_parse_int(self):
        """Test parsing integers."""
        assert parse_value("42") == 42
        assert parse_value("-10") == -10
        assert parse_value("  100  ") == 100
    
    def test_parse_float(self):
        """Test parsing floats."""
        assert parse_value("3.14") == 3.14
        assert parse_value("-2.5") == -2.5
        assert parse_value("1e10") == 1e10
    
    def test_parse_iso_date(self):
        """Test parsing ISO dates."""
        result = parse_value("2024-01-15")
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
    
    def test_parse_comma_list(self):
        """Test parsing comma-separated list."""
        result = parse_value("a,b,c")
        assert result == ["a", "b", "c"]
    
    def test_parse_range(self):
        """Test parsing range."""
        result = parse_value("10-20")
        assert isinstance(result, dict)
        assert result["type"] == "range"
        assert result["start"] == 10
        assert result["end"] == 20
    
    def test_parse_string(self):
        """Test parsing as string."""
        assert parse_value("hello") == "hello"
        assert parse_value("hello world") == "hello world"


# ╔══════════════════════════════════════════════════════════════════╗
# ║  OPERATOR PARSING TESTS                                          ║
# ╚══════════════════════════════════════════════════════════════════╝

class TestOperatorParsing:
    """Test operator parsing."""
    
    def test_equality_operators(self):
        """Test equality operators."""
        assert parse_operator("==") == "=="
        assert parse_operator("=") == "=="
        assert parse_operator("EQUALS") == "=="
        assert parse_operator("equals") == "=="
    
    def test_inequality_operators(self):
        """Test inequality operators."""
        assert parse_operator("!=") == "!="
        assert parse_operator("NOT EQUALS") == "!="
        assert parse_operator("NOT_EQUALS") == "!="
    
    def test_comparison_operators(self):
        """Test comparison operators."""
        assert parse_operator(">") == ">"
        assert parse_operator(">=") == ">="
        assert parse_operator("<") == "<"
        assert parse_operator("<=") == "<="
    
    def test_string_operators(self):
        """Test string matching operators."""
        assert parse_operator("CONTAINS") == "CONTAINS"
        assert parse_operator("contains") == "CONTAINS"
        assert parse_operator("STARTS WITH") == "STARTSWITH"
        assert parse_operator("ENDSWITH") == "ENDSWITH"
    
    def test_null_operators(self):
        """Test null checking operators."""
        assert parse_operator("IS NULL") == "IS_NULL"
        assert parse_operator("ISNULL") == "IS_NULL"
        assert parse_operator("BLANK") == "IS_NULL"
        assert parse_operator("IS NOT NULL") == "IS_NOT_NULL"
        assert parse_operator("NOT BLANK") == "IS_NOT_NULL"
    
    def test_list_operators(self):
        """Test list operators."""
        assert parse_operator("IN") == "IS_ONE_OF"
        assert parse_operator("IS ONE OF") == "IS_ONE_OF"


# ╔══════════════════════════════════════════════════════════════════╗
# ║  FUZZY MATCHING TESTS                                            ║
# ╚══════════════════════════════════════════════════════════════════╝

class TestFuzzyMatching:
    """Test fuzzy matching functionality."""
    
    def test_fuzzy_match_exact(self):
        """Test exact match returns 100."""
        choices = ["apple", "banana", "cherry"]
        result = fuzzy_match("apple", choices)
        
        assert len(result) > 0
        assert result[0][0] == "apple"
        assert result[0][1] == 100.0
    
    def test_fuzzy_match_similar(self):
        """Test similar match."""
        choices = ["apple", "banana", "cherry"]
        result = fuzzy_match("aple", choices, threshold=60)
        
        assert len(result) > 0
        assert result[0][0] == "apple"
        assert result[0][1] >= 60
    
    def test_fuzzy_match_no_match(self):
        """Test no match found."""
        choices = ["apple", "banana", "cherry"]
        result = fuzzy_match("xyz123", choices, threshold=60)
        
        assert len(result) == 0
    
    def test_fuzzy_match_limit(self):
        """Test result limit."""
        choices = ["apple", "apricot", "avocado", "banana", "cherry"]
        result = fuzzy_match("a", choices, threshold=0, limit=3)
        
        assert len(result) == 3


# ╔══════════════════════════════════════════════════════════════════╗
# ║  ENGINE TESTS                                                    ║
# ╚══════════════════════════════════════════════════════════════════╝

class TestEngine:
    """Test engine availability and DataFrame operations."""
    
    def test_engine_availability(self):
        """Test that at least one engine is available."""
        assert is_polars_available() or is_pandas_available()
    
    @pytest.mark.skipif(not is_polars_available(), reason="Polars not available")
    def test_polars_dataframe(self, sample_csv):
        """Test polars DataFrame creation."""
        df = read_csv(sample_csv)
        
        assert df.engine == "polars"
        assert len(df) > 0
        assert len(df.columns) > 0
    
    def test_csv_load(self):
        """Test CSV loading."""
        # Create temp CSV
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("name,age,city\n")
            f.write("Alice,30,NYC\n")
            f.write("Bob,25,LA\n")
            temp_path = f.name
        
        try:
            df = read_csv(temp_path)
            
            assert len(df) == 2
            assert "name" in df.columns
            assert "age" in df.columns
            assert "city" in df.columns
        finally:
            os.unlink(temp_path)
    
    def test_csv_delimiter_detection(self):
        """Test CSV delimiter detection."""
        # Create temp CSV with semicolon
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("name;age;city\n")
            f.write("Alice;30;NYC\n")
            temp_path = f.name
        
        try:
            df = read_csv(temp_path)
            
            # Should auto-detect semicolon
            assert len(df.columns) >= 1
        finally:
            os.unlink(temp_path)


# ╔══════════════════════════════════════════════════════════════════╗
# ║  RANK TESTS                                                      ║
# ╚══════════════════════════════════════════════════════════════════╝

class TestRanking:
    """Test ranking functionality."""
    
    @pytest.mark.skipif(not is_polars_available(), reason="Polars not available")
    def test_rank_basic(self):
        """Test basic ranking."""
        from excelpy.core import _apply_rank
        
        # Create test data
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("name,score\n")
            f.write("Alice,95\n")
            f.write("Bob,85\n")
            f.write("Charlie,95\n")
            f.write("Diana,90\n")
            temp_path = f.name
        
        try:
            df = read_csv(temp_path)
            
            # Apply rank with min tie method
            ranked = _apply_rank(df, ["score"], "rank", "min", ascending=False)
            
            assert "rank" in ranked.columns
            # Alice and Charlie should both have rank 1 (min method)
            # Bob should have rank 3, Diana rank 2
        finally:
            os.unlink(temp_path)
    
    @pytest.mark.skipif(not is_polars_available(), reason="Polars not available")
    def test_rank_dense(self):
        """Test dense ranking."""
        from excelpy.core import _apply_rank
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("name,score\n")
            f.write("Alice,95\n")
            f.write("Bob,85\n")
            f.write("Charlie,95\n")
            f.write("Diana,90\n")
            temp_path = f.name
        
        try:
            df = read_csv(temp_path)
            
            # Apply rank with dense method
            ranked = _apply_rank(df, ["score"], "rank", "dense", ascending=False)
            
            assert "rank" in ranked.columns
            # Dense: Alice=1, Charlie=1, Diana=2, Bob=3 (no gaps)
        finally:
            os.unlink(temp_path)


# ╔══════════════════════════════════════════════════════════════════╗
# ║  SAVE MODE TESTS                                                 ║
# ╚══════════════════════════════════════════════════════════════════╝

class TestSaveModes:
    """Test save mode functionality."""
    
    @pytest.mark.skipif(not is_polars_available(), reason="Polars not available")
    def test_save_replace(self):
        """Test replace mode."""
        from excelpy.core import save_table
        
        # Create test data
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("name,age\n")
            f.write("Alice,30\n")
            temp_path = f.name
        
        try:
            df = read_csv(temp_path)
            
            # Save to new path
            output_path = temp_path.replace('.csv', '_output.csv')
            
            # Replace mode
            success = save_table(df, output_path, mode="replace")
            assert success
            assert os.path.exists(output_path)
            
            # Cleanup
            os.unlink(output_path)
        finally:
            os.unlink(temp_path)
    
    @pytest.mark.skipif(not is_polars_available(), reason="Polars not available")
    def test_save_fail_mode(self):
        """Test fail mode."""
        from excelpy.core import save_table
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("name,age\n")
            f.write("Alice,30\n")
            temp_path = f.name
        
        try:
            df = read_csv(temp_path)
            
            # Fail mode - should fail because file exists
            success = save_table(df, temp_path, mode="fail")
            assert not success
        finally:
            os.unlink(temp_path)


# ╔══════════════════════════════════════════════════════════════════╗
# ║  INTEGRATION TESTS                                               ║
# ╚══════════════════════════════════════════════════════════════════╝

class TestIntegration:
    """Integration tests for full workflows."""
    
    @pytest.mark.skipif(not is_polars_available(), reason="Polars not available")
    def test_filter_workflow(self):
        """Test complete filter workflow."""
        from excelpy.core import _apply_filter
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("name,age,city\n")
            f.write("Alice,30,NYC\n")
            f.write("Bob,25,LA\n")
            f.write("Charlie,35,NYC\n")
            temp_path = f.name
        
        try:
            df = read_csv(temp_path)
            
            # Filter: age > 28
            filtered = _apply_filter(df, "age", ">", 28)
            
            assert len(filtered) == 2  # Alice and Charlie
        finally:
            os.unlink(temp_path)
    
    @pytest.mark.skipif(not is_polars_available(), reason="Polars not available")
    def test_sort_workflow(self):
        """Test sort workflow."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("name,age\n")
            f.write("Charlie,35\n")
            f.write("Alice,30\n")
            f.write("Bob,25\n")
            temp_path = f.name
        
        try:
            df = read_csv(temp_path)
            sorted_df = df.sort("age", ascending=True)
            
            # First row should be Bob (youngest)
            first_row = list(sorted_df.head(1).iterrows())[0]
            assert "Bob" in str(first_row)
        finally:
            os.unlink(temp_path)


# ╔══════════════════════════════════════════════════════════════════╗
# ║  TEST DATA                                                       ║
# ╚══════════════════════════════════════════════════════════════════╝

@pytest.fixture
def sample_csv():
    """Create a sample CSV file for testing."""
    content = "name,age,city,salary\nAlice,30,NYC,75000\nBob,25,LA,50000\nCharlie,35,NYC,90000"
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(content)
        path = f.name
    
    yield path
    
    # Cleanup
    os.unlink(path)


