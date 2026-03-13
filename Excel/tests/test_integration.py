"""
DataEngine Pro - Integration Tests
===================================

Unit tests for the integration layer (adapter, session, column_resolver).

Usage:
    pytest tests/test_integration.py -v
"""

import os
import sys
import pytest
import pandas as pd
import tempfile
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import integration modules
from integration.adapter import Adapter, get_adapter
from integration.column_resolver import ColumnResolver
from integration.session import SessionManager


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Edward"],
        "age": [25, 30, 35, 40, 45],
        "city": ["New York", "Boston", "Chicago", "Seattle", "Miami"],
        "salary": [50000, 60000, 70000, 80000, 90000]
    })


@pytest.fixture
def adapter():
    """Create adapter instance."""
    return Adapter()


@pytest.fixture
def session_id(adapter):
    """Create session and return ID."""
    session = adapter.create_session()
    return session.id


@pytest.fixture
def session_with_data(adapter, session_id, sample_df):
    """Create session with sample data."""
    # Manually add data to session
    if isinstance(adapter._sessions.get(session_id), dict):
        adapter._sessions[session_id]["tables"]["test"] = sample_df
        adapter._sessions[session_id]["active_table"] = "test"
    return session_id


# =============================================================================
# ADAPTER TESTS
# =============================================================================

class TestAdapter:
    """Tests for the Adapter class."""
    
    def test_create_session(self, adapter):
        """Test session creation."""
        session = adapter.create_session()
        assert session.id is not None
        assert session.tables == []
        assert session.active_table == ""
    
    def test_list_tables_empty(self, adapter, session_id):
        """Test listing tables when none exist."""
        tables = adapter.list_tables(session_id)
        assert tables == []
    
    def test_load_csv(self, adapter, session_id):
        """Test loading CSV file."""
        # Use sample data from fixtures
        csv_path = Path(__file__).parent / "fixtures" / "sample_data.csv"
        
        if csv_path.exists():
            result = adapter.load_table(
                session_id,
                {"type": "csv", "path": str(csv_path)}
            )
            assert len(result.tables) > 0
            assert result.row_count > 0
    
    def test_preview(self, adapter, session_with_data, sample_df):
        """Test data preview."""
        result = adapter.preview(session_with_data)
        assert len(result.rows) <= 200
        assert len(result.columns) > 0
        assert result.summary["total_rows"] == len(sample_df)
    
    def test_get_schema(self, adapter, session_with_data, sample_df):
        """Test schema retrieval."""
        schema = adapter.get_schema(session_with_data)
        
        assert schema["row_count"] == len(sample_df)
        assert len(schema["columns"]) == len(sample_df.columns)
        
        # Check column names
        col_names = [c["name"] for c in schema["columns"]]
        for col in sample_df.columns:
            assert col in col_names
    
    def test_filter_operation(self, adapter, session_with_data):
        """Test filter operation."""
        result = adapter.op_filter(
            session_with_data,
            column="age",
            operator=">",
            value=30,
            limit=10
        )
        
        assert result.op_id is not None
        assert result.status == "completed"
        assert result.summary["rows_after"] == 3  # ages > 30: 35, 40, 45
    
    def test_filter_equals(self, adapter, session_with_data):
        """Test filter with equals operator."""
        result = adapter.op_filter(
            session_with_data,
            column="name",
            operator="==",
            value="Bob",
            limit=10
        )
        
        assert result.summary["rows_after"] == 1
    
    def test_filter_contains(self, adapter, session_with_data):
        """Test filter with contains operator."""
        result = adapter.op_filter(
            session_with_data,
            column="city",
            operator="contains",
            value="o",
            limit=10
        )
        
        # Should match "Boston", "Chicago", "Miami"
        assert result.summary["rows_after"] == 3
    
    def test_sort_operation(self, adapter, session_with_data):
        """Test sort operation."""
        result = adapter.op_sort(
            session_with_data,
            columns=["age"],
            ascending=False,
            limit=10
        )
        
        # Should be sorted descending by age
        rows = result.preview.rows
        assert rows[0]["age"] >= rows[1]["age"]
    
    def test_aggregate_operation(self, adapter, session_with_data):
        """Test aggregation."""
        result = adapter.op_aggregate(
            session_with_data,
            group_by=["city"],
            aggs={"salary": "sum"},
            limit=10
        )
        
        assert result.status == "completed"
        assert len(result.preview.rows) > 0
    
    def test_rank_operation(self, adapter, session_with_data):
        """Test ranking."""
        result = adapter.op_rank(
            session_with_data,
            by="salary",
            method="dense",
            new_col="rank",
            limit=10
        )
        
        assert result.status == "completed"
        # Check that rank column was added
        preview = adapter.preview(session_with_data)
        assert "rank" in preview.columns
    
    def test_sql_operation(self, adapter, session_with_data):
        """Test SQL query."""
        result = adapter.op_sql(
            session_with_data,
            "age > 30",
            limit=10
        )
        
        assert result.status == "completed"
        assert result.summary["rows_after"] == 2  # ages 35, 40
    
    def test_undo(self, adapter, session_with_data, sample_df):
        """Test undo operation."""
        # First, apply a filter
        adapter.op_filter(
            session_with_data,
            column="age",
            operator=">",
            value=30
        )
        
        # Verify data was filtered
        df = adapter.get_dataframe(session_with_data)
        assert len(df) == 3  # Only ages > 30
        
        # Undo
        success = adapter.undo(session_with_data)
        assert success
        
        # Verify data was restored
        df = adapter.get_dataframe(session_with_data)
        assert len(df) == len(sample_df)
    
    def test_diff(self, adapter, session_with_data):
        """Test diff functionality."""
        # Apply an operation
        adapter.op_filter(
            session_with_data,
            column="age",
            operator=">",
            value=30
        )
        
        # Get diff
        diff = adapter.get_diff(session_with_data)
        
        assert "before" in diff
        assert "after" in diff
        assert "summary" in diff
    
    def test_save_csv(self, adapter, session_with_data):
        """Test saving to CSV."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            result = adapter.save(
                session_with_data,
                dest={"type": "csv", "path": tmp_path}
            )
            
            assert result["success"] is True
            assert os.path.exists(tmp_path)
            
            # Verify content
            df = pd.read_csv(tmp_path)
            assert len(df) == 5
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)


# =============================================================================
# COLUMN RESOLVER TESTS
# =============================================================================

class TestColumnResolver:
    """Tests for the ColumnResolver class."""
    
    def test_exact_match(self, sample_df):
        """Test exact column name matching."""
        resolver = ColumnResolver(sample_df)
        
        assert resolver.resolve("name") == "name"
        assert resolver.resolve("age") == "age"
        assert resolver.resolve("salary") == "salary"
    
    def test_case_insensitive(self, sample_df):
        """Test case-insensitive matching."""
        resolver = ColumnResolver(sample_df)
        
        assert resolver.resolve("NAME") == "name"
        assert resolver.resolve("Age") == "age"
        assert resolver.resolve("SALARY") == "salary"
    
    def test_numeric_index(self, sample_df):
        """Test numeric index resolution."""
        resolver = ColumnResolver(sample_df)
        
        assert resolver.resolve("0") == "id"
        assert resolver.resolve("1") == "name"
        assert resolver.resolve("4") == "salary"
    
    def test_excel_letter(self, sample_df):
        """Test Excel letter notation."""
        resolver = ColumnResolver(sample_df)
        
        assert resolver.resolve("A") == "id"
        assert resolver.resolve("B") == "name"
        assert resolver.resolve("E") == "salary"
    
    def test_partial_match(self, sample_df):
        """Test partial matching."""
        resolver = ColumnResolver(sample_df)
        
        # Should match exactly if unique
        assert resolver.resolve("nam") == "name"
        assert resolver.resolve("ag") == "age"
    
    def test_invalid_column(self, sample_df):
        """Test invalid column returns None."""
        resolver = ColumnResolver(sample_df)
        
        assert resolver.resolve("nonexistent") is None
        assert resolver.resolve("xyz") is None
    
    def test_suggestions(self, sample_df):
        """Test column suggestions."""
        resolver = ColumnResolver(sample_df)
        
        suggestions = resolver.suggest("nam")
        assert len(suggestions) > 0
        assert any(s["column"] == "name" for s in suggestions)
    
    def test_column_info(self, sample_df):
        """Test column info retrieval."""
        resolver = ColumnResolver(sample_df)
        
        info = resolver.resolve("name")
        assert info == "name"
        
        # Get detailed info
        detailed = resolver.get_column_info("name")
        assert detailed["valid"] is True
        assert detailed["resolved_name"] == "name"
        assert detailed["index"] == 1
    
    def test_all_columns(self, sample_df):
        """Test getting all columns."""
        resolver = ColumnResolver(sample_df)
        
        all_cols = resolver.get_all_columns()
        assert len(all_cols) == 5
        assert all_cols[0]["name"] == "id"
        assert all_cols[0]["index"] == 0


# =============================================================================
# SESSION MANAGER TESTS
# =============================================================================

class TestSessionManager:
    """Tests for the SessionManager class."""
    
    def test_create_session(self):
        """Test session creation."""
        manager = SessionManager()
        
        session_id = manager.create_session()
        assert session_id is not None
        
        session = manager.get_session(session_id)
        assert session is not None
    
    def test_add_table(self):
        """Test adding table to session."""
        manager = SessionManager()
        session_id = manager.create_session()
        
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        
        success = manager.add_table(session_id, "test_table", df)
        assert success is True
        
        tables = manager.list_tables(session_id)
        assert "test_table" in tables
    
    def test_get_table(self):
        """Test retrieving table."""
        manager = SessionManager()
        session_id = manager.create_session()
        
        df = pd.DataFrame({"a": [1, 2, 3]})
        manager.add_table(session_id, "test", df)
        
        retrieved = manager.get_table(session_id, "test")
        assert retrieved is not None
        assert len(retrieved) == 3
    
    def test_snapshot_and_undo(self):
        """Test snapshot and undo."""
        manager = SessionManager(max_undo=5)
        session_id = manager.create_session()
        
        # Add table
        df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
        manager.add_table(session_id, "test", df)
        
        # Save snapshot
        snapshot_id = manager.save_snapshot(session_id, "test")
        assert snapshot_id is not None
        
        # Modify table
        modified_df = df[df["a"] > 2]
        manager.add_table(session_id, "test", modified_df)
        
        # Verify modified
        assert len(manager.get_table(session_id, "test")) == 3
        
        # Undo
        success = manager.undo(session_id, "test")
        assert success
        
        # Verify restored
        restored = manager.get_table(session_id, "test")
        assert len(restored) == 5
    
    def test_can_undo(self):
        """Test can_undo check."""
        manager = SessionManager()
        session_id = manager.create_session()
        
        df = pd.DataFrame({"a": [1, 2, 3]})
        manager.add_table(session_id, "test", df)
        
        # Initially no undo available
        assert manager.can_undo(session_id, "test") is False
        
        # Save snapshot
        manager.save_snapshot(session_id, "test")
        
        # Now undo should be available
        assert manager.can_undo(session_id, "test") is True


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
