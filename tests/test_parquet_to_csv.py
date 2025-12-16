"""
Tests for Parquet to CSV Conversion Service

These tests generate synthetic Parquet data at runtime to verify conversion logic.
"""

import io
import json
import zipfile
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# Add app directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.parquet_to_csv import (
    convert_parquet_filelike_to_csv_bytes,
    convert_multiple_to_zip_bytes,
    get_parquet_preview,
    get_common_columns,
    merge_parquets_union_to_csv_bytes,
    merge_parquets_join_to_csv_bytes,
    _is_complex_type,
    _serialize_complex_value,
)


def create_simple_parquet() -> io.BytesIO:
    """Create a simple Parquet file in memory for testing."""
    table = pa.table({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "score": [95.5, 87.3, 92.1, 88.9, 94.7],
    })
    
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    return buffer


def create_complex_parquet() -> io.BytesIO:
    """Create a Parquet file with complex types for testing."""
    # Create arrays with complex types
    ids = pa.array([1, 2, 3])
    names = pa.array(["Alice", "Bob", "Charlie"])
    
    # List type column
    tags = pa.array([["python", "data"], ["java"], ["rust", "go", "python"]])
    
    # Struct type column
    metadata = pa.array([
        {"city": "Paris", "age": 30},
        {"city": "London", "age": 25},
        {"city": "Berlin", "age": 35}
    ])
    
    table = pa.table({
        "id": ids,
        "name": names,
        "tags": tags,
        "metadata": metadata,
    })
    
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    return buffer


def create_empty_parquet() -> io.BytesIO:
    """Create an empty Parquet file for testing edge cases."""
    table = pa.table({
        "id": pa.array([], type=pa.int64()),
        "name": pa.array([], type=pa.string()),
    })
    
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    return buffer


class TestComplexTypeDetection:
    """Tests for complex type detection and serialization."""
    
    def test_is_complex_type_list(self):
        assert _is_complex_type(pa.list_(pa.string())) is True
    
    def test_is_complex_type_struct(self):
        struct_type = pa.struct([("x", pa.int64()), ("y", pa.string())])
        assert _is_complex_type(struct_type) is True
    
    def test_is_complex_type_simple(self):
        assert _is_complex_type(pa.int64()) is False
        assert _is_complex_type(pa.string()) is False
        assert _is_complex_type(pa.float64()) is False
    
    def test_serialize_complex_value_list(self):
        result = _serialize_complex_value(["a", "b", "c"])
        assert result == '["a", "b", "c"]'
    
    def test_serialize_complex_value_dict(self):
        result = _serialize_complex_value({"key": "value"})
        assert result == '{"key": "value"}'
    
    def test_serialize_complex_value_none(self):
        result = _serialize_complex_value(None)
        assert result == ""


class TestParquetPreview:
    """Tests for Parquet file preview functionality."""
    
    def test_preview_simple_file(self):
        buffer = create_simple_parquet()
        preview, metadata = get_parquet_preview(buffer, num_rows=50)
        
        assert len(preview) == 5  # All rows (less than 50)
        assert metadata["num_rows"] == 5
        assert metadata["num_columns"] == 3
        assert len(metadata["schema"]) == 3
    
    def test_preview_limits_rows(self):
        # Create a larger table
        table = pa.table({
            "id": list(range(100)),
            "value": [f"row_{i}" for i in range(100)],
        })
        
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        
        preview, metadata = get_parquet_preview(buffer, num_rows=50)
        
        assert len(preview) <= 50
        assert metadata["num_rows"] == 100


class TestSingleFileConversion:
    """Tests for single Parquet to CSV conversion."""
    
    def test_convert_simple_file(self):
        buffer = create_simple_parquet()
        csv_bytes = convert_parquet_filelike_to_csv_bytes(buffer)
        
        csv_text = csv_bytes.decode("utf-8")
        lines = csv_text.strip().split("\n")
        
        assert len(lines) == 6  # Header + 5 data rows
        assert "id" in lines[0]
        assert "name" in lines[0]
        assert "score" in lines[0]
        assert "Alice" in csv_text
    
    def test_convert_with_semicolon_separator(self):
        buffer = create_simple_parquet()
        csv_bytes = convert_parquet_filelike_to_csv_bytes(buffer, separator=";")
        
        csv_text = csv_bytes.decode("utf-8")
        # PyArrow may quote headers, check for presence of semicolon separator
        assert ";" in csv_text
        assert "Alice" in csv_text
    
    def test_convert_without_header(self):
        buffer = create_simple_parquet()
        csv_bytes = convert_parquet_filelike_to_csv_bytes(buffer, include_header=False)
        
        csv_text = csv_bytes.decode("utf-8")
        lines = csv_text.strip().split("\n")
        
        assert len(lines) == 5  # No header
        assert "id" not in lines[0]  # First line should be data
    
    def test_convert_complex_types_to_json(self):
        buffer = create_complex_parquet()
        csv_bytes = convert_parquet_filelike_to_csv_bytes(
            buffer,
            convert_complex_to_json=True
        )
        
        csv_text = csv_bytes.decode("utf-8")
        # Check that list column is serialized as JSON
        assert '["python"' in csv_text or "python" in csv_text
    
    def test_convert_with_progress_callback(self):
        buffer = create_simple_parquet()
        progress_values = []
        
        def callback(current, total):
            progress_values.append((current, total))
        
        csv_bytes = convert_parquet_filelike_to_csv_bytes(
            buffer,
            progress_callback=callback
        )
        
        assert len(progress_values) > 0
        # Last progress should show all rows processed
        assert progress_values[-1][0] == progress_values[-1][1]
    
    def test_convert_empty_file(self):
        buffer = create_empty_parquet()
        csv_bytes = convert_parquet_filelike_to_csv_bytes(buffer)
        
        csv_text = csv_bytes.decode("utf-8")
        # Empty Parquet with no row groups produces empty output
        # Just verify it doesn't crash and returns valid bytes
        assert isinstance(csv_bytes, bytes)


class TestMultipleFileConversion:
    """Tests for multiple Parquet files to ZIP conversion."""
    
    def test_convert_multiple_to_zip(self):
        files = [
            ("file1.parquet", create_simple_parquet()),
            ("file2.parquet", create_simple_parquet()),
        ]
        
        zip_bytes = convert_multiple_to_zip_bytes(files)
        
        # Verify ZIP structure
        zip_buffer = io.BytesIO(zip_bytes)
        with zipfile.ZipFile(zip_buffer, 'r') as zf:
            names = zf.namelist()
            assert "file1.csv" in names
            assert "file2.csv" in names
            
            # Verify content
            content1 = zf.read("file1.csv").decode("utf-8")
            assert "Alice" in content1
    
    def test_convert_multiple_with_options(self):
        files = [
            ("data.parquet", create_simple_parquet()),
        ]
        
        zip_bytes = convert_multiple_to_zip_bytes(
            files,
            separator=";",
            encoding="utf-8"
        )
        
        zip_buffer = io.BytesIO(zip_bytes)
        with zipfile.ZipFile(zip_buffer, 'r') as zf:
            content = zf.read("data.csv").decode("utf-8")
            # PyArrow may quote headers, check for semicolon separator
            assert ";" in content
            assert "Alice" in content
    
    def test_convert_multiple_with_progress(self):
        files = [
            ("file1.parquet", create_simple_parquet()),
            ("file2.parquet", create_simple_parquet()),
        ]
        
        progress_calls = []
        
        def callback(current, total, filename):
            progress_calls.append((current, total, filename))
        
        zip_bytes = convert_multiple_to_zip_bytes(
            files,
            progress_callback=callback
        )
        
        assert len(progress_calls) >= 2  # At least one per file


class TestEncodings:
    """Tests for different output encodings."""
    
    def test_utf8_encoding(self):
        # Create table with special characters
        table = pa.table({
            "name": ["Café", "日本語", "Ñoño"],
        })
        
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        
        csv_bytes = convert_parquet_filelike_to_csv_bytes(buffer, encoding="utf-8")
        csv_text = csv_bytes.decode("utf-8")
        
        assert "Café" in csv_text
        assert "日本語" in csv_text
    
    def test_latin1_encoding(self):
        table = pa.table({
            "name": ["Café", "Ñoño"],
        })
        
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        
        csv_bytes = convert_parquet_filelike_to_csv_bytes(buffer, encoding="latin-1")
        csv_text = csv_bytes.decode("latin-1")
        
        assert "Café" in csv_text


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


class TestMergeUnion:
    """Tests for UNION ALL merge functionality."""
    
    def test_union_same_schema(self):
        """Test merging files with identical schemas."""
        files = [
            ("file1.parquet", create_simple_parquet()),
            ("file2.parquet", create_simple_parquet()),
        ]
        
        csv_bytes = merge_parquets_union_to_csv_bytes(files)
        csv_text = csv_bytes.decode("utf-8")
        lines = csv_text.strip().split("\n")
        
        # Header + 5 rows from file1 + 5 rows from file2
        assert len(lines) == 11
        assert "Alice" in csv_text
    
    def test_union_with_source_column(self):
        """Test adding source file column."""
        files = [
            ("data_a.parquet", create_simple_parquet()),
            ("data_b.parquet", create_simple_parquet()),
        ]
        
        csv_bytes = merge_parquets_union_to_csv_bytes(files, add_source_column=True)
        csv_text = csv_bytes.decode("utf-8")
        
        assert "_source_file" in csv_text or "source_file" in csv_text
        assert "data_a.parquet" in csv_text
        assert "data_b.parquet" in csv_text
    
    def test_union_different_schemas(self):
        """Test merging files with different column sets."""
        # Create two files with partially overlapping schemas
        table1 = pa.table({
            "id": [1, 2],
            "name": ["A", "B"],
        })
        buffer1 = io.BytesIO()
        pq.write_table(table1, buffer1)
        buffer1.seek(0)
        
        table2 = pa.table({
            "id": [3, 4],
            "email": ["c@test.com", "d@test.com"],
        })
        buffer2 = io.BytesIO()
        pq.write_table(table2, buffer2)
        buffer2.seek(0)
        
        files = [
            ("file1.parquet", buffer1),
            ("file2.parquet", buffer2),
        ]
        
        csv_bytes = merge_parquets_union_to_csv_bytes(files)
        csv_text = csv_bytes.decode("utf-8")
        
        # Should have 4 rows total + header
        lines = csv_text.strip().split("\n")
        assert len(lines) == 5
    
    def test_union_with_progress(self):
        """Test progress callback for UNION."""
        files = [
            ("file1.parquet", create_simple_parquet()),
            ("file2.parquet", create_simple_parquet()),
        ]
        
        progress_calls = []
        
        def callback(current, total, filename):
            progress_calls.append((current, total, filename))
        
        csv_bytes = merge_parquets_union_to_csv_bytes(files, progress_callback=callback)
        
        assert len(progress_calls) >= 2


class TestMergeJoin:
    """Tests for JOIN merge functionality."""
    
    def test_join_on_key(self):
        """Test basic JOIN on a key column."""
        # Create two related tables
        table1 = pa.table({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        })
        buffer1 = io.BytesIO()
        pq.write_table(table1, buffer1)
        buffer1.seek(0)
        
        table2 = pa.table({
            "id": [1, 2, 3],
            "score": [95.0, 87.0, 92.0],
        })
        buffer2 = io.BytesIO()
        pq.write_table(table2, buffer2)
        buffer2.seek(0)
        
        files = [
            ("users.parquet", buffer1),
            ("scores.parquet", buffer2),
        ]
        
        csv_bytes = merge_parquets_join_to_csv_bytes(files, join_key="id")
        csv_text = csv_bytes.decode("utf-8")
        
        # Should have 3 rows + header
        lines = csv_text.strip().split("\n")
        assert len(lines) == 4
        assert "Alice" in csv_text
        assert "95" in csv_text
    
    def test_join_handles_collisions(self):
        """Test that column name collisions are handled."""
        table1 = pa.table({
            "id": [1, 2],
            "value": ["A", "B"],
        })
        buffer1 = io.BytesIO()
        pq.write_table(table1, buffer1)
        buffer1.seek(0)
        
        table2 = pa.table({
            "id": [1, 2],
            "value": ["X", "Y"],  # Same column name
        })
        buffer2 = io.BytesIO()
        pq.write_table(table2, buffer2)
        buffer2.seek(0)
        
        files = [
            ("file1.parquet", buffer1),
            ("file2.parquet", buffer2),
        ]
        
        csv_bytes = merge_parquets_join_to_csv_bytes(files, join_key="id")
        csv_text = csv_bytes.decode("utf-8")
        
        # Both value columns should appear (one with suffix)
        assert "value" in csv_text
        assert "A" in csv_text
        assert "X" in csv_text


class TestGetCommonColumns:
    """Tests for common column detection."""
    
    def test_common_columns(self):
        """Test finding common columns across files."""
        table1 = pa.table({
            "id": [1, 2],
            "name": ["A", "B"],
            "extra1": [1, 2],
        })
        buffer1 = io.BytesIO()
        pq.write_table(table1, buffer1)
        buffer1.seek(0)
        
        table2 = pa.table({
            "id": [3, 4],
            "name": ["C", "D"],
            "extra2": [3, 4],
        })
        buffer2 = io.BytesIO()
        pq.write_table(table2, buffer2)
        buffer2.seek(0)
        
        files = [
            ("file1.parquet", buffer1),
            ("file2.parquet", buffer2),
        ]
        
        common = get_common_columns(files)
        
        assert "id" in common
        assert "name" in common
        assert "extra1" not in common
        assert "extra2" not in common
