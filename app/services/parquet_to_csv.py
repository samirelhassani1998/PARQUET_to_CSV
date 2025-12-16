"""
Parquet to CSV Conversion Service

This module provides streaming conversion from Parquet files to CSV format,
designed to handle large files efficiently without loading everything into memory.
"""

import io
import json
import logging
import tempfile
import zipfile
from typing import Any, BinaryIO, Dict, List, Optional, Tuple
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _serialize_complex_value(value: Any) -> str:
    """
    Serialize complex Arrow values (list, struct, map) to JSON string.
    
    Args:
        value: A Python value that may be a complex type
        
    Returns:
        JSON string representation of the value
    """
    if value is None:
        return ""
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return str(value)


def _is_complex_type(arrow_type: pa.DataType) -> bool:
    """
    Check if an Arrow type is complex (list, struct, map) and needs JSON serialization.
    
    Args:
        arrow_type: PyArrow data type
        
    Returns:
        True if the type is complex
    """
    return (
        pa.types.is_list(arrow_type) or
        pa.types.is_large_list(arrow_type) or
        pa.types.is_struct(arrow_type) or
        pa.types.is_map(arrow_type) or
        pa.types.is_nested(arrow_type)
    )


def _cast_complex_columns_to_string(table: pa.Table, convert_complex: bool = True) -> pa.Table:
    """
    Convert complex-typed columns to string (JSON) representation.
    
    Args:
        table: PyArrow table
        convert_complex: Whether to convert complex types
        
    Returns:
        Table with complex columns cast to strings
    """
    if not convert_complex:
        return table
    
    new_columns = []
    for i, field in enumerate(table.schema):
        column = table.column(i)
        if _is_complex_type(field.type):
            # Convert to Python, serialize to JSON, create new string array
            python_values = column.to_pylist()
            json_values = [_serialize_complex_value(v) for v in python_values]
            new_column = pa.array(json_values, type=pa.string())
            new_columns.append(new_column)
            logger.info(f"Converted complex column '{field.name}' to JSON string")
        else:
            new_columns.append(column)
    
    return pa.table(dict(zip(table.column_names, new_columns)))


def get_parquet_preview(
    file_like: BinaryIO,
    num_rows: int = 50
) -> Tuple[pa.Table, Dict[str, Any]]:
    """
    Read a preview of a Parquet file and return metadata.
    
    Args:
        file_like: File-like object containing Parquet data
        num_rows: Number of rows to preview
        
    Returns:
        Tuple of (preview table, metadata dict)
    """
    try:
        parquet_file = pq.ParquetFile(file_like)
        metadata = {
            "num_rows": parquet_file.metadata.num_rows,
            "num_columns": parquet_file.metadata.num_columns,
            "num_row_groups": parquet_file.metadata.num_row_groups,
            "schema": [
                {"name": field.name, "type": str(field.type)}
                for field in parquet_file.schema_arrow
            ]
        }
        
        # Read only first batch for preview
        preview_table = parquet_file.read_row_group(0).slice(0, num_rows)
        
        logger.info(f"Preview loaded: {metadata['num_rows']} total rows, showing {len(preview_table)} rows")
        return preview_table, metadata
        
    except Exception as e:
        logger.error(f"Error reading Parquet preview: {e}")
        raise ValueError(f"Cannot read Parquet file: {e}")


def convert_parquet_filelike_to_csv_bytes(
    file_like: BinaryIO,
    separator: str = ",",
    encoding: str = "utf-8",
    include_header: bool = True,
    convert_complex_to_json: bool = True,
    batch_size: int = 50000,
    progress_callback: Optional[callable] = None
) -> bytes:
    """
    Convert a Parquet file to CSV bytes using streaming to minimize memory usage.
    
    Args:
        file_like: File-like object containing Parquet data
        separator: CSV field separator (default: ',')
        encoding: Output encoding (default: 'utf-8')
        include_header: Include column headers in output
        convert_complex_to_json: Convert complex types to JSON strings
        batch_size: Number of rows per batch for streaming
        progress_callback: Optional callback(current, total) for progress updates
        
    Returns:
        CSV data as bytes
    """
    logger.info(f"Starting Parquet to CSV conversion (batch_size={batch_size})")
    
    try:
        parquet_file = pq.ParquetFile(file_like)
        total_rows = parquet_file.metadata.num_rows
        processed_rows = 0
        
        # Configure CSV write options
        write_options = pa_csv.WriteOptions(
            include_header=include_header,
            delimiter=separator
        )
        
        # Use a BytesIO buffer for output
        output_buffer = io.BytesIO()
        
        # Process in batches
        first_batch = True
        for batch in parquet_file.iter_batches(batch_size=batch_size):
            # Convert batch to table for processing
            batch_table = pa.Table.from_batches([batch])
            
            # Cast complex columns if needed
            batch_table = _cast_complex_columns_to_string(batch_table, convert_complex_to_json)
            
            # Write to temporary buffer
            temp_buffer = io.BytesIO()
            batch_write_options = pa_csv.WriteOptions(
                include_header=(first_batch and include_header),
                delimiter=separator
            )
            pa_csv.write_csv(batch_table, temp_buffer, write_options=batch_write_options)
            
            # Append to main buffer
            output_buffer.write(temp_buffer.getvalue())
            
            processed_rows += len(batch)
            first_batch = False
            
            if progress_callback:
                progress_callback(processed_rows, total_rows)
        
        logger.info(f"Conversion complete: {processed_rows} rows written")
        
        # Handle encoding if not UTF-8
        csv_bytes = output_buffer.getvalue()
        if encoding.lower() != "utf-8":
            csv_text = csv_bytes.decode("utf-8")
            csv_bytes = csv_text.encode(encoding, errors="replace")
        
        return csv_bytes
        
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        raise ValueError(f"Failed to convert Parquet to CSV: {e}")


def convert_multiple_to_zip_bytes(
    files: List[Tuple[str, BinaryIO]],
    separator: str = ",",
    encoding: str = "utf-8",
    include_header: bool = True,
    convert_complex_to_json: bool = True,
    batch_size: int = 50000,
    progress_callback: Optional[callable] = None
) -> bytes:
    """
    Convert multiple Parquet files to CSV and package them in a ZIP archive.
    
    Args:
        files: List of (filename, file_like) tuples
        separator: CSV field separator
        encoding: Output encoding
        include_header: Include column headers
        convert_complex_to_json: Convert complex types to JSON strings
        batch_size: Number of rows per batch
        progress_callback: Optional callback(current_file, total_files, filename) for progress
        
    Returns:
        ZIP archive as bytes
    """
    logger.info(f"Converting {len(files)} files to ZIP archive")
    
    zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for i, (filename, file_like) in enumerate(files):
            try:
                # Generate CSV filename
                csv_filename = filename.rsplit('.', 1)[0] + '.csv'
                
                logger.info(f"Processing file {i+1}/{len(files)}: {filename}")
                
                if progress_callback:
                    progress_callback(i, len(files), filename)
                
                # Convert to CSV
                csv_bytes = convert_parquet_filelike_to_csv_bytes(
                    file_like,
                    separator=separator,
                    encoding=encoding,
                    include_header=include_header,
                    convert_complex_to_json=convert_complex_to_json,
                    batch_size=batch_size
                )
                
                # Add to ZIP
                zip_file.writestr(csv_filename, csv_bytes)
                logger.info(f"Added {csv_filename} to archive ({len(csv_bytes)} bytes)")
                
            except Exception as e:
                logger.error(f"Failed to process {filename}: {e}")
                # Add error file to ZIP
                error_msg = f"Error converting {filename}: {e}"
                zip_file.writestr(f"{filename}.error.txt", error_msg.encode('utf-8'))
    
    if progress_callback:
        progress_callback(len(files), len(files), "Complete")
    
    logger.info(f"ZIP archive created: {zip_buffer.tell()} bytes")
    return zip_buffer.getvalue()


def get_common_columns(files: List[Tuple[str, BinaryIO]]) -> List[str]:
    """
    Get columns that are common across all Parquet files.
    
    Args:
        files: List of (filename, file_like) tuples
        
    Returns:
        List of column names present in all files
    """
    if not files:
        return []
    
    common_cols = None
    for filename, file_like in files:
        try:
            file_like.seek(0)
            pf = pq.ParquetFile(file_like)
            cols = set(pf.schema_arrow.names)
            if common_cols is None:
                common_cols = cols
            else:
                common_cols = common_cols.intersection(cols)
        except Exception as e:
            logger.warning(f"Could not read schema from {filename}: {e}")
    
    return sorted(list(common_cols)) if common_cols else []


def get_unified_schema(files: List[Tuple[str, BinaryIO]]) -> Tuple[pa.Schema, List[str]]:
    """
    Analyze schemas from multiple Parquet files and return a unified schema.
    
    Args:
        files: List of (filename, file_like) tuples
        
    Returns:
        Tuple of (unified schema, list of warning messages)
    """
    warnings = []
    schemas = []
    
    for filename, file_like in files:
        try:
            file_like.seek(0)
            pf = pq.ParquetFile(file_like)
            schemas.append((filename, pf.schema_arrow))
        except Exception as e:
            warnings.append(f"Cannot read {filename}: {e}")
    
    if not schemas:
        raise ValueError("No valid Parquet files to merge")
    
    # Start with first schema
    base_name, base_schema = schemas[0]
    unified_fields = {field.name: field for field in base_schema}
    
    # Check compatibility with other schemas
    for filename, schema in schemas[1:]:
        for field in schema:
            if field.name in unified_fields:
                existing = unified_fields[field.name]
                if existing.type != field.type:
                    # Try to find common type (promote to string if incompatible)
                    warnings.append(
                        f"Column '{field.name}': type mismatch ({existing.type} vs {field.type}), will cast to string"
                    )
                    unified_fields[field.name] = pa.field(field.name, pa.string())
            else:
                # Column only in some files - add as nullable
                unified_fields[field.name] = field.with_nullable(True)
                warnings.append(f"Column '{field.name}' not in all files, will be null where missing")
    
    unified_schema = pa.schema(list(unified_fields.values()))
    return unified_schema, warnings


def _cast_batch_to_schema(batch: pa.RecordBatch, target_schema: pa.Schema, source_file: str = None) -> pa.RecordBatch:
    """
    Cast a record batch to match the target schema.
    """
    arrays = []
    for field in target_schema:
        if field.name == "_source_file":
            # Add source file column
            arrays.append(pa.array([source_file] * len(batch), type=pa.string()))
        elif field.name in batch.schema.names:
            col_idx = batch.schema.get_field_index(field.name)
            col = batch.column(col_idx)
            if col.type != field.type:
                # Cast to target type
                try:
                    col = col.cast(field.type)
                except Exception:
                    # Fall back to string
                    col = pa.array([str(v) if v is not None else None for v in col.to_pylist()], type=pa.string())
            arrays.append(col)
        else:
            # Column missing - fill with nulls
            arrays.append(pa.nulls(len(batch), type=field.type))
    
    return pa.RecordBatch.from_arrays(arrays, schema=target_schema)


def merge_parquets_union_to_csv_bytes(
    files: List[Tuple[str, BinaryIO]],
    add_source_column: bool = False,
    separator: str = ",",
    encoding: str = "utf-8",
    include_header: bool = True,
    convert_complex_to_json: bool = True,
    batch_size: int = 50000,
    progress_callback: Optional[callable] = None
) -> bytes:
    """
    Merge multiple Parquet files via UNION ALL and convert to CSV.
    
    Uses streaming to minimize memory usage - processes one batch at a time.
    
    Args:
        files: List of (filename, file_like) tuples
        add_source_column: Add '_source_file' column to identify row origin
        separator: CSV field separator
        encoding: Output encoding
        include_header: Include column headers
        convert_complex_to_json: Convert complex types to JSON
        batch_size: Rows per batch
        progress_callback: Optional callback(current_file, total_files, filename)
        
    Returns:
        Merged CSV as bytes
    """
    logger.info(f"Merging {len(files)} files via UNION ALL")
    
    # Get unified schema
    unified_schema, warnings = get_unified_schema(files)
    for w in warnings:
        logger.warning(w)
    
    # Add source file column if requested
    if add_source_column:
        unified_schema = unified_schema.append(pa.field("_source_file", pa.string()))
    
    output_buffer = io.BytesIO()
    first_batch = True
    total_files = len(files)
    
    for file_idx, (filename, file_like) in enumerate(files):
        try:
            file_like.seek(0)
            pf = pq.ParquetFile(file_like)
            
            if progress_callback:
                progress_callback(file_idx, total_files, filename)
            
            for batch in pf.iter_batches(batch_size=batch_size):
                # Cast batch to unified schema
                source = filename if add_source_column else None
                unified_batch = _cast_batch_to_schema(batch, unified_schema, source)
                batch_table = pa.Table.from_batches([unified_batch])
                
                # Handle complex types
                batch_table = _cast_complex_columns_to_string(batch_table, convert_complex_to_json)
                
                # Write to buffer
                temp_buffer = io.BytesIO()
                write_options = pa_csv.WriteOptions(
                    include_header=(first_batch and include_header),
                    delimiter=separator
                )
                pa_csv.write_csv(batch_table, temp_buffer, write_options=write_options)
                output_buffer.write(temp_buffer.getvalue())
                first_batch = False
                
        except Exception as e:
            logger.error(f"Error processing {filename}: {e}")
            raise ValueError(f"Failed to merge {filename}: {e}")
    
    if progress_callback:
        progress_callback(total_files, total_files, "Complete")
    
    csv_bytes = output_buffer.getvalue()
    if encoding.lower() != "utf-8":
        csv_bytes = csv_bytes.decode("utf-8").encode(encoding, errors="replace")
    
    logger.info(f"UNION merge complete: {len(csv_bytes)} bytes")
    return csv_bytes


def merge_parquets_join_to_csv_bytes(
    files: List[Tuple[str, BinaryIO]],
    join_key: str,
    join_type: str = "inner",
    separator: str = ",",
    encoding: str = "utf-8",
    progress_callback: Optional[callable] = None
) -> bytes:
    """
    Join multiple Parquet files on a key column using DuckDB.
    
    Args:
        files: List of (filename, file_like) tuples
        join_key: Column name to join on
        join_type: Type of join (inner, left, right, outer)
        separator: CSV field separator
        encoding: Output encoding
        progress_callback: Optional callback(current, total, message)
        
    Returns:
        Joined CSV as bytes
    """
    import duckdb
    
    logger.info(f"Joining {len(files)} files on '{join_key}' ({join_type} join)")
    
    if len(files) < 2:
        raise ValueError("Need at least 2 files for JOIN")
    
    # Write files to temp directory
    temp_dir = tempfile.mkdtemp()
    temp_files = []
    
    try:
        for i, (filename, file_like) in enumerate(files):
            if progress_callback:
                progress_callback(i, len(files) + 1, f"Preparing {filename}")
            
            file_like.seek(0)
            temp_path = Path(temp_dir) / f"file_{i}.parquet"
            with open(temp_path, 'wb') as f:
                f.write(file_like.read())
            temp_files.append(temp_path)
        
        if progress_callback:
            progress_callback(len(files), len(files) + 1, "Executing JOIN...")
        
        # Build JOIN query
        conn = duckdb.connect(":memory:")
        
        # Map join type
        join_sql = {
            "inner": "INNER JOIN",
            "left": "LEFT JOIN", 
            "right": "RIGHT JOIN",
            "outer": "FULL OUTER JOIN"
        }.get(join_type, "INNER JOIN")
        
        # Build SELECT with aliased columns to avoid collisions
        # Start with first file - MUST have alias t0
        from_clause = f"read_parquet('{temp_files[0]}') t0"
        
        # Get column names for first file
        first_cols = conn.execute(f"SELECT * FROM read_parquet('{temp_files[0]}') LIMIT 0").description
        first_col_names = [col[0] for col in first_cols]
        
        select_parts = [f"t0.{join_key}"]  # Join key once
        for col in first_col_names:
            if col != join_key:
                select_parts.append(f"t0.{col}")
        
        # Add remaining files with JOIN
        for i, temp_file in enumerate(temp_files[1:], 1):
            from_clause += f" {join_sql} read_parquet('{temp_file}') t{i} ON t0.{join_key} = t{i}.{join_key}"
            
            # Get columns from this file
            file_cols = conn.execute(f"SELECT * FROM read_parquet('{temp_file}') LIMIT 0").description
            for col in file_cols:
                col_name = col[0]
                if col_name != join_key:
                    # Add suffix if column exists
                    alias = f"{col_name}_{i}" if col_name in first_col_names else col_name
                    select_parts.append(f"t{i}.{col_name} AS {alias}")
        
        # Execute query
        query = f"SELECT {', '.join(select_parts)} FROM {from_clause}"
        logger.info(f"Executing: {query[:200]}...")
        
        result = conn.execute(query).fetchall()
        columns = [col[0] for col in conn.description]
        
        # Convert to CSV
        output_buffer = io.BytesIO()
        
        # Write header
        header = separator.join(f'"{c}"' for c in columns) + "\n"
        output_buffer.write(header.encode("utf-8"))
        
        # Write rows
        for row in result:
            row_str = separator.join(
                f'"{str(v)}"' if v is not None else '' for v in row
            ) + "\n"
            output_buffer.write(row_str.encode("utf-8"))
        
        conn.close()
        
        if progress_callback:
            progress_callback(len(files) + 1, len(files) + 1, "Complete")
        
        csv_bytes = output_buffer.getvalue()
        if encoding.lower() != "utf-8":
            csv_bytes = csv_bytes.decode("utf-8").encode(encoding, errors="replace")
        
        logger.info(f"JOIN complete: {len(result)} rows, {len(csv_bytes)} bytes")
        return csv_bytes
        
    finally:
        # Cleanup temp files
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)
