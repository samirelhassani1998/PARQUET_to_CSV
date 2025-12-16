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
