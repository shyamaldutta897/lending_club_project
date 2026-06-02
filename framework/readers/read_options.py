"""
Format-specific read options for different data source types.
Used by the generic data reader to configure Spark's read operations.
"""
# CSV read options: header row present, don't infer schema (use explicit schema instead)
csv_option = {'header': 'true', 'inferSchema': 'false'}

# JSON read options: support multi-line JSON objects, explicit schema required
json_option = {'multiline': 'true', 'inferSchema': 'false'}

# Parquet read options: parquet format is self-describing, no additional options needed
parquet_option = {'mergeSchema':'true'}

# Delta read options: Similar to Parquet, delta format is self-describing as well, no additional options needed
delta_option={'mergeSchema':'true'}

def get_read_options(format: str):
    """
    Get format-specific read options for the requested data type.
    Args:
        format (str): Data format - 'csv', 'json', or 'parquet' 
    Returns:
        dict: Format-specific options for Spark read operation  
    Raises:
        ValueError: If the format is not supported
    """
    if format == "csv":
        return csv_option
    elif format == "json":
        return json_option
    elif format == "parquet":
        return parquet_option
    elif format == "delta":
        return delta_option
    else:
        raise ValueError(f"Unsupported format: {format}")