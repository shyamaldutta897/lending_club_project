from pyspark.sql import DataFrame

def write(df: DataFrame,
          file_format: str,
          mode: str,
          partitionBy: list,
          output_path: str):
    """
    Write a DataFrame to the specified location in the requested format.
    
    Args:
        df (DataFrame): The Spark DataFrame to write
        file_format (str): Output format - "csv" or "parquet"
        mode (str): Write mode - "overwrite", "append", "ignore", or "error"
        partitionBy (list): Optional list of column names to partition by for large datasets.
                           None if no partitioning needed.
        output_path (str): File system path where data will be written
        
    The writer automatically:
    - Adds headers for CSV output (required for readable CSVs)
    - Applies optional partitioning to improve query performance
    - Logs the output location for tracking and debugging
    """
    print(f'Writing to location {output_path}')
    writer = df.write\
        .format(file_format)\
        .mode(mode)
    
    # CSV files need header row for readability
    if file_format == "csv":
        writer = writer.option('header', 'true')
    # Apply partitioning if specified (useful for large datasets)
    if partitionBy:
        writer = writer.partitionBy(*partitionBy)
    
    writer.save(output_path)