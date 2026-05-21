csv_option={'header':'true', 'inferSchema':'false'}
json_option={'multiline':'true', 'inferSchema':'false'}
parquet_option={}

def get_read_options(format:str):
    if format=="csv":
        return csv_option
    elif format=="json":
        return json_option
    elif format=="parquet":
        return parquet_option
    else:
        raise ValueError(f"Unsupported format: {format}")