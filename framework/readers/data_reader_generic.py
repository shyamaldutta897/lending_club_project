from framework.readers.read_options import get_read_options

def read(spark,
         format:str,
         file_path:str,
         schema=None,
         options:dict=None):
    reader = spark.read.format(format)
    if schema:
        reader = reader.schema(schema)
    if options:
        reader = reader.options(**options)


    return reader.load(file_path)