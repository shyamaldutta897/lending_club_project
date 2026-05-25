def read(spark,
         format: str,
         file_path: str,
         schema=None,
         options: dict = None):

    reader = spark.read.format(format)

    # If CSV with schema provided, disable header so schema applies by column position
    if format == 'csv' and schema is not None:
        if options is None:
            options = {}
        else:
            options = dict(options)
        options['header'] = 'false'

    if schema:
        reader = reader.schema(schema)
    if options:
        reader = reader.options(**options)

    return reader.load(file_path)