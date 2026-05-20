from pyspark.sql import DataFrame

def write(df: DataFrame,
          file_format:str="parquet",
          mode:str="overwrite",
          partitionBy:list=None,
          output_path:str=None):
    writer= df.write\
        .format(file_format)\
        .mode(mode)
    if file_format=="csv":
        writer=writer.option('header','true')
    if partitionBy:
        writer=writer.partitionBy(*partitionBy)
    writer.save(output_path)

        