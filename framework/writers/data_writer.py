from pyspark.sql import DataFrame

def write(df: DataFrame,
          file_format:str,
          mode:str,
          partitionBy:list,
          output_path:str):
    writer= df.write\
        .format(file_format)\
        .mode(mode)
    if file_format=="csv":
        writer=writer.option('header','true')
    if partitionBy:
        writer=writer.partitionBy(*partitionBy)
    writer.save(output_path)

        