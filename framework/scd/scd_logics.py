from framework.readers.data_reader_generic import read
from framework.writers.data_writer import write 
from delta import DeltaTable
from pyspark.sql.functions import *
from framework.session.spark_session import create_spark_session
import os 

def write_scd2(incoming_df,primary_key,target_location,tracking_cols):
    absolute_taregt_path=os.path.abspath(target_location)
    spark=create_spark_session('LOCAL')

    if not DeltaTable.isDeltaTable(spark,absolute_taregt_path):
        #valid_to is converted to timestamp since Parquet drops NullType fields
        df=incoming_df.withColumn('valid_from',current_timestamp())\
                      .withColumn('valid_to',lit(None).cast('timestamp'))\
                      .withColumn('is_current',lit(True))

        write(df,'delta','overwrite',None,absolute_taregt_path)

        return