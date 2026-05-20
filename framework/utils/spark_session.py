from pyspark.sql import SparkSession
from framework.config.config_reader import get_pyspark_config

def create_spark_session(env):
    builder = SparkSession.builder \
        .config(conf=get_pyspark_config(env))
    
    if env == "LOCAL":
        builder = builder.master("local[2]")
    else:
        builder = builder.enableHiveSupport()
    return builder.getOrCreate()