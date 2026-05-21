from pyspark.sql import SparkSession
from framework.config.config_reader import get_pyspark_config
import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"

def create_spark_session(env):
    builder = SparkSession.builder \
        .config(conf=get_pyspark_config(env))\
        .config("spark.hadoop.io.native.lib.available", "false")
    
    if env == "LOCAL":
        builder = builder.master("local[2]")
    else:
        builder = builder.enableHiveSupport()
    return builder.getOrCreate()