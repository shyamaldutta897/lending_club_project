import os
import sys

# Get the absolute path of the directory where THIS script lives
script_dir = os.path.dirname(os.path.abspath(__file__))
# Add the project root to sys.path so the top-level 'lib' package is importable
project_root = os.path.abspath(os.path.join(script_dir, os.pardir))
sys.path.insert(0, project_root)

from pyspark.sql import SparkSession
from lib.ConfigReader import get_pyspark_config
def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
        .config(conf=get_pyspark_config(env)) \
        .master("local[2]") \
        .getOrCreate()
   
    else:
        return SparkSession.builder \
        .config(conf=get_pyspark_config(env)) \
        .enableHiveSupport() \
        .getOrCreate()


