from pathlib import Path
from pyspark.sql import SparkSession
from framework.config.config_reader import get_pyspark_config
from configs.calculation_config import custom_spark_confs
import os

# Configure Hadoop paths for Windows compatibility
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"

def create_spark_session(env):
    """
    Initialize and configure a Spark session for the given environment.
    
    Args:
        env (str): Environment name - "LOCAL" for development (2-core local mode),
                   or other environment names for production (Hive-enabled)
    
    Returns:
        SparkSession: Configured Spark session ready for data processing
        
    The session includes:
    - Initializing Spark with Deltalake 3.2
    - Environment-specific Spark configurations from pyspark.conf
    - Custom scoring parameters injected from calculation_config
    - Hadoop native lib disabled to prevent Windows compatibility issues
    - Local master with 2 cores for LOCAL environment
    - Hive support for other environments
    - Enables logging with log4j2 support
    """
    # Point Spark at the repository log4j2 configuration file using a file URI.
    log4j_path = Path(__file__).resolve().parents[2] / 'configs' / 'log4j2.properties'
    log4j_uri = log4j_path.as_uri()

    builder = SparkSession.builder \
        .appName('lending_club_app')\
        .config(conf=get_pyspark_config(env))\
        .config("spark.hadoop.io.native.lib.available", "false")\
        .config('spark.driver.extraJavaOptions',
                f'-Dlog4j2.configurationFile={log4j_uri}')\
        .config('spark.executor.extraJavaOptions',
                f'-Dlog4j2.configurationFile={log4j_uri}')\
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Inject custom scoring parameters (point thresholds, weights, etc.)
    for key, val in custom_spark_confs.items():
        builder = builder.config(key, val)
    
    # Configure for local development or production
    if env == "LOCAL":
        builder = builder.master("local[2]")
    else:
        builder = builder.enableHiveSupport()

    return builder.getOrCreate()

