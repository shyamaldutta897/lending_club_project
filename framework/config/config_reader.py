import os
import configparser
from pyspark import SparkConf
# loading the application configs in python dictionary
def get_app_config(env):
    config = configparser.ConfigParser()

    # Dynamically find the absolute path of the configs folder
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.abspath(
    os.path.join(
        script_dir,
        "..",
        "..",
        "configs",
        "application.conf"
        )
    )
    read_files=config.read(config_path)
    if not read_files:
        raise ValueError(f"Unable to read the application configuration file at {config_path}")
    
    app_conf = {}
    if env not in config.sections():
        raise ValueError(f"Environment '{env}' not found in the configuration file." 
                        f"Available environments: {config.sections()}")

    for (key, val) in config.items(env):
        app_conf[key] = val
    return app_conf


# loading the pyspark configs and creating a spark conf object
def get_pyspark_config(env):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    pyspark_config_path = os.path.abspath(
    os.path.join(
        script_dir,
        "..",
        "..",
        "configs",
        "pyspark.conf"
        )
    )

    config = configparser.ConfigParser()
    read_files=config.read(pyspark_config_path)

    if not read_files:
        raise ValueError(f"Unable to read the PySpark configuration file at {pyspark_config_path}")
    
    pyspark_conf = SparkConf()
    for (key, val) in config.items(env):
        pyspark_conf.set(key, val)
    return pyspark_conf



