import os
import configparser
from pyspark import SparkConf

def get_app_config(env):
    """
    Load application configuration for the specified environment.
    Args:
        env (str): Environment name (LOCAL, TEST, PROD) matching a section in application.conf
    Returns:
        dict: Configuration key-value pairs for the environment 
    Raises:
        ValueError: If config file cannot be read or environment not found
        
    The configuration includes file paths, output locations, and environment-specific settings
    like which Spark mode to use and where to find input/output data.
    """
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
    read_files = config.read(config_path)
    if not read_files:
        raise ValueError(f"Unable to read the application configuration file at {config_path}")
    
    app_conf = {}
    if env not in config.sections():
        raise ValueError(f"Environment '{env}' not found in the configuration file." 
                        f"Available environments: {config.sections()}")

    for (key, val) in config.items(env):
        app_conf[key] = val
    return app_conf


def get_pyspark_config(env):
    """
    Load PySpark-specific configurations for the specified environment.
    Args:
        env (str): Environment name (LOCAL, TEST, PROD)
    Returns:
        SparkConf: Spark configuration object ready to use when building a session   
    Raises:
        ValueError: If pyspark.conf file cannot be found or read  
    Settings include memory allocation, serialization, and execution optimization parameters
    tailored to the environment's resource availability.
    """
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
    read_files = config.read(pyspark_config_path)

    if not read_files:
        raise ValueError(f"Unable to read the PySpark configuration file at {pyspark_config_path}")
    
    pyspark_conf = SparkConf()
    for (key, val) in config.items(env):
        pyspark_conf.set(key, val)
    return pyspark_conf
