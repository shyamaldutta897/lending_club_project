import pytest
from framework.session.spark_session import create_spark_session
from framework.config.config_reader import get_app_config
import os

# Configure Hadoop paths for Windows compatibility
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"

"""
Default scope of fixture is function, So it means that SparkSession will be created for each and 
every test functions - which is very time consuming and ineffecient. So calling out the scope as session 
so that SparkSession reamins active throughout all the tests

"""
@pytest.fixture(scope="session")
def spark():
    """
    Creates a shared Spark session for the entire test run.
    """
    print("\nStarting shared Spark Session for unit tests...")
    # Initialize using your framework's built-in session creator
    spark_session = create_spark_session('LOCAL')
    
    yield spark_session
    
    print("\nStopping shared Spark Session...")
    spark_session.stop()
@pytest.fixture #scope should only be function, so not calling out that parameter
def config():
    print("Getting the data locations..")
    get_config=get_app_config('LOCAL')
    return  get_config
@pytest.fixture
def test_helper(spark):
    data=[('jack',20),('john',30)]
    schema = 'name string, age int'
    

    setup_df=spark.createDataFrame(data=data,schema=schema)

    return setup_df

