import pytest
from framework.session.spark_session import create_spark_session

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