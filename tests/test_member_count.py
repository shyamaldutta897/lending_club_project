import pytest
from configs.calculation_config import custom_spark_confs as params
from framework.session.spark_session import create_spark_session
from framework.config.config_reader import get_app_config
from pipelines.curated import read_processed_data as prd
from framework.readers.data_reader_generic import read
from framework.writers.data_writer import write
from framework.readers.read_options import get_read_options
from pyspark.sql.functions import *
import os

# Force Spark to use the EXACT same Python environment that Pytest is running in
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Configure Hadoop paths for Windows compatibility
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"


from pipelines.curated.business_logics_implement import (
    create_ph_pts_view,
    create_ldh_ph_df_view,
    create_fh_ldh_ph_df_view,
    create_loan_score_view,
    loan_score_final_view
)
@pytest.mark.utility
def test_read(spark,test_helper):
    path='data/uat/test_read'
    schema = 'name string, age int'
    test_helper.write\
            .format('csv')\
            .mode('overwrite')\
            .save(path)
    
    test_df=read(spark,'csv',path,schema,{'header':'false','inferSchema':'false'})

    assert test_helper.schema == test_df.schema, f'Read is not working properly, Schemas are not identical'
    assert test_helper.collect() == test_df.collect() , f'Data is not matching while read operation'
@pytest.mark.utility
def test_write(spark,test_helper):
    schema = 'name string, age int'
    path='data/uat/test_write'

    write(test_helper.repartition(1),'csv','overwrite',None,path)
    test_df=spark.read.format('csv').schema(schema).load(path)

    sort_cols=test_df.columns

    assert test_helper.schema== test_df.schema, f'Write is not working properly, Schemas are not identical '
    assert test_helper.sort(*sort_cols).collect() == test_df.sort(*sort_cols).collect() , f'Data is not matching after write operation'

"""
Use of marker in pytest - we can call selected test cases using pytest-m "marker_name"  
e.g. - pytest -m "transformation" - would call only test_loan_score_final_member_counts function
"""
@pytest.mark.transformation
def test_loan_score_final_member_counts(spark,config):

    expected_counts = {
        'F': 4836,
        'E': 47432,
        'B': 60649,
        'D': 561792,
        'C': 1576758,
        None:4071
        }

    app_params = {
        'lending_project.very_good_grade_pts': 90,
        'lending_project.good_grade_pts': 80,
        'lending_project.bad_grade_pts': 60,
        'lending_project.very_bad_grade_pts': 40,
        'lending_project.unacceptable_grade_pts': 20
        }

    #app_config = get_app_config('LOCAL')
    

    members_df = prd.read_members(spark=spark, config=config)
    loans_df = prd.read_loans(spark=spark, config=config)
    loans_repayment_df = prd.read_loan_repayment(spark=spark, config=config)
    loans_delinq_df = prd.read_delinquencies(spark=spark, config=config)
    loans_defaulters_df = prd.read_defaulters(spark=spark, config=config)

    prd.register_views(
        spark,
        members_df,
        loans_df,
        loans_repayment_df,
        loans_delinq_df,
        loans_defaulters_df
    )

    create_ph_pts_view(spark, params)
    create_ldh_ph_df_view(spark, params)
    create_fh_ldh_ph_df_view(spark, params)
    create_loan_score_view(spark)

    final_df = loan_score_final_view(spark, params)

    test_df=final_df.groupBy('loan_final_grade').agg(count('member_id').alias('member_count'))

    actual_counts={}

    for row in test_df.collect():
        actual_counts[row['loan_final_grade']]=row['member_count']
    
    for grade,expected_count in expected_counts.items():
        actual_count=actual_counts[grade]
        assert actual_count==expected_count, f'mismatch for grade : {grade}. Expected count : {expected_count}, got {actual_count}'




