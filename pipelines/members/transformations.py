from framework.readers.data_reader_generic import read
from framework.session.spark_session import create_spark_session
from framework.config.config_reader import get_app_config, get_pyspark_config

from pyspark.sql.functions import *

def clean_members_data(members_df):
    member_details=members_df.withColumn('ingestion_date',current_timestamp())
    member_details=member_details.dropDuplicates()
    member_details=member_details.filter(col('annual_income').isNotNull())
    member_details=member_details.withColumn('emp_length',regexp_replace('emp_length',r'[^0-9*]','').cast('int'))
    avg_emp_len=member_details.select(round(avg('emp_length'),0)).first()[0]
    member_details=member_details.fillna(avg_emp_len,subset=['emp_length'])
    member_details=member_details.withColumn('address_state',when(length(col('address_state'))>2,'N/A').otherwise(col('address_state')))
    member_details=member_details.repartition(10)

    return member_details