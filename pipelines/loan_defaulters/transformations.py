from framework.session.spark_session import create_spark_session
from framework.config.config_reader import get_app_config, get_pyspark_config

from pyspark.sql.functions import *

def clean_delinq_data(loan_defaulters_df):
   print('Cleaning delinquency data...')
   delinq_df=loan_defaulters_df\
                .select('member_id',
                        'delinq_2_years',
                        'delinq_amount',
                        'months_since_last_delinq')\
                .withColumn('delinq_2_years',col('delinq_2_years').cast('int'))\
                .withColumn('months_since_last_delinq',col('months_since_last_delinq').cast('int'))
   return delinq_df

def clean_defaulters_data(loan_defaulters_df):
    print('Cleaning defaulters data...')
    defaulters_df=loan_defaulters_df\
                        .select('member_id',
                                'public_record',
                                'public_record_bankruptcies',
                                'months_since_last_record',
                                'inquiry_last_6_months')\
                        .withColumn('public_record',col('public_record').cast('int'))\
                        .withColumn('public_record_bankruptcies',col('public_record_bankruptcies').cast('int'))\
                        .withColumn('inquiry_last_6_months',col('inquiry_last_6_months').cast('int'))\
                        .withColumn('months_since_last_record',col('months_since_last_record').cast('int'))
    return defaulters_df