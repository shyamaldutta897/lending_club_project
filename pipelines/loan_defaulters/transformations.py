from framework.readers.data_reader import read_loan_defaulters_data
from framework.utils.spark_session import create_spark_session
from framework.config.config_reader import get_app_config, get_pyspark_config

from pyspark.sql.functions import *

def clean_delinq_df(loan_defaulters_df):
   delinq_df=loan_defaulters_df.select('member_id_custom','delinq_2_years','delinq_amount','months_since_last_delinq')
   return delinq_df

def clean_defaulters_df(loan_defaulters_df):
    defaulters_df=loan_defaulters_df.select('member_id_custom','public_record','public_record_bankruptcies','inquiry_last_6_months')
    return defaulters_df