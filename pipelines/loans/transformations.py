from framework.readers.data_reader import read_loans_data
from framework.utils.spark_session import create_spark_session
from framework.config.config_reader import get_app_config, get_pyspark_config

from pyspark.sql.functions import *

def clean_loan_details(loan_details_df):
    loan_df_ingest=loan_details_df.withColumn('ingestion_date',current_timestamp())
    required_fields=['loan_amount','funded_amount','term','interest_rate','installment','issue_month_year','loan_status','purpose','title']
    loan_df_filtered=loan_df_ingest.dropna(how='any',subset=required_fields)
    loan_df_year_added=loan_df_filtered.withColumn('term_int',regexp_replace('term',r'[^0-9]*','').cast('int'))\
                                   .withColumn('term_years',(col('term_int')/12).cast('int'))\
                                   .drop('term_int')
    loan_valid_purpose=['debt_consolidation','credit_card','home_improvement','major_purchase','medical','small_business','car','vacation','moving','house','wedding','renewable_energy','educational','other']
    loan_df_purpose_fixed=loan_df_year_added.withColumn('purpose',when(col('purpose').isin(loan_valid_purpose), col('purpose')).otherwise('other'))

    return loan_df_purpose_fixed