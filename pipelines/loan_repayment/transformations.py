from framework.session.spark_session import create_spark_session
from framework.config.config_reader import get_app_config, get_pyspark_config

from pyspark.sql.functions import *

def clean_loan_repayment(loan_repayment_df):
    print('Cleaning repayment data...')
    loans_repayment_ingestion_date=loan_repayment_df.withColumn('ingestion_date',current_timestamp())
    loans_repayment_corrected=loans_repayment_ingestion_date.withColumn('total_payment',expr("total_principal_received+total_interest_received+total_late_fee_received"))\
                                                .filter(col('total_payment')!=0)
    loans_repayment_dates_fix=loans_repayment_corrected.withColumn('last_payment_date',when(col('last_payment_date').cast('int').isNotNull(),None).otherwise(col('last_payment_date')))\
                                                   .withColumn('next_payment_date',when(col('next_payment_date').cast('int').isNotNull(),None).otherwise(col('next_payment_date')))
    
    return loans_repayment_dates_fix