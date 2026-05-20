from framework.readers.data_reader import read_loan_repayments_data
from framework.utils.spark_session import create_spark_session
from framework.config.config_reader import get_app_config, get_pyspark_config

from pyspark.sql.functions import *

def clean_loan_repayment(loan_repayment_df):
    loans_repayment_ingestion_date=loan_repayment_df.withColumn('ingestion_date',current_timestamp())
    fields=['total_principal_received','total_interest_received','total_late_fee_received','total_payment','last_payment_amount',]
    loans_repayment_dropna=loans_repayment_ingestion_date.dropna(subset=fields)
    loans_repayment_corrected=loans_repayment_dropna.withColumn('total_payment',expr("total_principal_received+total_interest_received+total_late_fee_received"))\
                                                .filter(col('total_payment')!=0)
    loans_repayment_dates_fix=loans_repayment_corrected.withColumn('last_payment_date',when(col('last_payment_date').cast('int').isNotNull(),None).otherwise(col('last_payment_date')))\
                                                   .withColumn('next_payment_date',when(col('next_payment_date').cast('int').isNotNull(),None).otherwise(col('next_payment_date')))
    
    return loans_repayment_dates_fix