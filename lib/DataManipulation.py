from DataReader import read_members_data, read_loan_details, read_loan_repayment, read_loan_defaulters
from utils import get_spark_session
from lib import ConfigReader

from pyspark.sql.functions import *

def members_data_clean(members_df):
    member_details=members_df.withColumn('ingestion_date',current_timestamp())
    member_details=member_details.dropDuplicates()
    member_details=member_details.filter(col('annual_income').isNotNull())
    member_details=member_details.withColumn('emp_length',regexp_replace('emp_length',r'[^0-9*]','').cast('int'))
    avg_emp_len=member_details.select(round(avg('emp_length'),0)).first()[0]
    member_details=member_details.fillna(avg_emp_len,subset=['emp_length'])
    member_details=member_details.withColumn('address_state',when(length(col('address_state'))>2,'N/A').otherwise(col('address_state')))
    member_details=member_details.repartition(10)

    return member_details

def loan_details_clean(loan_details_df):
    loan_df_ingest=loan_details_df.withColumn('ingestion_date',current_timestamp())
    loan_df_filtered=['loan_amount','funded_amount','term','interest_rate','installment','issue_month_year','loan_status','purpose','title']
    loan_df_filtered=loan_df_ingest.dropna(how='any',subset=loan_df_filtered)
    loan_df_year_added=loan_df_filtered.withColumn('term_int',regexp_replace('term',r'[^0-9]*','').cast('int'))\
                                   .withColumn('term_years',(col('term_int')/12).cast('int'))\
                                   .drop('term_int')
    loan_valid_purpose=['debt_consolidation','credit_card','home_improvement','major_purchase','medical','small_business','car','vacation','moving','house','wedding','renewable_energy','educational','other']
    loan_df_purpose_fixed=loan_df_year_added.withColumn('purpose',when(col('purpose').isin(loan_valid_purpose), col('purpose')).otherwise('other'))

    return loan_df_purpose_fixed

def loan_repayment_clean(loan_repayment_df):
    loans_repayment_ingestion_date=loan_repayment_df.withColumn('ingestion_date',current_timestamp())
    fields=['total_principal_received','total_interest_received','total_late_fee_received','total_payment','last_payment_amount',]
    loans_repayment_dropna=loans_repayment_ingestion_date.dropna(subset=fields)
    loans_repayment_corrected=loans_repayment_dropna.withColumn('total_payment',expr("total_principal_received+total_interest_received+total_late_fee_received"))\
                                                .filter(col('total_payment')!=0)
    loans_repayment_dates_fix=loans_repayment_corrected.withColumn('last_payment_date',when(col('last_payment_date').cast('int').isNotNull(),None).otherwise(col('last_payment_date')))\
                                                   .withColumn('next_payment_date',when(col('next_payment_date').cast('int').isNotNull(),None).otherwise(col('next_payment_date')))
    
    return loans_repayment_dates_fix

def delinq_df(loan_defaulters_df):
   delinq_df=loan_defaulters_df.select('member_id_custom','delinq_2_years','delinq_amount','months_since_last_delinq')
   return delinq_df

def defaulters_df(loan_defaulters_df):
    defaulters_df=loan_defaulters_df.select('member_id_custom','public_record','public_record_bankruptcies','inquiry_last_6_months')
    return defaulters_df


    



  