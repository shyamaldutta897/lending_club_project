from framework.session.spark_session import create_spark_session
from framework.config.config_reader import get_app_config, get_pyspark_config

from pyspark.sql.functions import *

def clean_loan_details(loan_details_df):
    """
    Transform and enrich loan detail records.
    
    Transformations:
    - Add ingestion timestamp for audit trail
    - Convert loan term from string format (e.g., "36 months") to numeric years
    - Standardize loan purposes to a controlled vocabulary
    - Handle missing values by defaulting to 'other' category
    """
    
    # Add timestamp for tracking when record was processed
    loan_df_ingest = loan_details_df.withColumn('ingestion_date', current_timestamp())
    
    # Convert term string to numeric years (e.g., "36 months" → 3 years)
    loan_df_year_added = loan_df_ingest.withColumn('term_int', regexp_replace('term', r'[^0-9]*', '').cast('int'))\
                                       .withColumn('term_years', (col('term_int') / 12).cast('int'))\
                                       .drop('term_int')
    
    # Standardize loan purposes to known categories
    loan_valid_purpose = ['debt_consolidation', 'credit_card', 'home_improvement', 'major_purchase', 
                          'medical', 'small_business', 'car', 'vacation', 'moving', 'house', 
                          'wedding', 'renewable_energy', 'educational', 'other']
    loan_df_purpose_fixed = loan_df_year_added.withColumn('purpose', 
                                                           when(col('purpose').isin(loan_valid_purpose), col('purpose'))
                                                           .otherwise('other'))

    return loan_df_purpose_fixed