from framework.readers.data_reader_generic import read
from framework.session.spark_session import create_spark_session
from framework.config.config_reader import get_app_config
from schemas.loans_schema import get_loan_details_schema
from framework.readers.read_options import get_read_options
from pipelines.loans.transformations import clean_loan_details
from framework.writers.data_writer import write
from framework.dq_checks.dq_orchestrator import apply_dq_for_table
from framework.logger.logger_file import Log4j
from framework.scd.scd_logics import write_scd2
from pyspark.sql.functions import *

spark=create_spark_session("LOCAL")

logger=Log4j(spark)

logger.info('Created Spark session')

logger.info(f'Reading data from loacation {get_app_config("LOCAL")["loans.file.path"]}')

raw_file_format=get_app_config("LOCAL")["raw_file_format"]
processed_file_format=get_app_config("LOCAL")["processed_file_format"]

loan_details_df=read(spark=spark,\
               format=raw_file_format,\
                file_path=get_app_config("LOCAL")["loans.file.path"],\
                schema=get_loan_details_schema(),\
                options=get_read_options(raw_file_format))

logger.info('Segregating bad data from raw files')
# Apply data quality checks to identify valid and invalid records
# Returns three DataFrames: clean (passed QC), bad (failed QC), detailed_bad (failures with reason)
pk=['id']
clean_df, bad_df, detailed_bad_df = apply_dq_for_table(
    loan_details_df,
    table_name="loan_details",
    key_columns=pk
)

logger.info('Enabling SCD2')

tracking_cols=[col for col in clean_df.columns if col]
write_scd2(incoming_df=clean_df,
           primary_key=pk[0],
           target_location=get_app_config("LOCAL")["loans.output.clean.path"],
           tracking_cols=tracking_cols
           )

logger.info(f'Clean data has been written to location {get_app_config("LOCAL")["loans.output.clean.path"]}')

clean_df_scd2=read(spark=spark,
                   format=processed_file_format,
                   file_path=get_app_config("LOCAL")["loans.output.clean.path"],
                   options=get_read_options(processed_file_format)
                   )

clean_df_current=clean_df_scd2.filter(col('is_current')==True)

logger.info('Preparing data for processed layer')

loan_details_transformed_df=clean_loan_details(loan_details_df)

logger.info('Preparing data for processed layer')
# Transform clean data into standardized format
loans_transformed_df = clean_loan_details(clean_df_current)

logger.info(f'Writing data to processed location {get_app_config("LOCAL")["loans.output.processed.path"]}')

write(df=loans_transformed_df,
      file_format=processed_file_format,
      mode='overwrite',
      partitionBy=None,
      output_path=get_app_config("LOCAL")["loans.output.processed.path"]
      )

logger.info(f'Writing DQ rejected data to location {get_app_config("LOCAL")["loans.output.bad.path"]}')
write(df=detailed_bad_df,\
      file_format=processed_file_format,\
      mode="overwrite",\
      partitionBy=None,\
      output_path=get_app_config("LOCAL")["loans.output.bad.path"])