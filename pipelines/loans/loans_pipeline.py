from framework.readers.data_reader_generic import read
from framework.session.spark_session import create_spark_session
from framework.config.config_reader import get_app_config
from schemas.loans_schema import get_loan_details_schema
from framework.readers.read_options import get_read_options
from pipelines.loans.transformations import clean_loan_details
from framework.writers.data_writer import write
from framework.dq_checks.dq_orchestrator import apply_dq_for_table
from framework.logger.logger_file import Log4j

spark=create_spark_session("LOCAL")

logger=Log4j(spark)

logger.info('Created Spark session')

logger.info(f'Reading data from loacation {get_app_config("LOCAL")["loans.file.path"]}')

loan_details_df=read(spark,\
               'csv',\
                get_app_config("LOCAL")["loans.file.path"],\
                get_loan_details_schema(),\
                get_read_options("csv"))

logger.info('Segregating bad data from raw files')

clean_df, bad_df, detailed_bad_df = apply_dq_for_table(
    loan_details_df,
    table_name="loan_details",
    key_columns=["id"]
)

logger.info('Preparing data for processed layer')

loan_details_transformed_df=clean_loan_details(loan_details_df)

logger.info(f'Writing clean data to location {get_app_config("LOCAL")["loans.output.clean.path"]}')

write(df=loan_details_transformed_df,\
      file_format="csv",\
      mode="overwrite",\
      partitionBy=None,\
      output_path=get_app_config("LOCAL")["loans.output.clean.path"])

logger.info(f'Writing DQ rejected data to location {get_app_config("LOCAL")["loans.output.bad.path"]}')
write(df=detailed_bad_df,\
      file_format="csv",\
      mode="overwrite",\
      partitionBy=None,\
      output_path=get_app_config("LOCAL")["loans.output.bad.path"])