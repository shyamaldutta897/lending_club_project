from framework.readers.data_reader_generic import read
from framework.session.spark_session import create_spark_session
from framework.config.config_reader import get_app_config
from schemas.loan_defaulters_schema import get_loan_defaulters_schema
from framework.readers.read_options import get_read_options
from pipelines.loan_defaulters.transformations import clean_defaulters_data,clean_delinq_data
from framework.writers.data_writer import write
from framework.dq_checks.dq_orchestrator import apply_dq_for_table
from framework.logger.logger_file import Log4j

spark=create_spark_session("LOCAL")

logger=Log4j(spark)

logger.info('Created Spark session')

logger.info(f'Reading data from loacation {get_app_config("LOCAL")["loan_defaulters.file.path"]}')

loan_defaulters_df=read(spark,\
               'csv',\
                get_app_config("LOCAL")["loan_defaulters.file.path"],\
                get_loan_defaulters_schema(),\
                get_read_options("csv"))



logger.info('Segregating bad data from raw files')

clean_df, bad_df, detailed_bad_df= apply_dq_for_table(loan_defaulters_df,
                                                      table_name="loan_defaulters",
                                                      key_columns=None)

logger.info('Separating defaulters data into delinquent data and defaulters data for processed layer')

delinq_transformed_df=clean_delinq_data(clean_df)
defaulters_transformed_df=clean_defaulters_data(clean_df)

logger.info(f'Writing clean data to location {get_app_config("LOCAL")["loan_defaulters_delinq.output.clean.path"]}')

write(df=delinq_transformed_df,\
      file_format="csv",\
      mode="overwrite",\
      partitionBy=None,\
      output_path=get_app_config("LOCAL")["loan_defaulters_delinq.output.clean.path"])

logger.info(f'Writing clean data to location {get_app_config("LOCAL")["loan_defaulters.output.clean.path"]}')

write(df=defaulters_transformed_df,\
      file_format="csv",\
      mode="overwrite",\
      partitionBy=None,\
      output_path=get_app_config("LOCAL")["loan_defaulters.output.clean.path"])


logger.info(f'Writing DQ rejected data to location {get_app_config("LOCAL")["loan_defaulters.output.bad.path"]}')
write(df=detailed_bad_df,\
      file_format="csv",\
      mode="overwrite",\
      partitionBy=None,\
      output_path=get_app_config("LOCAL")["loan_defaulters.output.bad.path"])