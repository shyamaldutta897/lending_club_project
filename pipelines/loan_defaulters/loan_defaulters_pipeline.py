from framework.readers.data_reader_generic import read
from framework.session.spark_session import create_spark_session
from framework.config.config_reader import get_app_config
from schemas.loan_defaulters_schema import get_loan_defaulters_schema
from framework.readers.read_options import get_read_options
from pipelines.loan_defaulters.transformations import clean_defaulters_data,clean_delinq_data
from framework.writers.data_writer import write
from framework.dq_checks.dq_orchestrator import apply_dq_for_table
from framework.scd.scd_logics import write_scd2
from pyspark.sql.functions import *
from framework.logger.logger_file import Log4j

spark=create_spark_session("LOCAL")

logger=Log4j(spark)

logger.info('Created Spark session')

logger.info(f'Reading data from location {get_app_config("LOCAL")["loan_defaulters.file.path"]}')

raw_file_format=get_app_config("LOCAL")["raw_file_format"]
processed_file_format=get_app_config("LOCAL")["processed_file_format"]

loan_defaulters_df=read(spark=spark,\
                format=raw_file_format,\
                file_path=get_app_config("LOCAL")["loan_defaulters.file.path"],\
                schema=get_loan_defaulters_schema(),\
                options=get_read_options(raw_file_format))



logger.info('Segregating bad data from raw files')
pk=["member_id"]
clean_df, bad_df, detailed_bad_df= apply_dq_for_table(loan_defaulters_df,
                                                      table_name="loan_defaulters",
                                                      key_columns=pk)

logger.info('Enabling SCD2')

#pk=['member_id']
tracking_cols=[col for col in clean_df.columns if col]
write_scd2(incoming_df=clean_df,
           primary_key=pk[0],
           target_location=get_app_config("LOCAL")["loan_defaulters.output.clean.path"],
           tracking_cols=tracking_cols
           )

logger.info(f'Clean data has been written to location {get_app_config("LOCAL")["loan_defaulters.output.clean.path"]}')


clean_df_scd2=read(spark=spark,
                   format=processed_file_format,
                   file_path=get_app_config("LOCAL")["loan_defaulters.output.clean.path"],
                   options=get_read_options(processed_file_format)
                   )

clean_df_current=clean_df_scd2.filter(col('is_current')==True)


logger.info('Separating defaulters data into delinquent data and defaulters data for processed layer')

delinq_transformed_df=clean_delinq_data(clean_df)
defaulters_transformed_df=clean_defaulters_data(clean_df)

logger.info(f'Writing processed data to location {get_app_config("LOCAL")["loan_defaulters_delinq.output.processed.path"]}')

write(df=delinq_transformed_df,\
      file_format="csv",\
      mode="overwrite",\
      partitionBy=None,\
      output_path=get_app_config("LOCAL")["loan_defaulters_delinq.output.processed.path"])

logger.info(f'Writing processed data to location {get_app_config("LOCAL")["loan_defaulters.output.processed.path"]}')

write(df=defaulters_transformed_df,\
      file_format="csv",\
      mode="overwrite",\
      partitionBy=None,\
      output_path=get_app_config("LOCAL")["loan_defaulters.output.processed.path"])


logger.info(f'Writing DQ rejected data to location {get_app_config("LOCAL")["loan_defaulters.output.bad.path"]}')

write(df=detailed_bad_df,\
      file_format="csv",\
      mode="overwrite",\
      partitionBy=None,\
      output_path=get_app_config("LOCAL")["loan_defaulters.output.bad.path"])