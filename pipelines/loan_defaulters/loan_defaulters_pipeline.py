from framework.readers.data_reader_generic import read
from framework.session.spark_session import create_spark_session
from framework.config.config_reader import get_app_config
from schemas.loan_defaulters_schema import get_loan_defaulters_schema
from framework.readers.read_options import get_read_options
from pipelines.loan_defaulters.transformations import clean_defaulters_data,clean_delinq_data
from framework.writers.data_writer import write
from framework.dq_checks.dq_orchestrator import apply_dq_for_table

spark=create_spark_session("LOCAL")

loan_defaulters_df=read(spark,\
               'csv',\
                get_app_config("LOCAL")["loan_defaulters.file.path"],\
                get_loan_defaulters_schema(),\
                get_read_options("csv"))



clean_df, bad_df, detailed_bad_df= apply_dq_for_table(loan_defaulters_df,
                                                      table_name="loan_defaulters",
                                                      key_columns=None)


delinq_transformed_df=clean_delinq_data(clean_df)
defaulters_transformed_df=clean_defaulters_data(clean_df)

write(df=delinq_transformed_df,\
      file_format="csv",\
      mode="overwrite",\
      partitionBy=None,\
      output_path=get_app_config("LOCAL")["loan_defaulters_delinq.output.clean.path"])

write(df=defaulters_transformed_df,\
      file_format="csv",\
      mode="overwrite",\
      partitionBy=None,\
      output_path=get_app_config("LOCAL")["loan_defaulters.output.clean.path"])

write(df=detailed_bad_df,\
      file_format="csv",\
      mode="overwrite",\
      partitionBy=None,\
      output_path=get_app_config("LOCAL")["loan_defaulters.output.bad.path"])