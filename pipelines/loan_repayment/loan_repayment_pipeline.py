from framework.readers.data_reader_generic import read
from framework.session.spark_session import create_spark_session
from framework.config.config_reader import get_app_config
from schemas.loan_repayment_schema import get_loan_repayment_schema
from framework.readers.read_options import get_read_options
from pipelines.loan_repayment.transformations import clean_loan_repayment
from framework.writers.data_writer import write
from framework.dq_checks.dq_orchestrator import apply_dq_for_table

spark=create_spark_session("LOCAL")

loan_repayment_df=read(spark,\
               'csv',\
                get_app_config("LOCAL")["loan_repayment.file.path"],\
                get_loan_repayment_schema(),\
                get_read_options("csv"))

clean_df, bad_df, detailed_bad_df = apply_dq_for_table(
    loan_repayment_df,
    table_name="loan_repayment",
    key_columns=["id"]
)

loan_repayment_transformed_df=clean_loan_repayment(loan_repayment_df)

write(df=loan_repayment_transformed_df,\
      file_format="csv",\
      mode="overwrite",\
      partitionBy=None,\
      output_path=get_app_config("LOCAL")["loan_repayment.output.clean.path"])

write(df=detailed_bad_df,\
      file_format="csv",\
      mode="overwrite",\
      partitionBy=None,\
      output_path=get_app_config("LOCAL")["loan_repayment.output.bad.path"])