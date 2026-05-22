from framework.readers.data_reader_generic import read
from framework.session.spark_session import create_spark_session
from framework.config.config_reader import get_app_config
from schemas.loans_schema import get_loan_details_schema
from framework.readers.read_options import get_read_options
from pipelines.loans.transformations import clean_loan_details
from framework.writers.data_writer import write
from framework.dq_checks.dq_orchestrator import apply_dq_for_table

spark=create_spark_session("LOCAL")

loan_details_df=read(spark,\
               'csv',\
                get_app_config("LOCAL")["loans.file.path"],\
                get_loan_details_schema(),\
                get_read_options("csv"))

clean_df, bad_df, detailed_bad_df = apply_dq_for_table(
    loan_details_df,
    table_name="loan_details",
    key_columns=["id"]
)

raw=loan_details_df.count()
clean=clean_df.count()
bad=bad_df.count()

print(f'raw count:{raw}, clean_count:{clean}, bad_count:{bad}')

loan_details_transformed_df=clean_loan_details(loan_details_df)

write(df=loan_details_transformed_df,\
      file_format="csv",\
      mode="overwrite",\
      partitionBy=None,\
      output_path=get_app_config("LOCAL")["loans.output.clean.path"])

write(df=detailed_bad_df,\
      file_format="csv",\
      mode="overwrite",\
      partitionBy=None,\
      output_path=get_app_config("LOCAL")["loans.output.bad.path"])