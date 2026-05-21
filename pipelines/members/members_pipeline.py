from framework.readers.data_reader_generic import read
from framework.session.spark_session import create_spark_session
from framework.config.config_reader import get_app_config
from schemas.members_schema import get_members_schema
from framework.readers.read_options import get_read_options
from pipelines.members.transformations import clean_members_data
from framework.writers.data_writer import write

spark=create_spark_session("LOCAL")

member_df=read(spark,\
               'csv',\
                get_app_config("LOCAL")["members.file.path"],\
                get_members_schema(),\
                get_read_options("csv"))

member_transformed_df=clean_members_data(member_df)

write(df=member_transformed_df,\
      file_format="csv",\
      mode="overwrite",\
      partitionBy=None,\
      output_path=get_app_config("LOCAL")["members.output.path"])