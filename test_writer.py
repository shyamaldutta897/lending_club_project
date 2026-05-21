from framework.session.spark_session import create_spark_session
from framework.writers.data_writer import write
from framework.readers.data_reader import read_members_data
from framework.config.config_reader import get_app_config, get_pyspark_config

spark = create_spark_session("LOCAL")

members_df = read_members_data(spark, "LOCAL")

output_path = get_app_config("LOCAL")["members.output.path"]
  
write(members_df, "csv","overwrite",None, output_path)