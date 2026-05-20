from framework.utils.spark_session import create_spark_session
from schemas.members_schema import get_members_schema
from framework.config.config_reader import get_app_config
from framework.readers.data_reader import read_members_data

spark = create_spark_session("LOCAL")

df = read_members_data(spark, "LOCAL")

df.show(1)