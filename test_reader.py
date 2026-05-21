from framework.session.spark_session import create_spark_session
from schemas.members_schema import get_members_schema
from framework.config.config_reader import get_app_config
from framework.readers.data_reader import read_members_data
from framework.readers.data_reader_generic import read
from framework.readers.read_options import get_read_options

spark = create_spark_session("LOCAL")
file_path = get_app_config("LOCAL")["members.file.path"]
schema=get_members_schema()
options=get_read_options("csv")

df = read(spark, "csv", file_path, schema, options)

df.show(1)
df.printSchema()


