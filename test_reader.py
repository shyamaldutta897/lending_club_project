from webbrowser import get

from framework.session.spark_session import create_spark_session
from schemas.loan_defaulters_schema import get_loan_defaulters_schema
from framework.config.config_reader import get_app_config
from framework.readers.data_reader_generic import read
from framework.readers.read_options import get_read_options
from pyspark.sql.functions import *

spark = create_spark_session("LOCAL")
file_path = get_app_config("LOCAL")["final_data.output.path"]
options=get_read_options("csv")

df = read(spark, "parquet", file_path, schema=None, options=options)

df=df.groupBy('loan_final_grade').agg(count('member_id').alias('member_total'))
df.show()
df.printSchema()


