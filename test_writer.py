from framework.session.spark_session import create_spark_session
from framework.writers.data_writer import write
from framework.config.config_reader import get_app_config, get_pyspark_config

from framework.session.spark_session import create_spark_session
from schemas.loan_defaulters_schema import get_loan_defaulters_schema
from framework.config.config_reader import get_app_config
from framework.readers.data_reader_generic import read
from framework.readers.read_options import get_read_options

from pyspark.sql.functions import *


spark = create_spark_session("LOCAL")
file_path = get_app_config("LOCAL")["loan_defaulters.file.path"]
schema=get_loan_defaulters_schema()
options=get_read_options("csv")

df = read(spark, "csv", file_path, schema, options)

df_check=df.select('member_id_custom', 'delinq_2_years')\
           .withColumn('delinq_2yrs_check',
                       when(col('delinq_2_years').isNull(), 'actual null')\
                       .when (col('delinq_2_years').cast('int').isNull(), 'garbage value')
                       .otherwise('all good'))

output_path = get_app_config("LOCAL")["test.output.file.path"]
  
write(df_check, "csv","overwrite",None, output_path)