from framework.session.spark_session import create_spark_session
from framework.writers.data_writer import write
from framework.config.config_reader import get_app_config, get_pyspark_config

from framework.session.spark_session import create_spark_session
from schemas.members_schema import get_members_schema
from framework.config.config_reader import get_app_config
from framework.readers.data_reader_generic import read
from framework.readers.read_options import get_read_options

from framework.dq_checks import dq_logics

from pyspark.sql.functions import *


spark = create_spark_session("LOCAL")
file_path = get_app_config("LOCAL")["members.file.path"]
schema=get_members_schema()
options=get_read_options("csv")

df = read(spark, "csv", file_path, schema, options)

# df_check=df.select('member_id', 'delinq_2_years')\
#            .withColumn('delinq_2yrs_check',
#                        when(col('delinq_2_years').isNull(), 'actual null')\
#                        .when (col('delinq_2_years').cast('int').isNull(), 'garbage value')
#                        .otherwise('all good'))

rule={'rule_id': 'R5', 
'columns': ['zip_code'], 
'type': 'zip_code_check', 
'check': 'Accpeted length of field value is 5', 
'threshold': 0.98}

df_dq_check=dq_logics.get_failed_rows_for_rule(df,rule)

output_path = get_app_config("LOCAL")["test.output.file.path"]

#df_dq_check.show(5)

write(df_dq_check, "csv","overwrite",None, output_path)