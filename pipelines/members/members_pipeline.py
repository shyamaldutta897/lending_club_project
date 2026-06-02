from framework.readers.data_reader_generic import read
from framework.session.spark_session import create_spark_session
from framework.config.config_reader import get_app_config
from schemas.members_schema import get_members_schema
from framework.readers.read_options import get_read_options
from pipelines.members.transformations import clean_members_data
from framework.writers.data_writer import write
from framework.dq_checks.dq_orchestrator import apply_dq_for_table
from framework.logger.logger_file import Log4j
from framework.scd.scd_logics import write_scd2
from pyspark.sql.functions import *

# Initialize Spark session for data processing
spark = create_spark_session("LOCAL")

logger=Log4j(spark)

logger.info('Created Spark session')

logger.info(f'Reading data from loacation {get_app_config("LOCAL")["members.file.path"]}')

# Load raw member data with schema validation
file_format='csv'
member_df = read(spark=spark,
                 format=file_format,
                 file_path=get_app_config("LOCAL")["members.file.path"],
                 schema=get_members_schema(),
                 options=get_read_options(file_format))



logger.info('Segregating bad data from raw files')
# Apply data quality checks to identify valid and invalid records
# Returns three DataFrames: clean (passed QC), bad (failed QC), detailed_bad (failures with reason)
pk=["member_id"]
clean_df, bad_df, detailed_bad_df = apply_dq_for_table(
    member_df,
    table_name="member_details",
    key_columns=pk
)

logger.info('Enabling SCD2')

tracking_cols=[col for col in clean_df.columns if col]
write_scd2(incoming_df=clean_df,
           primary_key=pk[0],
           target_location=get_app_config("LOCAL")["members.output.clean.path"],
           tracking_cols=tracking_cols
           )
logger.info(f'Clean data has been written to location {get_app_config("LOCAL")["members.output.clean.path"]}')

file_format='delta'
clean_df_scd2=read(spark=spark,
                   format=file_format,
                   file_path=get_app_config("LOCAL")["members.output.clean.path"],
                   options=get_read_options(file_format)
                   )

clean_df_current=clean_df_scd2.filter(col('is_current')==True)

logger.info('Preparing data for processed layer')
# Transform clean data into standardized format
member_transformed_df = clean_members_data(clean_df_current)

logger.info(f'Writing data to processed location {get_app_config("LOCAL")["members.output.processed.path"]}')
file_format='delta'
write(df=member_transformed_df,
      file_format=file_format,
      mode='overwrite',
      partitionBy=None,
      output_path=get_app_config("LOCAL")["members.output.processed.path"]
      )

logger.info(f'Writing DQ rejected data to location {get_app_config("LOCAL")["members.output.bad.path"]}')
# Archive failed records for manual investigation and remediation
write(df=detailed_bad_df,
      file_format=file_format,
      mode="overwrite",
      partitionBy=None,
      output_path=get_app_config("LOCAL")["members.output.bad.path"])