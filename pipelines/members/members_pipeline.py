from framework.readers.data_reader_generic import read
from framework.session.spark_session import create_spark_session
from framework.config.config_reader import get_app_config
from schemas.members_schema import get_members_schema
from framework.readers.read_options import get_read_options
from pipelines.members.transformations import clean_members_data
from framework.writers.data_writer import write
from framework.dq_checks.dq_orchestrator import apply_dq_for_table

# Initialize Spark session for data processing
spark = create_spark_session("LOCAL")

# Load raw member data with schema validation
member_df = read(spark,
                 'csv',
                 get_app_config("LOCAL")["members.file.path"],
                 get_members_schema(),
                 get_read_options("csv"))

# Apply data quality checks to identify valid and invalid records
# Returns three DataFrames: clean (passed QC), bad (failed QC), detailed_bad (failures with reason)
clean_df, bad_df, detailed_bad_df = apply_dq_for_table(
    member_df,
    table_name="member_details",
    key_columns=["member_id"]
)

# Transform clean data into standardized format
member_transformed_df = clean_members_data(clean_df)

# Write processed data to output location (clean records)
write(df=member_transformed_df,
      file_format="csv",
      mode="overwrite",
      partitionBy=None,
      output_path=get_app_config("LOCAL")["members.output.clean.path"])

# Archive failed records for manual investigation and remediation
write(df=detailed_bad_df,
      file_format="csv",
      mode="overwrite",
      partitionBy=None,
      output_path=get_app_config("LOCAL")["members.output.bad.path"])