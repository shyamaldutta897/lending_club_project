from framework.readers.data_reader_generic import read
from framework.writers.data_writer import write 
from delta import DeltaTable
from pyspark.sql.functions import *
from framework.session.spark_session import create_spark_session
from framework.readers.read_options import get_read_options
from framework.config.config_reader import get_app_config
import os 

def write_scd2(incoming_df,primary_key,target_location,tracking_cols):
    """
    Write incoming data to a Delta table using Slowly Changing Dimension Type 2 logic.

    Args:
        incoming_df (DataFrame): New source data that needs to be loaded into the target table
        primary_key (str): Column name that uniquely identifies a business record
        target_location (str): Delta table storage location
        tracking_cols (list): Tracking columns input

    Returns:
        None

    The function:
    1. Resolves the absolute target table path
    2. Creates a Spark session for local execution
    3. Initializes the Delta table if it does not already exist
    4. Reads the current target data when the table already exists
    5. Builds a staging dataset containing new records and expired old records
    6. Applies MERGE INTO to update old current rows and insert new current rows

    SCD Type 2 columns:
        - valid_from: Timestamp from which the record version is valid
        - valid_to: Timestamp until which the record version is valid
        - is_current: Flag to identify the latest active record version
    """
    absolute_target_path=os.path.abspath(target_location)
    
    spark=create_spark_session('LOCAL')

    if not DeltaTable.isDeltaTable(spark,absolute_target_path):
        print(f'Initializing table at {absolute_target_path}')
        # Add SCD metadata columns while creating the table for the first load.
        # valid_to is converted to timestamp since Parquet drops NullType fields.
        df=incoming_df.withColumn('valid_from',current_timestamp())\
                      .withColumn('valid_to',lit(None).cast('timestamp'))\
                      .withColumn('is_current',lit(True))\
                      .withColumn('is_deleted_in_source',lit(False))
                      

        write(df,'delta','overwrite',None,absolute_target_path)

        return # End the function after initial table creation.
    
    print(f'Applying MERGE INTO to {absolute_target_path}')

    file_format="delta"
    read_options=get_read_options(file_format)

    # Read target data and expose only current records for comparison.
    target_df=read(spark,"delta",absolute_target_path,None,read_options)
    target_df.filter(col('is_current')==True).createOrReplaceTempView('target_view')

    # Register incoming data so the staging SQL can compare source and target rows.
    incoming_df.createOrReplaceTempView('incoming_view')

    # Exclude SCD metadata columns from business-column comparison.
    exclude_cols=['valid_from','valid_to','is_current','is_deleted_in_source']
    tracking_cols=[c for c in tracking_cols if c not in exclude_cols]
    

    # Build reusable SQL fragments for source/target column selection and change detection.
    target_cols=','.join([f"target.{c}" for c in tracking_cols])
    incoming_cols=','.join([f"incoming.{c}" for c in tracking_cols])
    change_condition=" OR ".join([f"incoming.{col} != target.{col}" for col in tracking_cols])

    # Define insert column/value lists for the MERGE INTO statement.
    all_columns=[*tracking_cols,'valid_from','valid_to','is_current','is_deleted_in_source']
    insert_columns=",".join(all_columns)
    insert_values=",".join(f"source.{c}" for c in all_columns)

    print(f'scd tracking cols:{tracking_cols}')

    # Stage three kinds of rows:
    # 1. New current rows for brand-new records or changed records.
    # 2. Old current rows that should be expired when a change is detected.
    # 3. Rows that are deleted in source but exists in target
    # 3. For changed records setting primary key as null so that they can be appended along with the new records
    # 4. For old records keeping the primary key available so that using merge into these records can be updated.
    # 5. 1st part is for new and changed records, 2nd part for old records in target, 3rd part for deleted records in source that exist in target.
    staging_query=f"""
                SELECT {incoming_cols}, 
                current_timestamp() as valid_from,
                NULL as valid_to,
                true as is_current,
                False as is_deleted_in_source,
                CASE WHEN
                    incoming.{primary_key} IS NOT NULL THEN NULL 
                    ELSE incoming.{primary_key} END  as merge_key
                FROM incoming_view incoming
                LEFT JOIN target_view target on
                incoming.{primary_key}=target.{primary_key}
                WHERE target.{primary_key} IS NULL or ({change_condition})

                UNION ALL

                SELECT {target_cols},
                target.valid_from,
                current_timestamp() as valid_to,
                False as is_current,
                False as is_deleted_in_source,
                target.{primary_key} as merge_key
                FROM target_view target
                LEFT JOIN incoming_view incoming ON
                target.{primary_key}=incoming.{primary_key}
                WHERE target.is_current=True
                AND ({change_condition})
                AND incoming.{primary_key} IS NOT NULL

                UNION ALL

                SELECT {target_cols},
                target.valid_from,
                current_timestamp() as valid_to,
                False as is_current,
                True as is_deleted_in_source,
                CASE WHEN
                    target.{primary_key} IS NOT NULL THEN NULL 
                    ELSE target.{primary_key} END  as merge_key
                FROM target_view target
                LEFT JOIN incoming_view incoming 
                ON target.{primary_key}=incoming.{primary_key}
                WHERE target.is_current=True
                AND incoming.{primary_key} IS NULL

                """
    
    staging_df=spark.sql(staging_query)
    # Persist the staging output for debugging or downstream audit checks.
    write(staging_df,'parquet','overwrite',None,get_app_config("LOCAL")['scd.staging.output.file.path'])
    
    # Use the staged rows as the source for Delta MERGE.
    spark.sql(staging_query).createOrReplaceTempView('final_view')
    merge_df=spark.sql(f"""
                MERGE INTO delta.`{absolute_target_path}` AS target
                USING final_view as source
                on target.{primary_key}=source.merge_key
                AND target.is_current=true

                WHEN MATCHED THEN
                UPDATE SET
                target.valid_to=current_timestamp(),
                target.is_current=false,
                target.is_deleted_in_source=false

                WHEN NOT MATCHED THEN
                INSERT({insert_columns})
                VALUES({insert_values})

              """)


   
    
    




    
