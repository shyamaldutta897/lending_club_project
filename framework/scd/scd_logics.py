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

    # Exclude SCD metadata columns from business-column comparison.
    exclude_cols=['valid_from','valid_to','is_current','is_deleted_in_source','row_hash','sk_id']
    tracking_cols=[c for c in tracking_cols if c not in exclude_cols]

    #Creating a hash value combining all tracking fields, so that there is no long chain of fields comparing
    #like incoming.col_a!=target.col or incoming.col_b!=target.col_b etc
    #Creates one unique hash id concatenating all values in a row
    incoming_df_hashed=incoming_df.withColumn('row_hash',md5(concat_ws('|',*tracking_cols)))

    if not DeltaTable.isDeltaTable(spark,absolute_target_path):
        print(f'Initializing table at {absolute_target_path}')
        # Add SCD metadata columns while creating the table for the first load.
        # valid_to is converted to timestamp since Parquet drops NullType fields.
        df=incoming_df_hashed.withColumn('valid_from',current_timestamp())\
                      .withColumn('valid_to',lit(None).cast('timestamp'))\
                      .withColumn('is_current',lit(True))\
                      .withColumn('is_deleted_in_source',lit(False))\
                      .withColumn('sk_id',md5(concat_ws('|',*primary_key,col('valid_from'))))
                      #Added a surrogate key - as a best practice for next phase of processing. \
                      #Start with concatenating value of pk and valid_from(date)

        write(df,'delta','overwrite',None,absolute_target_path)

        return # End the function after initial table creation.
    
    print(f'Applying MERGE INTO to {absolute_target_path}')

    file_format="delta"
    read_options=get_read_options(file_format)

    # Read target data and expose only current records for comparison.
    target_df=read(spark,"delta",absolute_target_path,None,read_options)
    target_df.filter(col('is_current')==True).createOrReplaceTempView('target_view')

    # Register incoming data so the staging SQL can compare source and target rows.
    incoming_df_hashed.createOrReplaceTempView('incoming_view')



    

    # # Build reusable SQL fragments for source/target column selection and change detection.
    target_cols=','.join([f"target.{c}" for c in tracking_cols])
    incoming_cols=','.join([f"incoming.{c}" for c in tracking_cols])
    # change_condition=" OR ".join([f"incoming.{col} != target.{col}" for col in tracking_cols])

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
    
    pk=','.join(primary_key)
    staging_query=f"""
                SELECT {incoming_cols}, 
                --Here we are changing the surrogate key with latest timestamp
                md5(concat_ws('|',incoming.{pk},current_timestamp())) as sk_id,
                incoming.row_hash,
                current_timestamp() as valid_from,
                NULL as valid_to,
                true as is_current,
                False as is_deleted_in_source,
                NULL as merge_key
                FROM incoming_view incoming
                LEFT JOIN target_view target on
                incoming.{pk}=target.{pk}
                WHERE target.{pk} IS NULL 
                or target.row_hash !=incoming.row_hash

                UNION ALL

                SELECT {target_cols},
                target.sk_id,
                target.row_hash,
                target.valid_from,
                current_timestamp() as valid_to,
                False as is_current,
                False as is_deleted_in_source,
                target.{pk} as merge_key
                FROM target_view target
                LEFT JOIN incoming_view incoming ON
                target.{pk}=incoming.{pk}
                WHERE  target.row_hash!=incoming.row_hash
                AND incoming.{pk} IS NOT NULL

                UNION ALL

                SELECT {target_cols},
                target.sk_id,
                target.row_hash,
                target.valid_from,
                current_timestamp() as valid_to,
                False as is_current,
                True as is_deleted_in_source,
                target.{pk} as merge_key
                FROM target_view target
                LEFT JOIN incoming_view incoming 
                ON target.{pk}=incoming.{pk}
                WHERE target.is_current=True
                AND incoming.{pk} IS NULL

                """
    
    staging_df=spark.sql(staging_query)
    # Persist the staging output for debugging or downstream audit checks.
    write(staging_df,'csv','overwrite',None,get_app_config("LOCAL")['scd.staging.output.file.path'])
    
    # Use the staged rows as the source for Delta MERGE.
    spark.sql(staging_query).createOrReplaceTempView('final_view')
    merge_df=spark.sql(f"""
                MERGE INTO delta.`{absolute_target_path}` AS target
                USING final_view as source
                on target.{pk}=source.merge_key
                AND target.is_current=true

                WHEN MATCHED THEN
                UPDATE SET
                target.valid_to=source.valid_to,
                target.is_current=source.is_current,
                target.is_deleted_in_source=source.is_deleted_in_source

                WHEN NOT MATCHED THEN
                INSERT({insert_columns})
                VALUES({insert_values})

              """)


   
    
    




    
