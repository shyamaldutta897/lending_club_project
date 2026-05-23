from configs.calculation_config import custom_spark_confs as params
from framework.session.spark_session import create_spark_session
from framework.config.config_reader import get_app_config
from pipelines.curated import read_processed_data as prd
from framework.writers.data_writer import write


from pipelines.curated.business_logics_implement import (
    create_ph_pts_view,
    create_ldh_ph_df_view,
    create_fh_ldh_ph_df_view,
    create_loan_score_view,
    loan_score_final_view
)

def run_pipeline():
    """
    Main orchestration pipeline for loan scoring and grading.
    
    This is the central execution entry point that coordinates:
    1. Spark session initialization and configuration loading
    2. Reading all pre-processed datasets
    3. Registering datasets as temporary SQL views
    4. Executing the three-dimensional scoring logic sequentially
    5. Calculating composite scores and assigning final grades
    6. Writing the complete graded dataset to output location
    
    The pipeline assumes that all input datasets have already been:
    - Read from raw data files
    - Validated for data quality
    - Transformed into clean, standardized formats
    - Written to the processed data directories
    
    This pipeline reads those processed datasets and applies the
    business logic to score and grade each loan application.
    """
    # 1. Initialize Spark session with environment-specific settings
    spark = create_spark_session('LOCAL')
    app_config = get_app_config('LOCAL')
    
    print("Reading processed datasets...")
    # 2. Load all processed datasets from processed data directory
    # (Previously validated and transformed by individual pipeline scripts)
    members_df = prd.read_members(spark=spark, config=app_config)
    loans_df = prd.read_loans(spark=spark, config=app_config)
    loans_repayment_df = prd.read_loan_repayment(spark=spark, config=app_config)
    loans_delinq_df = prd.read_delinquencies(spark=spark, config=app_config)
    loans_defaulters_df = prd.read_defaulters(spark=spark, config=app_config)

    print("Registering base temporary views...")
    # 3. Register all DataFrames as temporary SQL views for use in scoring queries
    prd.register_views(
        spark,
        members_df,
        loans_df,
        loans_repayment_df,
        loans_delinq_df,
        loans_defaulters_df
    )

    print("Executing business logic transformation layers...")
    # 4. Chain the scoring transformations sequentially
    # Each function creates a temp view that the next one depends on
    create_ph_pts_view(spark, params)                      # Payment history points
    create_ldh_ph_df_view(spark, params)                   # + Defaulter history points
    create_fh_ldh_ph_df_view(spark, params)                # + Financial health points
    create_loan_score_view(spark)                          # Composite scoring
    
    # 5. Retrieve the final scored and graded DataFrame
    final_df = loan_score_final_view(spark, params)
    
    print("Transformation complete. Sample output preview:")
    final_df.show(5)
    
    # 6. Write the complete graded dataset to output location
    destination_path = app_config["final_data.output.path"]
    print(f"Writing final dataset to: {destination_path}")
    write(df=final_df, file_format='parquet', mode='overwrite', partitionBy=None, output_path=destination_path)
    
    print("Pipeline executed successfully!")

# Entry point: ensures script only runs when directly executed, not when imported
if __name__ == "__main__":
    run_pipeline()