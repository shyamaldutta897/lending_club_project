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
    # 1. Initialize local Spark session and configurations
    spark = create_spark_session('LOCAL')
    app_config = get_app_config('LOCAL')
    
    print("Reading processed datasets...")
    # 2. Extract DataFrames ONCE here in the central execution pipeline
    members_df = prd.read_members(spark=spark, config=app_config)
    loans_df = prd.read_loans(spark=spark, config=app_config)
    loans_repayment_df = prd.read_loan_repayment(spark=spark, config=app_config)
    loans_delinq_df = prd.read_delinquencies(spark=spark, config=app_config)
    loans_defaulters_df = prd.read_defaulters(spark=spark, config=app_config)

    print("Registering base temporary views...")
    # 3. Pass the loaded DataFrames directly into your catalog registration manager
    prd.register_views(
        spark,
        members_df,
        loans_df,
        loans_repayment_df,
        loans_delinq_df,
        loans_defaulters_df
    )

    print("Executing business logic transformation layers...")
    # 4. Chain the view calculations sequentially
    # Each function registers an internal TempView that the next step relies on
    create_ph_pts_view(spark, params)
    create_ldh_ph_df_view(spark, params)
    create_fh_ldh_ph_df_view(spark, params)
    create_loan_score_view(spark)
    
    # 5. Capture the final calculated target DataFrame
    final_df = loan_score_final_view(spark, params)
    
    print("Transformation complete. Sample output preview:")
    final_df.show(5)
    
    # 6. Extract target path and write data out safely
    destination_path = app_config["final_data.output.path"]
    print(f"Writing final dataset to: {destination_path}")
    write(df=final_df,file_format='parquet',mode='overwrite',partitionBy=None,output_path=destination_path)
    
    print("Pipeline executed successfully!")

# Protected main entry point ensures clean framework behavior and safe importing
if __name__ == "__main__":
    run_pipeline()