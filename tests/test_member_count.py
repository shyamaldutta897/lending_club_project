import pytest
from configs.calculation_config import custom_spark_confs as params
from framework.session.spark_session import create_spark_session
from framework.config.config_reader import get_app_config
from pipelines.curated import read_processed_data as prd
from pyspark.sql.functions import *


from pipelines.curated.business_logics_implement import (
    create_ph_pts_view,
    create_ldh_ph_df_view,
    create_fh_ldh_ph_df_view,
    create_loan_score_view,
    loan_score_final_view
)


def test_loan_score_final_member_counts(spark):

    expected_counts = {
        'F': 4836,
        'E': 47432,
        'B': 60649,
        'D': 561792,
        'C': 1576758,
        None:4071
        }

    app_params = {
        'lending_project.very_good_grade_pts': 90,
        'lending_project.good_grade_pts': 80,
        'lending_project.bad_grade_pts': 60,
        'lending_project.very_bad_grade_pts': 40,
        'lending_project.unacceptable_grade_pts': 20
        }

    app_config = get_app_config('LOCAL')
    

    members_df = prd.read_members(spark=spark, config=app_config)
    loans_df = prd.read_loans(spark=spark, config=app_config)
    loans_repayment_df = prd.read_loan_repayment(spark=spark, config=app_config)
    loans_delinq_df = prd.read_delinquencies(spark=spark, config=app_config)
    loans_defaulters_df = prd.read_defaulters(spark=spark, config=app_config)

    prd.register_views(
        spark,
        members_df,
        loans_df,
        loans_repayment_df,
        loans_delinq_df,
        loans_defaulters_df
    )

    create_ph_pts_view(spark, params)
    create_ldh_ph_df_view(spark, params)
    create_fh_ldh_ph_df_view(spark, params)
    create_loan_score_view(spark)

    final_df = loan_score_final_view(spark, params)

    test_df=final_df.groupBy('loan_final_grade').agg(count('member_id').alias('member_count'))

    actual_counts={}

    for row in test_df.collect():
        actual_counts[row['loan_final_grade']]=row['member_count']
    
    for grade,expected_count in expected_counts.items():
        actual_count=actual_counts[grade]
        assert actual_count==expected_count, f'mismatch for grade : {grade}. Expected count : {expected_count}, got {actual_count}'






test_loan_score_final_member_counts(create_spark_session('LOCAL'))


