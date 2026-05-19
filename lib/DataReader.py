import os
import sys

# Get the absolute path of the directory where THIS script lives
script_dir = os.path.dirname(os.path.abspath(__file__))
# Add the project root to sys.path so the top-level 'lib' package is importable
project_root = os.path.abspath(os.path.join(script_dir, os.pardir))
sys.path.insert(0, project_root)

from lib import ConfigReader
from utils import get_spark_session


def get_members_schema():
    member_schema = """
        member_id STRING,
        emp_title STRING,
        emp_length STRING,
        home_ownership STRING,
        annual_income FLOAT,
        address_state STRING,
        address_country STRING,
        zip_code STRING,
        grade STRING,
        sub_grade STRING,
        verification_status STRING,
        total_high_credit_limit FLOAT,
        application_type STRING,
        join_annual_income FLOAT,
        verification_status_joint STRING
    """
    return member_schema

def read_members_data(spark, env):
    conf = ConfigReader.get_app_config(env)
    members_file_path = conf["members.file.path"]
    return spark.read\
                .format("csv")\
                .schema(get_members_schema())\
                .option("header", "true")\
                .load(members_file_path)


def loan_details_schema():
    loan_schema = """id STRING,
    member_id STRING,
    loan_amount FLOAT,
    funded_amount FLOAT,
    term STRING,
    interest_rate FLOAT,
    installment FLOAT,
    issue_month_year STRING,
    loan_status STRING,
    purpose STRING,
    title STRING"""

    return loan_schema


def read_loan_details(spark, env):
    conf = ConfigReader.get_app_config(env)
    loan_file_path = conf["loans.file.path"]
    return spark.read\
                .format("csv")\
                .schema(loan_details_schema())\
                .option("header", "true")\
                .load(loan_file_path)
   


def loan_repayment_schema():
    repayment_schema = """
    id STRING,
    total_principal_received FLOAT,
    total_interest_received FLOAT,
    total_late_fee_received FLOAT,
    total_payment FLOAT,
    last_payment_amount FLOAT,
    last_payment_date STRING,
    next_payment_date STRING"""
   
    return repayment_schema

def read_loan_repayment(spark, env):
    conf = ConfigReader.get_app_config(env)
    loan_repayment_file_path = conf["loan_repayment.file.path"]
    return spark.read\
                .format("csv")\
                .schema(loan_repayment_schema())\
                .option("header", "true")\
                .load(loan_repayment_file_path)


def loan_defaulters_schema():
    defaulters_schema = """
    member_id_custom STRING,
    delinq_2_years INT,
    delinq_amount FLOAT,
    public_record INT,
    public_record_bankruptcies INT,
    inquiry_last_6_months INT,
    total_rec_late_fee STRING,
    months_since_last_delinq FLOAT,
    months_since_last_record FLOAT
    """
    return defaulters_schema

def read_loan_defaulters(spark, env):
    conf = ConfigReader.get_app_config(env)
    loan_defaulters_file_path = conf["loan_defaulters.file.path"]
    return spark.read\
                .format("csv")\
                .schema(loan_defaulters_schema())\
                .option("header", "true")\
                .load(loan_defaulters_file_path)



