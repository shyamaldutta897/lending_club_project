from framework.config.config_reader import get_app_config
from framework.utils.spark_session import create_spark_session

from schemas.members_schema import get_members_schema
from schemas.loans_schema import get_loan_details_schema
from schemas.loan_repayment_schema import get_loan_repayment_schema
from schemas.loan_defaulters_schema import get_loan_defaulters_schema

def read_members_data(spark, env):

    conf = get_app_config(env)

    members_file_path = conf["members.file.path"]

    return spark.read \
        .format("csv") \
        .schema(get_members_schema()) \
        .option("header", "true") \
        .load(members_file_path)

def read_loans_data(spark, env):

    conf = get_app_config(env)

    loans_file_path = conf["loans.file.path"]

    return spark.read \
        .format("csv") \
        .schema(get_loan_details_schema()) \
        .option("header", "true") \
        .load(loans_file_path)

def read_loan_repayments_data(spark, env):

    conf = get_app_config(env)

    repayments_file_path = conf["loan_repayment.file.path"]

    return spark.read \
        .format("csv") \
        .schema(get_loan_repayment_schema()) \
        .option("header", "true") \
        .load(repayments_file_path)

def read_loan_defaulters_data(spark, env):

    conf = get_app_config(env)

    defaulters_file_path = conf["loan_defaulters.file.path"]

    return spark.read \
        .format("csv") \
        .schema(get_loan_defaulters_schema()) \
        .option("header", "true") \
        .load(defaulters_file_path)

