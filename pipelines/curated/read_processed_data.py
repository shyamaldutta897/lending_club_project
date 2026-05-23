from framework.readers.data_reader_generic import read
from framework.readers.read_options import get_read_options
from framework.config.config_reader import get_app_config
from framework.session.spark_session import create_spark_session
from schemas import members_schema, loans_schema, loan_repayment_schema

# Do not create a Spark session at import time; callers must pass `spark`.

def read_members(spark, config):
    """
    Read processed member profile data.
    
    Args:
        spark: Spark session (created by caller)
        config: Configuration dictionary with file paths
        
    Returns:
        DataFrame: Member profiles ready for scoring
    """
    return read(
        spark,
        "csv",
        config["members.output.clean.path"],
        members_schema.get_members_schema(),
        get_read_options("csv"),
    )


def read_loans(spark, config):
    """
    Read processed loan detail data.
    
    Args:
        spark: Spark session (created by caller)
        config: Configuration dictionary with file paths
        
    Returns:
        DataFrame: Loan details ready for scoring
    """
    return read(
        spark,
        "csv",
        config["loans.output.clean.path"],
        loans_schema.get_loan_details_schema(),
        get_read_options("csv"),
    )


def read_loan_repayment(spark, config):
    """
    Read processed loan repayment and payment history data.
    
    Args:
        spark: Spark session (created by caller)
        config: Configuration dictionary with file paths
        
    Returns:
        DataFrame: Payment history ready for scoring
    """
    return read(
        spark,
        "csv",
        config["loan_repayment.output.clean.path"],
        loan_repayment_schema.get_loan_repayment_schema(),
        get_read_options("csv"),
    )


def read_delinquencies(spark, config):
    """
    Read processed delinquency information data.
    
    Args:
        spark: Spark session (created by caller)
        config: Configuration dictionary with file paths
        
    Returns:
        DataFrame: Delinquency metrics ready for scoring
    """
    schema = "member_id string, delinq_2_years int, delinq_amount float, months_since_last_delinq int"
    return read(
        spark,
        "csv",
        config["loan_defaulters_delinq.output.clean.path"],
        schema,
        get_read_options("csv"),
    )


def read_defaulters(spark, config):
    """
    Read processed defaulter and credit risk indicator data.
    
    Args:
        spark: Spark session (created by caller)
        config: Configuration dictionary with file paths
        
    Returns:
        DataFrame: Credit risk indicators ready for scoring
    """
    schema = """
        member_id string,
        public_record int,
        public_record_bankruptcies int,
        inquiry_last_6_months int,
        months_since_last_delinq int,
        months_since_last_record int
    """
    return read(
        spark,
        "csv",
        config["loan_defaulters_delinq.output.clean.path"],
        schema,
        get_read_options("csv"),
    )


def register_views(spark, members_df, loans_df, loan_repayment_df, delinq_df, defaulters_df):
    """
    Register all DataFrames as temporary SQL views for use in scoring queries.
    
    This makes the data available for SQL-based transformations in the
    scoring engine without needing to manipulate DataFrames directly.
    
    Args:
        spark: Spark session
        members_df: Member profiles DataFrame
        loans_df: Loan details DataFrame
        loan_repayment_df: Payment history DataFrame
        delinq_df: Delinquency metrics DataFrame
        defaulters_df: Credit risk indicators DataFrame
    """
    members_df.createOrReplaceTempView("members")
    loans_df.createOrReplaceTempView("loans")
    loan_repayment_df.createOrReplaceTempView("loan_repayment")
    delinq_df.createOrReplaceTempView("delinq_details")
    defaulters_df.createOrReplaceTempView("defaulter_details")
