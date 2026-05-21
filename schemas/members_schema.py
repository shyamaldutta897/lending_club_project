
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