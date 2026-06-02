def get_members_schema():
    """
    Define the schema for member profile data.
    
    Returns:
        str: Spark SQL schema string defining member detail structure
        
    This schema captures comprehensive member information including:
    - Identification and employment details
    - Income and address information
    - Credit assessment (grades) and verification status
    - Credit limits and application type
    """
    member_schema = """
        member_id STRING,
        emp_title STRING,
        emp_length STRING,
        home_ownership STRING,
        annual_income FLOAT,
        address_state STRING,
        zip_code STRING,
        address_country STRING,
        grade STRING,
        sub_grade STRING,
        verification_status STRING,
        total_high_credit_limit FLOAT,
        application_type STRING,
        join_annual_income FLOAT,
        verification_status_joint STRING
    """
    return member_schema
