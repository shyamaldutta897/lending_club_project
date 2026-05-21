def get_loan_defaulters_schema():
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