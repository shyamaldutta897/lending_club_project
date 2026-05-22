def get_loan_defaulters_schema():
    defaulters_schema = """
    member_id string,
    delinq_2_years string,
    delinq_amount float,
    public_record string,
    public_record_bankruptcies string,
    inquiry_last_6_months string,
    total_rec_late_fee string,
    months_since_last_delinq string,
    months_since_last_record string
    """
    return defaulters_schema