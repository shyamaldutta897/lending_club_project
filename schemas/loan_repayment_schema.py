def get_loan_repayment_schema():
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