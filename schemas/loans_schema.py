def get_loan_details_schema():
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