"""
Loan Scoring Business Logic Module

This module implements the three-dimensional loan risk scoring model:
1. Payment History Score (20% weight) - How reliably has the borrower paid?
2. Defaulter History Score (45% weight) - What's their credit risk track record?
3. Financial Health Score (35% weight) - What's their current financial position?

Each function creates a SQL temporary view with intermediate scores that feed into
the next stage, creating a chainable pipeline of transformations.
"""


def create_payment_history_view(spark, params):
    """
    DIMENSION 1: Payment History Scoring (20% weight)
    
    Evaluates how consistently the borrower has paid by scoring:
    1. Last Payment Score: How much was the most recent payment?
       - <50% of installment = 100 pts (very bad)
       - 50-100% of installment = 100 pts (very bad)
       - Exactly 100% = 500 pts (good)
       - 100-150% = 650 pts (very good)
       - >150% = 800 pts (excellent)
    
    2. Total Payment Score: How much total has been repaid?
       - 0% funded = 0 pts (unacceptable)
       - 0-50% of funded = 500 pts (good)
       - 50%+ of funded = 650 pts (very good)
    
    Creates temp view: payment_history (payment history points)
    """
    df = spark.sql(f"""
    SELECT c.member_id,
      CASE
        WHEN p.last_payment_amount < (c.installment * 0.5) THEN {params['lending_project.very_bad_rated_pts']}
        WHEN p.last_payment_amount >= (c.installment * 0.5) AND p.last_payment_amount < c.installment THEN {params['lending_project.very_bad_rated_pts']}
        WHEN p.last_payment_amount = c.installment THEN {params['lending_project.good_rated_pts']}
        WHEN p.last_payment_amount > c.installment AND p.last_payment_amount <= (c.installment * 1.50) THEN {params['lending_project.very_good_rated_pts']}
        WHEN p.last_payment_amount > (c.installment * 1.50) THEN {params['lending_project.excellent_rated_pts']}
        ELSE {params['lending_project.unacceptable_rated_pts']}
      END AS last_payment_pts,
      CASE
        WHEN p.total_payment >= (c.funded_amount * 0.50) THEN {params['lending_project.very_good_rated_pts']}
        WHEN p.total_payment < (c.funded_amount * 0.50) AND p.total_payment > 0 THEN {params['lending_project.good_rated_pts']}
        WHEN p.total_payment = 0 OR p.total_payment IS NULL THEN {params['lending_project.unacceptable_rated_pts']}
      END AS total_payment_pts
    FROM loan_repayment p
    INNER JOIN loans c ON c.id = p.id
    """)
 
    df.createOrReplaceTempView("payment_history")
    return df

def create_defaulters_score_view(spark, params):
    """
    DIMENSION 2: Defaulter History Scoring (45% weight) - HIGHEST WEIGHT
    
    Evaluates credit risk indicators from borrower's past:
    1. Delinquency Score: How many times have they been late in past 2 years?
       - 0 delinquencies = 800 pts (excellent)
       - 1-2 delinquencies = 250 pts (bad)
       - 3-5 delinquencies = 100 pts (very bad)
       - 5+ delinquencies = 750 pts (unacceptable)
    
    2. Public Records Score: Any public record issues?
    3. Bankruptcy Score: Any bankruptcy history?
    4. Recent Inquiry Score: How many hard inquiries in past 6 months?
       - 0 inquiries = 800 pts (excellent)
       - 1-2 inquiries = 250 pts (bad)
       - 3-5 inquiries = 100 pts (very bad)
       - 5+ inquiries = 0 pts (unacceptable)
    
    This dimension receives 45% weight because past credit problems are the
    strongest predictor of future loan default.
    
    Creates temp view: defaulters_score (defaulter/delinquency + payment history points)
    """
    df = spark.sql(f"""
    SELECT p.*, 
    CASE 
        WHEN d.delinq_2_years = 0 THEN {params['lending_project.excellent_rated_pts']} 
        WHEN d.delinq_2_years BETWEEN 1 AND 2 THEN {params['lending_project.bad_rated_pts']} 
        WHEN d.delinq_2_years BETWEEN 3 AND 5 THEN {params['lending_project.very_bad_rated_pts']} 
        WHEN d.delinq_2_years > 5 OR d.delinq_2_years IS NULL THEN {params['lending_project.unacceptable_grade_pts']} 
    END AS delinq_pts, 
    CASE 
        WHEN l.public_record = 0 THEN {params['lending_project.excellent_rated_pts']} 
        WHEN l.public_record BETWEEN 1 AND 2 THEN {params['lending_project.bad_rated_pts']} 
        WHEN l.public_record BETWEEN 3 AND 5 THEN {params['lending_project.very_bad_rated_pts']} 
        WHEN l.public_record > 5 OR l.public_record IS NULL THEN {params['lending_project.very_bad_rated_pts']} 
    END AS public_records_pts, 
    CASE 
        WHEN l.public_record_bankruptcies = 0 THEN {params['lending_project.excellent_rated_pts']} 
        WHEN l.public_record_bankruptcies BETWEEN 1 AND 2 THEN {params['lending_project.bad_rated_pts']} 
        WHEN l.public_record_bankruptcies BETWEEN 3 AND 5 THEN {params['lending_project.very_bad_rated_pts']} 
        WHEN l.public_record_bankruptcies > 5 OR l.public_record_bankruptcies IS NULL THEN {params['lending_project.very_bad_rated_pts']} 
    END as public_bankruptcies_pts, 
    CASE 
        WHEN l.inquiry_last_6_months = 0 THEN {params['lending_project.excellent_rated_pts']} 
        WHEN l.inquiry_last_6_months BETWEEN 1 AND 2 THEN {params['lending_project.bad_rated_pts']} 
        WHEN l.inquiry_last_6_months BETWEEN 3 AND 5 THEN {params['lending_project.very_bad_rated_pts']} 
        WHEN l.inquiry_last_6_months > 5 OR l.inquiry_last_6_months IS NULL THEN {params['lending_project.unacceptable_rated_pts']} 
    END AS enq_pts 
    FROM defaulter_details l 
    INNER JOIN delinq_details d ON d.member_id = l.member_id  
    INNER JOIN payment_history p ON p.member_id = l.member_id
    """)

    df.createOrReplaceTempView('defaulters_score')
    return df

def create_overall_member_score_view(spark,params):
    df=spark.sql(f"""
   SELECT ldef.*, 
   CASE 
   WHEN LOWER(l.loan_status) LIKE '%fully paid%' THEN {params['lending_project.excellent_rated_pts']} 
   WHEN LOWER(l.loan_status) LIKE '%current%' THEN {params['lending_project.good_rated_pts']} 
   WHEN LOWER(l.loan_status) LIKE '%in grace period%' THEN {params['lending_project.bad_rated_pts']} 
   WHEN LOWER(l.loan_status) LIKE '%late (16-30 days)%' OR LOWER(l.loan_status) LIKE '%late (31-120 days)%' THEN {params['lending_project.very_bad_rated_pts']} 
   WHEN LOWER(l.loan_status) LIKE '%charged off%' THEN {params['lending_project.unacceptable_rated_pts']} 
   ELSE {params['lending_project.unacceptable_rated_pts']} 
   END AS loan_status_pts, 
   CASE 
   WHEN LOWER(a.home_ownership) LIKE '%own' THEN {params['lending_project.excellent_rated_pts']} 
   WHEN LOWER(a.home_ownership) LIKE '%rent' THEN {params['lending_project.good_rated_pts']} 
   WHEN LOWER(a.home_ownership) LIKE '%mortgage' THEN {params['lending_project.bad_rated_pts']} 
   WHEN LOWER(a.home_ownership) LIKE '%any' OR LOWER(a.home_ownership) IS NULL THEN {params['lending_project.very_bad_rated_pts']} 
   END AS home_pts, 
   CASE 
   WHEN l.funded_amount <= (a.total_high_credit_limit * 0.10) THEN {params['lending_project.excellent_rated_pts']} 
   WHEN l.funded_amount > (a.total_high_credit_limit * 0.10) AND l.funded_amount <= (a.total_high_credit_limit * 0.20) THEN {params['lending_project.very_good_rated_pts']} 
   WHEN l.funded_amount > (a.total_high_credit_limit * 0.20) AND l.funded_amount <= (a.total_high_credit_limit * 0.30) THEN {params['lending_project.good_rated_pts']} 
   WHEN l.funded_amount > (a.total_high_credit_limit * 0.30) AND l.funded_amount <= (a.total_high_credit_limit * 0.50) THEN {params['lending_project.bad_rated_pts']} 
   WHEN l.funded_amount > (a.total_high_credit_limit * 0.50) AND l.funded_amount <= (a.total_high_credit_limit * 0.70) THEN {params['lending_project.very_bad_rated_pts']} 
   WHEN l.funded_amount > (a.total_high_credit_limit * 0.70) THEN {params['lending_project.unacceptable_rated_pts']} 
   ELSE {params['lending_project.unacceptable_rated_pts']} 
   END AS credit_limit_pts, 
   CASE 
   WHEN (a.grade) = 'A' AND (a.sub_grade)='A1' THEN {params['lending_project.excellent_rated_pts']} 
   WHEN (a.grade) = 'A' AND (a.sub_grade)='A2' THEN ({params['lending_project.excellent_rated_pts']} * 0.95) 
   WHEN (a.grade) = 'A' AND (a.sub_grade)='A3' THEN ({params['lending_project.excellent_rated_pts']} * 0.90) 
   WHEN (a.grade) = 'A' AND (a.sub_grade)='A4' THEN ({params['lending_project.excellent_rated_pts']} * 0.85) 
   WHEN (a.grade) = 'A' AND (a.sub_grade)='A5' THEN ({params['lending_project.excellent_rated_pts']} * 0.80) 
   WHEN (a.grade) = 'B' AND (a.sub_grade)='B1' THEN ({params['lending_project.very_good_rated_pts']}) 
   WHEN (a.grade) = 'B' AND (a.sub_grade)='B2' THEN ({params['lending_project.very_good_rated_pts']} * 0.95) 
   WHEN (a.grade) = 'B' AND (a.sub_grade)='B3' THEN ({params['lending_project.very_good_rated_pts']} * 0.90) 
   WHEN (a.grade) = 'B' AND (a.sub_grade)='B4' THEN ({params['lending_project.very_good_rated_pts']} * 0.85) 
   WHEN (a.grade) = 'B' AND (a.sub_grade)='B5' THEN ({params['lending_project.very_good_rated_pts']} * 0.80) 
   WHEN (a.grade) = 'C' AND (a.sub_grade)='C1' THEN ({params['lending_project.good_rated_pts']}) 
   WHEN (a.grade) = 'C' AND (a.sub_grade)='C2' THEN ({params['lending_project.good_rated_pts']} * 0.95) 
   WHEN (a.grade) = 'C' AND (a.sub_grade)='C3' THEN ({params['lending_project.good_rated_pts']} * 0.90) 
   WHEN (a.grade) = 'C' AND (a.sub_grade)='C4' THEN ({params['lending_project.good_rated_pts']} * 0.85) 
   WHEN (a.grade) = 'C' AND (a.sub_grade)='C5' THEN ({params['lending_project.good_rated_pts']} * 0.80) 
   WHEN (a.grade) = 'D' AND (a.sub_grade)='D1' THEN ({params['lending_project.bad_rated_pts']}) 
   WHEN (a.grade) = 'D' AND (a.sub_grade)='D2' THEN ({params['lending_project.bad_rated_pts']} * 0.95) 
   WHEN (a.grade) = 'D' AND (a.sub_grade)='D3' THEN ({params['lending_project.bad_rated_pts']} * 0.90) 
   WHEN (a.grade) = 'D' AND (a.sub_grade)='D4' THEN ({params['lending_project.bad_rated_pts']} * 0.85) 
   WHEN (a.grade) = 'D' AND (a.sub_grade)='D5' THEN ({params['lending_project.bad_rated_pts']} * 0.80) 
   WHEN (a.grade) = 'E' AND (a.sub_grade)='E1' THEN ({params['lending_project.very_bad_rated_pts']}) 
   WHEN (a.grade) = 'E' AND (a.sub_grade)='E2' THEN ({params['lending_project.very_bad_rated_pts']} * 0.95) 
   WHEN (a.grade) = 'E' AND (a.sub_grade)='E3' THEN ({params['lending_project.very_bad_rated_pts']} * 0.90) 
   WHEN (a.grade) = 'E' AND (a.sub_grade)='E4' THEN ({params['lending_project.very_bad_rated_pts']} * 0.85) 
   WHEN (a.grade) = 'E' AND (a.sub_grade)='E5' THEN ({params['lending_project.very_bad_rated_pts']} * 0.80) 
   WHEN (a.grade) IN ('F', 'G') THEN ({params['lending_project.unacceptable_rated_pts']}) 
   END AS grade_pts 
   FROM defaulters_score ldef 
   INNER JOIN loans l ON ldef.member_id = l.member_id 
   INNER JOIN members a ON a.member_id = ldef.member_id
    """)
    df.createOrReplaceTempView('overall_member_score')
    return df 


def create_loan_score_view(spark):
    """Calculate composite loan score combining all three dimensions."""
    loan_score = spark.sql("""
                with loan_score_init as(
                  SELECT member_id, 
                ((last_payment_pts + total_payment_pts) * 0.20) AS payment_history_pts, 
                ((delinq_pts + public_records_pts + public_bankruptcies_pts + enq_pts) * 0.45) AS defaulters_history_pts, 
                ((loan_status_pts + home_pts + credit_limit_pts + grade_pts) * 0.35) AS financial_health_pts 
                FROM overall_member_score)
                
                select *, 
                        (payment_history_pts+
                        defaulters_history_pts+
                        financial_health_pts) as loan_score
                from loan_score_init
                """)
    loan_score.createOrReplaceTempView("loan_score_eval")
    return loan_score


def loan_score_final_view(spark,params):
   loan_score_final= spark.sql(f"""
   SELECT ls.*, 
   CASE 
   WHEN loan_score > {params['lending_project.very_good_grade_pts']} THEN 'A' 
   WHEN loan_score <= {params['lending_project.very_good_grade_pts']} AND loan_score > {params['lending_project.good_grade_pts']} THEN 'B' 
   WHEN loan_score <= {params['lending_project.good_grade_pts']} AND loan_score > {params['lending_project.bad_grade_pts']} THEN 'C' 
   WHEN loan_score <= {params['lending_project.bad_grade_pts']} AND loan_score > {params['lending_project.very_bad_grade_pts']} THEN 'D' 
   WHEN loan_score <= {params['lending_project.very_bad_grade_pts']} AND loan_score > {params['lending_project.unacceptable_grade_pts']} THEN 'E'  
   WHEN loan_score <= {params['lending_project.unacceptable_grade_pts']} THEN 'F' 
   END AS loan_final_grade 
   FROM loan_score_eval ls
    """)
   return loan_score_final









    








