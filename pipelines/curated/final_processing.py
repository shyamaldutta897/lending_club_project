from framework.readers.data_reader_generic import read
from framework.writers.data_writer import write
from framework.readers.read_options import get_read_options
from framework.config.config_reader import get_app_config,get_pyspark_config
from framework.session.spark_session import create_spark_session
from schemas import members_schema,loans_schema,loan_repayment_schema
from pyspark.sql.functions import *

spark=create_spark_session('LOCAL')

members_df=read(spark,\
               'csv',\
                get_app_config("LOCAL")["members.output.clean.path"],\
                members_schema.get_members_schema(),\
                get_read_options("csv"))

members_df.createOrReplaceTempView('members')

loans_df=read(spark,\
               'csv',\
                get_app_config("LOCAL")["loans.output.clean.path"],\
                loans_schema.get_loan_details_schema(),\
                get_read_options("csv"))

loans_df.createOrReplaceTempView('loans')

loans_repayment_df=read(spark,\
               'csv',\
                get_app_config("LOCAL")["loan_repayment.output.clean.path"],\
                loan_repayment_schema.get_loan_repayment_schema(),\
                get_read_options("csv"))

loans_repayment_df.createOrReplaceTempView('loan_repayment')

delinq_schema = 'member_id string,delinq_2_years int, delinq_amount float, months_since_last_delinq int'
delinq_df=read(spark,\
               'csv',\
                get_app_config("LOCAL")["loan_defaulters_delinq.output.clean.path"],\
                delinq_schema,\
                get_read_options("csv"))

delinq_df.createOrReplaceTempView('delinq_details')

defaulters_schema="""member_id string, 
                     public_record int,
                    public_record_bankruptcies int, 
                    inquiry_last_6_months int, 
                    months_since_last_delinq int,
                    months_since_last_record int"""
defaulters_df=read(spark,\
               'csv',\
                get_app_config("LOCAL")["loan_defaulters_delinq.output.clean.path"],\
                defaulters_schema,\
                get_read_options("csv"))

defaulters_df.createOrReplaceTempView('defaulter_details')


ph_df = spark.sql("select c.member_id, \
   case \
   when p.last_payment_amount < (c.installment * 0.5) then ${lending_project.very_bad_rated_pts} \
   when p.last_payment_amount >= (c.installment * 0.5) and p.last_payment_amount < c.installment then ${lending_project.very_bad_rated_pts} \
   when (p.last_payment_amount = (c.installment)) then ${lending_project.good_rated_pts} \
   when p.last_payment_amount > (c.installment) and p.last_payment_amount <= (c.installment * 1.50) then ${lending_project.very_good_rated_pts} \
   when p.last_payment_amount > (c.installment * 1.50) then ${lending_project.excellent_rated_pts} \
   else ${lending_project.unacceptable_rated_pts} \
   end as last_payment_pts, \
   case \
   when p.total_payment >= (c.funded_amount * 0.50) then ${lending_project.very_good_rated_pts} \
   when p.total_payment < (c.funded_amount * 0.50) and p.total_payment > 0 then ${lending_project.good_rated_pts} \
   when p.total_payment = 0 or (p.total_payment) is null then ${lending_project.unacceptable_rated_pts} \
   end as total_payment_pts \
from loan_repayment p \
inner join loans c on c.id = p.id")

ph_df.createOrReplaceTempView('ph_pts')

ldh_ph_df = spark.sql(
    "select p.*, \
    CASE \
    WHEN d.delinq_2_years = 0 THEN ${lending_project.excellent_rated_pts} \
    WHEN d.delinq_2_years BETWEEN 1 AND 2 THEN ${lending_project.bad_rated_pts} \
    WHEN d.delinq_2_years BETWEEN 3 AND 5 THEN ${lending_project.very_bad_rated_pts} \
    WHEN d.delinq_2_years > 5 OR d.delinq_2_years IS NULL THEN ${lending_project.unacceptable_grade_pts} \
    END AS delinq_pts, \
    CASE \
    WHEN l.public_record = 0 THEN ${lending_project.excellent_rated_pts} \
    WHEN l.public_record BETWEEN 1 AND 2 THEN ${lending_project.bad_rated_pts} \
    WHEN l.public_record BETWEEN 3 AND 5 THEN ${lending_project.very_bad_rated_pts} \
    WHEN l.public_record > 5 OR l.public_record IS NULL THEN ${lending_project.very_bad_rated_pts} \
    END AS public_records_pts, \
    CASE \
    WHEN l.public_record_bankruptcies = 0 THEN ${lending_project.excellent_rated_pts} \
    WHEN l.public_record_bankruptcies BETWEEN 1 AND 2 THEN ${lending_project.bad_rated_pts} \
    WHEN l.public_record_bankruptcies BETWEEN 3 AND 5 THEN ${lending_project.very_bad_rated_pts} \
    WHEN l.public_record_bankruptcies > 5 OR l.public_record_bankruptcies IS NULL THEN ${lending_project.very_bad_rated_pts} \
    END as public_bankruptcies_pts, \
    CASE \
    WHEN l.inquiry_last_6_months = 0 THEN ${lending_project.excellent_rated_pts} \
    WHEN l.inquiry_last_6_months BETWEEN 1 AND 2 THEN ${lending_project.bad_rated_pts} \
    WHEN l.inquiry_last_6_months BETWEEN 3 AND 5 THEN ${lending_project.very_bad_rated_pts} \
    WHEN l.inquiry_last_6_months > 5 OR l.inquiry_last_6_months IS NULL THEN ${lending_project.unacceptable_rated_pts} \
    END AS enq_pts \
    FROM defaulter_details l \
    INNER JOIN delinq_details d ON d.member_id = l.member_id  \
    INNER JOIN ph_pts p ON p.member_id = l.member_id")

ldh_ph_df = spark.sql(
    "select p.*, \
    CASE \
    WHEN d.delinq_2_years = 0 THEN ${lending_project.excellent_rated_pts} \
    WHEN d.delinq_2_years BETWEEN 1 AND 2 THEN ${lending_project.bad_rated_pts} \
    WHEN d.delinq_2_years BETWEEN 3 AND 5 THEN ${lending_project.very_bad_rated_pts} \
    WHEN d.delinq_2_years > 5 OR d.delinq_2_years IS NULL THEN ${lending_project.unacceptable_grade_pts} \
    END AS delinq_pts, \
    CASE \
    WHEN l.public_record = 0 THEN ${lending_project.excellent_rated_pts} \
    WHEN l.public_record BETWEEN 1 AND 2 THEN ${lending_project.bad_rated_pts} \
    WHEN l.public_record BETWEEN 3 AND 5 THEN ${lending_project.very_bad_rated_pts} \
    WHEN l.public_record > 5 OR l.public_record IS NULL THEN ${lending_project.very_bad_rated_pts} \
    END AS public_records_pts, \
    CASE \
    WHEN l.public_record_bankruptcies = 0 THEN ${lending_project.excellent_rated_pts} \
    WHEN l.public_record_bankruptcies BETWEEN 1 AND 2 THEN ${lending_project.bad_rated_pts} \
    WHEN l.public_record_bankruptcies BETWEEN 3 AND 5 THEN ${lending_project.very_bad_rated_pts} \
    WHEN l.public_record_bankruptcies > 5 OR l.public_record_bankruptcies IS NULL THEN ${lending_project.very_bad_rated_pts} \
    END as public_bankruptcies_pts, \
    CASE \
    WHEN l.inquiry_last_6_months = 0 THEN ${lending_project.excellent_rated_pts} \
    WHEN l.inquiry_last_6_months BETWEEN 1 AND 2 THEN ${lending_project.bad_rated_pts} \
    WHEN l.inquiry_last_6_months BETWEEN 3 AND 5 THEN ${lending_project.very_bad_rated_pts} \
    WHEN l.inquiry_last_6_months > 5 OR l.inquiry_last_6_months IS NULL THEN ${lending_project.unacceptable_rated_pts} \
    END AS enq_pts \
    FROM defaulter_details l \
    INNER JOIN delinq_details d ON d.member_id = l.member_id  \
    INNER JOIN ph_pts p ON p.member_id = l.member_id")

ldh_ph_df.createOrReplaceTempView('ldh_ph_pts')

fh_ldh_ph_df = spark.sql("select ldef.*, \
   CASE \
   WHEN LOWER(l.loan_status) LIKE '%fully paid%' THEN ${lending_project.excellent_rated_pts} \
   WHEN LOWER(l.loan_status) LIKE '%current%' THEN ${lending_project.good_rated_pts} \
   WHEN LOWER(l.loan_status) LIKE '%in grace period%' THEN ${lending_project.bad_rated_pts} \
   WHEN LOWER(l.loan_status) LIKE '%late (16-30 days)%' OR LOWER(l.loan_status) LIKE '%late (31-120 days)%' THEN ${lending_project.very_bad_rated_pts} \
   WHEN LOWER(l.loan_status) LIKE '%charged off%' THEN ${lending_project.unacceptable_rated_pts} \
   else ${lending_project.unacceptable_rated_pts} \
   END AS loan_status_pts, \
   CASE \
   WHEN LOWER(a.home_ownership) LIKE '%own' THEN ${lending_project.excellent_rated_pts} \
   WHEN LOWER(a.home_ownership) LIKE '%rent' THEN ${lending_project.good_rated_pts} \
   WHEN LOWER(a.home_ownership) LIKE '%mortgage' THEN ${lending_project.bad_rated_pts} \
   WHEN LOWER(a.home_ownership) LIKE '%any' OR LOWER(a.home_ownership) IS NULL THEN ${lending_project.very_bad_rated_pts} \
   END AS home_pts, \
   CASE \
   WHEN l.funded_amount <= (a.total_high_credit_limit * 0.10) THEN ${lending_project.excellent_rated_pts} \
   WHEN l.funded_amount > (a.total_high_credit_limit * 0.10) AND l.funded_amount <= (a.total_high_credit_limit * 0.20) THEN ${lending_project.very_good_rated_pts} \
   WHEN l.funded_amount > (a.total_high_credit_limit * 0.20) AND l.funded_amount <= (a.total_high_credit_limit * 0.30) THEN ${lending_project.good_rated_pts} \
   WHEN l.funded_amount > (a.total_high_credit_limit * 0.30) AND l.funded_amount <= (a.total_high_credit_limit * 0.50) THEN ${lending_project.bad_rated_pts} \
   WHEN l.funded_amount > (a.total_high_credit_limit * 0.50) AND l.funded_amount <= (a.total_high_credit_limit * 0.70) THEN ${lending_project.very_bad_rated_pts} \
   WHEN l.funded_amount > (a.total_high_credit_limit * 0.70) THEN ${lending_project.unacceptable_rated_pts} \
   else ${lending_project.unacceptable_rated_pts} \
   END AS credit_limit_pts, \
   CASE \
   WHEN (a.grade) = 'A' and (a.sub_grade)='A1' THEN ${lending_project.excellent_rated_pts} \
   WHEN (a.grade) = 'A' and (a.sub_grade)='A2' THEN (${lending_project.excellent_rated_pts} * 0.95) \
   WHEN (a.grade) = 'A' and (a.sub_grade)='A3' THEN (${lending_project.excellent_rated_pts} * 0.90) \
   WHEN (a.grade) = 'A' and (a.sub_grade)='A4' THEN (${lending_project.excellent_rated_pts} * 0.85) \
   WHEN (a.grade) = 'A' and (a.sub_grade)='A5' THEN (${lending_project.excellent_rated_pts} * 0.80) \
   WHEN (a.grade) = 'B' and (a.sub_grade)='B1' THEN (${lending_project.very_good_rated_pts}) \
   WHEN (a.grade) = 'B' and (a.sub_grade)='B2' THEN (${lending_project.very_good_rated_pts} * 0.95) \
   WHEN (a.grade) = 'B' and (a.sub_grade)='B3' THEN (${lending_project.very_good_rated_pts} * 0.90) \
   WHEN (a.grade) = 'B' and (a.sub_grade)='B4' THEN (${lending_project.very_good_rated_pts} * 0.85) \
   WHEN (a.grade) = 'B' and (a.sub_grade)='B5' THEN (${lending_project.very_good_rated_pts} * 0.80) \
   WHEN (a.grade) = 'C' and (a.sub_grade)='C1' THEN (${lending_project.good_rated_pts}) \
   WHEN (a.grade) = 'C' and (a.sub_grade)='C2' THEN (${lending_project.good_rated_pts} * 0.95) \
   WHEN (a.grade) = 'C' and (a.sub_grade)='C3' THEN (${lending_project.good_rated_pts} * 0.90) \
   WHEN (a.grade) = 'C' and (a.sub_grade)='C4' THEN (${lending_project.good_rated_pts} * 0.85) \
   WHEN (a.grade) = 'C' and (a.sub_grade)='C5' THEN (${lending_project.good_rated_pts} * 0.80) \
   WHEN (a.grade) = 'D' and (a.sub_grade)='D1' THEN (${lending_project.bad_rated_pts}) \
   WHEN (a.grade) = 'D' and (a.sub_grade)='D2' THEN (${lending_project.bad_rated_pts} * 0.95) \
   WHEN (a.grade) = 'D' and (a.sub_grade)='D3' THEN (${lending_project.bad_rated_pts} * 0.90) \
   WHEN (a.grade) = 'D' and (a.sub_grade)='D4' THEN (${lending_project.bad_rated_pts} * 0.85) \
   WHEN (a.grade) = 'D' and (a.sub_grade)='D5' THEN (${lending_project.bad_rated_pts} * 0.80) \
   WHEN (a.grade) = 'E' and (a.sub_grade)='E1' THEN (${lending_project.very_bad_rated_pts}) \
   WHEN (a.grade) = 'E' and (a.sub_grade)='E2' THEN (${lending_project.very_bad_rated_pts} * 0.95) \
   WHEN (a.grade) = 'E' and (a.sub_grade)='E3' THEN (${lending_project.very_bad_rated_pts} * 0.90) \
   WHEN (a.grade) = 'E' and (a.sub_grade)='E4' THEN (${lending_project.very_bad_rated_pts} * 0.85) \
   WHEN (a.grade) = 'E' and (a.sub_grade)='E5' THEN (${lending_project.very_bad_rated_pts} * 0.80) \
   WHEN (a.grade) in ('F', 'G') THEN (${lending_project.unacceptable_rated_pts}) \
   END AS grade_pts \
   FROM ldh_ph_pts ldef \
   INNER JOIN loans l ON ldef.member_id = l.member_id \
   INNER JOIN members a ON a.member_id = ldef.member_id")

fh_ldh_ph_df.createOrReplaceTempView("fh_ldh_ph_pts")

loan_score = spark.sql("SELECT member_id, \
((last_payment_pts+total_payment_pts)*0.20) as payment_history_pts, \
((delinq_pts + public_records_pts + public_bankruptcies_pts + enq_pts) * 0.45) as defaulters_history_pts, \
((loan_status_pts + home_pts + credit_limit_pts + grade_pts)*0.35) as financial_health_pts \
FROM fh_ldh_ph_pts")

final_loan_score = loan_score.withColumn\
                    ('loan_score',\
                      loan_score.payment_history_pts\
                    + loan_score.defaulters_history_pts\
                    + loan_score.financial_health_pts)

final_loan_score.createOrReplaceTempView("loan_score_eval")

loan_score_final = spark.sql("select ls.*, \
case \
WHEN loan_score > ${lending_project.very_good_grade_pts} THEN 'A' \
WHEN loan_score <= ${lending_project.very_good_grade_pts} AND loan_score > ${lending_project.good_grade_pts} THEN 'B' \
WHEN loan_score <= ${lending_project.good_grade_pts} AND loan_score > ${lending_project.bad_grade_pts} THEN 'C' \
WHEN loan_score <= ${lending_project.bad_grade_pts} AND loan_score  > ${lending_project.very_bad_grade_pts} THEN 'D' \
WHEN loan_score <= ${lending_project.very_bad_grade_pts} AND loan_score > ${lending_project.unacceptable_grade_pts} THEN 'E'  \
WHEN loan_score <= ${lending_project.unacceptable_grade_pts} THEN 'F' \
end as loan_final_grade \
from loan_score_eval ls")

loan_score_final=loan_score_final.repartition(10)

write(df=loan_score_final,\
      file_format='csv',\
      mode='overwrite',\
      partitionBy=None,\
      output_path=get_app_config("LOCAL")["final_data.output.path"])










