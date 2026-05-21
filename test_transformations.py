from framework.utils.spark_session import create_spark_session
from framework.readers.data_reader import read_loans_data
from pipelines.loans.transformations import clean_loan_details


spark = create_spark_session("LOCAL")

loans_df = read_loans_data(spark, "LOCAL")

clean_df = clean_loan_details(loans_df)

clean_df.show(5)