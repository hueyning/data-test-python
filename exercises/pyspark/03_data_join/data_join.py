"""
As a Data Engineer at a rapidly growing e-commerce company, you are given two CSV files: users.csv and purchases.csv.
The users.csv file contains details about your users, and the purchases.csv file holds records of all purchases made.

Your team is interested in gaining a deeper understanding of customer behavior.
A question they’re particularly interested in is: "What is the total spending of each customer?"

Your task is to extract meaningful information from these data sets to answer this question.
The output of your work should be a JSON file, output.json.
"""
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))
from utils import LOGGER, setup_windows_env
from conf import DATA_JOIN_INPUT_USER_CSV, DATA_JOIN_INPUT_PURCHASE_CSV, DATA_JOIN_OUTPUT_JSON


def join_data(users_df, purchases_df):
    # Sum purchases groupBy user_id
    sum_purchases_df = purchases_df \
        .groupBy("user_id") \
        .agg(func.round(func.sum("price"), 2).alias("total_spending"))

    # Join - use inner join to filter out users with 0 spend, save storage space
    join_df = users_df \
        .join(sum_purchases_df, on="user_id", how="inner") \
        .orderBy("total_spending", ascending=False)

    return join_df


if __name__ == "__main__":
    setup_windows_env()
    spark = SparkSession.builder.appName(Path(__file__).stem).getOrCreate()
    LOGGER.info(f"spark config: {spark.sparkContext.getConf().getAll()}")

    # Input
    users_df = spark \
        .read \
        .option("header", True) \
        .csv(DATA_JOIN_INPUT_USER_CSV) \
        .selectExpr("CAST(user_id AS INT)", "CAST(name AS STRING)")
    purchases_df = spark \
        .read \
        .option("header", True) \
        .csv(DATA_JOIN_INPUT_PURCHASE_CSV) \
        .selectExpr("CAST(user_id AS INT)", "CAST(price AS DOUBLE)")

    # Transformation
    join_df = join_data(users_df, purchases_df)

    # Output
    join_df \
        .write \
        .mode("overwrite") \
        .json(DATA_JOIN_OUTPUT_JSON)

    LOGGER.info(f"Output file {DATA_JOIN_OUTPUT_JSON} successfully generated! Ending program.")
