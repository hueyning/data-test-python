"""
As a Data Engineer at a digital marketing agency, your team uses an in-house analytics tool that tracks user activity across various campaigns run by the company.
Each user interaction, whether a click or a view, is registered as an event.
The collected data, stored in a JSON file named data.json,
contains information about the date of the event (event_date) and the count of events that happened on that date (event_count).

The company wants to understand the total number of user interactions that occurred each day to identify trends in user engagement.
As such, your task is to analyze this data and prepare a summary report.
Your report should include the following information:
- The date of the events (event_date).
- The total number of events that occurred on each date (total_events).
The output should be sorted in descending order based on the total number of events,
and the results should be saved in a CSV file named output.csv.
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))
from utils import LOGGER, setup_windows_env
from conf import DATA_AGG_INPUT_JSON, DATA_AGG_OUTPUT_CSV


def aggregate_data(df):
    # Perform Aggregation
    agg_df = df \
        .groupBy("event_date") \
        .sum("event_count").withColumnRenamed("sum(event_count)", "total_events") \
        .orderBy(func.col("total_events").desc(), func.col("event_date").desc())
    return agg_df


if __name__ == '__main__':
    setup_windows_env()
    spark = SparkSession.builder.appName(Path(__file__).stem).getOrCreate()
    LOGGER.info(f"spark config: {spark.sparkContext.getConf().getAll()}")

    # Input
    df = spark \
        .read \
        .option("multiLine", True) \
        .json(DATA_AGG_INPUT_JSON) \
        .selectExpr("CAST(event_date AS DATE)", "CAST(user_id AS INT)", "CAST(event_count AS LONG)")

    # Transform
    agg_df = aggregate_data(df)

    # Output
    agg_df \
        .write \
        .partitionBy("event_date") \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(DATA_AGG_OUTPUT_CSV)

    LOGGER.info(f"Output file {DATA_AGG_OUTPUT_CSV} successfully generated! Ending program.")
