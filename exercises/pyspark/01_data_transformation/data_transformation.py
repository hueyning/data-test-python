"""
You are a Data Engineer at an online retail company that has a significant number of daily users on its website.
The company captures several types of user interaction data: including user clicks, views, and purchases.

You are given a CSV file named data.csv, which contains this information. The columns in the file are as follows: user_id, timestamp, event_type, and duration.
Your task is to perform an analysis to better understand user behavior on the website.
Specifically, your manager wants to understand the average duration of a ‘click’ event for each user.
This means you need to consider only those events where users have clicked on something on the website.

Finally, your analysis should be presented in the form of a Parquet file named output.parquet that contains two columns: user_id and avg_duration.
The challenge here is to devise the most efficient and accurate solution using PySpark to read, process, and write the data.
Please also keep in mind the potential size and scale of the data while designing your solution.
"""

import pyspark
from pyspark.sql import SparkSession
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))
from utils import LOGGER, setup_windows_env
from conf import DATA_TRANSFORM_INPUT_CSV, DATA_TRANSFORM_OUTPUT_PARQUET


def transform_data(spark, df):
    df.createOrReplaceTempView("data_view")

    # Cast to correct data types
    # Cast timestamp to date-level - less granularity but save storage
    # If number of DAU is huge, we might not have space to support (dau_cnt * 24) for each day
    cast_df = spark.sql("""
    SELECT
        CAST(user_id AS INT) AS user_id
        ,CAST(timestamp AS DATE) AS date
        ,CAST(event_type AS STRING) AS event_type
        ,CAST(duration AS FLOAT) AS duration
    FROM data_view
    """)
    cast_df.createOrReplaceTempView("cast_data_view")

    # Get avg click duration group by user_id
    agg_df = spark.sql("""
    SELECT
        user_id
        ,date
        ,AVG(duration) AS avg_duration
    FROM cast_data_view
    WHERE event_type = 'click'
    GROUP BY user_id, date
    """)

    return agg_df


if __name__ == "__main__":
    setup_windows_env()
    spark = SparkSession.builder.appName(Path(__file__).stem).getOrCreate()
    LOGGER.info(f"spark config: {spark.sparkContext.getConf().getAll()}")

    # Input
    df = spark \
        .read \
        .option("header", True) \
        .csv(DATA_TRANSFORM_INPUT_CSV)

    # Transform
    agg_df = transform_data(spark, df)

    # Output
    agg_df \
        .write \
        .partitionBy("date") \
        .mode("overwrite") \
        .parquet(DATA_TRANSFORM_OUTPUT_PARQUET)

    LOGGER.info(f"Output file {DATA_TRANSFORM_OUTPUT_PARQUET} successfully generated! Ending program.")
