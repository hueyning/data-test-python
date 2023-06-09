import pytest
import data_transformation
import datetime as dt


def test_transform_data(spark):
    input_data = [
        (1, "2022-01-01 00:00:00", "click", "1"),
        (1, "2022-01-01 00:00:00", "view", "2"),
        (1, "2022-01-01 00:00:00", "click", "3"),
        (2, "2022-01-01 00:00:01", "click", "10"),
        (2, "2022-01-01 00:00:01", "click", "11"),
        (2, "2022-01-01 00:00:01", "clickk", "12"),
    ]
    input_df = spark.createDataFrame(input_data, schema=["user_id", "timestamp", "event_type", "duration"])

    expected_data = [(1, dt.date(2022, 1, 1), 2.0), (2, dt.date(2022, 1, 1), 10.5)]
    expected_df = spark.createDataFrame(expected_data, schema=["user_id", "date", "avg_duration"])
    actual_df = data_transformation.transform_data(spark, input_df)

    assert sorted(expected_df.collect()) == sorted(actual_df.collect())


def test_transform_data_partitioning(spark):
    input_data = [
        (1, "2022-01-01 23:59:59", "click", "1"),
        (1, "2022-01-01 23:59:59", "click", "1"),
        (1, "2022-01-01 23:59:59", "click", "1"),
        (1, "2022-01-02 00:00:00", "click", "2"),
        (1, "2022-01-02 00:00:00", "click", "2"),
        (1, "2022-01-02 00:00:00", "click", "2"),
        (2, "2022-01-02 23:59:59", "click", "3"),
        (2, "2022-01-02 23:59:59", "click", "3"),
        (2, "2022-01-02 23:59:59", "click", "3"),
        (2, "2022-01-03 00:00:00", "click", "4"),
        (2, "2022-01-03 00:00:00", "click", "5"),
        (2, "2022-01-03 00:00:00", "click", "6"),
    ]
    input_df = spark.createDataFrame(input_data, schema=["user_id", "timestamp", "event_type", "duration"])

    expected_data = [
        (1, dt.date(2022, 1, 1), 1.0), (1, dt.date(2022, 1, 2), 2.0),
        (2, dt.date(2022, 1, 2), 3.0), (2, dt.date(2022, 1, 3), 5.0),
    ]
    expected_df = spark.createDataFrame(expected_data, schema=["user_id", "date", "avg_duration"])
    actual_df = data_transformation.transform_data(spark, input_df)

    assert sorted(expected_df.collect()) == sorted(actual_df.collect())
