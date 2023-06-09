import pytest
import data_aggregation
import datetime as dt


def test_agg_data_total_event_desc(spark):
    input_data = [
        ("1", dt.date(2022, 1, 1), 25),
        ("1", dt.date(2022, 1, 2), 1),
        ("1", dt.date(2022, 1, 3), 1),
        ("2", dt.date(2022, 1, 1), 20),
        ("2", dt.date(2022, 1, 3), 99),
        ("3", dt.date(2022, 1, 2), 1),
    ]
    input_df = spark.createDataFrame(input_data, schema=["user_id", "event_date", "event_count"])

    expected_data = [(dt.date(2022, 1, 3), 100), (dt.date(2022, 1, 1), 45), (dt.date(2022, 1, 2), 2)]
    expected_df = spark.createDataFrame(expected_data, schema=["event_date", "total_events"])
    actual_df = data_aggregation.aggregate_data(input_df)

    assert (expected_df.collect()) == (actual_df.collect())


def test_agg_data_event_date_desc(spark):
    '''
    If total_events count is the same, order from largest date to smallest date
    '''
    input_data = [
        ("1", dt.date(2022, 1, 1), 1),
        ("1", dt.date(2022, 1, 1), 1),
        ("1", dt.date(2022, 1, 1), 1),
        ("2", dt.date(2022, 1, 2), 1),
        ("2", dt.date(2022, 1, 2), 1),
        ("2", dt.date(2022, 1, 2), 1),
        ("1", dt.date(2022, 1, 3), 1),
        ("1", dt.date(2022, 1, 3), 1),
        ("2", dt.date(2022, 1, 3), 1),
    ]
    input_df = spark.createDataFrame(input_data, schema=["user_id", "event_date", "event_count"])

    expected_data = [(dt.date(2022, 1, 3), 3), (dt.date(2022, 1, 2), 3), (dt.date(2022, 1, 1), 3)]
    expected_df = spark.createDataFrame(expected_data, schema=["event_date", "total_events"])
    actual_df = data_aggregation.aggregate_data(input_df)

    assert (expected_df.collect()) == (actual_df.collect())
