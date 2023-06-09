import pytest
import data_join


def test_join_data(spark):
    user_data = [(1, "Tommy"), (100, "Alice"), (1000, "Jack")]
    users_df = spark.createDataFrame(user_data, schema=["user_id", "name"])

    purchase_data = [(100, 74.68), (1000, 99.99), (1000, 0.02)]
    purchases_df = spark.createDataFrame(purchase_data, schema=["user_id", "price"])

    expected_data = [(1000, "Jack", 100.01), (100, "Alice", 74.68)]
    expected_df = spark.createDataFrame(expected_data, schema=["user_id", "name", "total_spending"])
    actual_df = data_join.join_data(users_df, purchases_df)

    assert sorted(expected_df.collect()) == sorted(actual_df.collect())
