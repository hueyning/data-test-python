import os
import pytest
import sys
from pyspark.sql import SparkSession

# Setup windows conf for pytest env
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['HADOOP_HOME'] = "C:\\hadoop"


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark
    spark.stop()


def test_spark_session(spark):
    assert spark is not None
