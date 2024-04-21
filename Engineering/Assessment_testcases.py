# Import necessary modules
import pytest
from pyspark.sql import SparkSession
from Assessment_methods import (calculate_avg_min_time, read_data)
import filepaths


# Fixture to create a SparkSession for testing
@pytest.fixture(scope="module")
def spark_session():
    spark = SparkSession.builder \
        .master('local[1]') \
        .appName('TestNBCFunctions') \
        .getOrCreate()
    yield spark
    spark.stop()


# Test function to check the functionality of calculate_avg_min_time
def test_calculate_avg_min_time(spark_session):
    # Read the test input file
    input_df = read_data(filepaths.input_path)

    # Call the function being tested
    result_df = calculate_avg_min_time(input_df)

    # Assert the output DataFrame is as expectJed
    expected_data = [('Jackie', '3.00', 2.56), ('Mike', '3.35', 2.51), ('Jimlollar', '3.48', 2.44)]
    expected_df = spark_session.createDataFrame(expected_data, ['Driver', 'Avg_time', 'Fastest_Lap'])
    assert result_df.collect() == expected_df.collect()

