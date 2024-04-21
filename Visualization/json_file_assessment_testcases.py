import pytest
from json_file_assessment import (create_spark_session,
                                  valid_json_dataframe)


@pytest.fixture(scope="module")
def spark():
    return create_spark_session()


@pytest.fixture(scope="module")
def test_data_generate(spark):
    test_data = [(1, 'item1', 3, '2022-01-01'),
                 (2, None, 6, '2022-01-02'),
                 (3, 'item3', 4, '2022-05-05')]
    schema = ['id', 'item', 'rating', 'datetime']

    test_data_df = spark.createDataFrame(test_data, schema)
    return test_data_df


def test_create_spark_session():
    spark = create_spark_session()
    assert spark is not None


def test_read_json_to_dataframe():
    spark = create_spark_session()
    test_data = [(1, 'item1', 3, '2022-01-01'),
                 (2, None, 6, '2022-01-02')]  # Include invalid data
    schema = ['id', 'item', 'rating', 'datetime']
    test_df1 = spark.createDataFrame(test_data, schema)

    assert test_df1.count() > 0


def test_valid_json_dataframe(spark, test_data_generate):
    test_df = test_data_generate
    valid_data_df = valid_json_dataframe(test_df)
    assert valid_data_df.filter(valid_data_df['item'].isNull()).count() == 0
    assert valid_data_df.filter((valid_data_df['rating'] < 1)
                                | (valid_data_df['rating'] > 5)).count() == 0
    assert valid_data_df.count() == 2
