# Import necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField,
                               IntegerType, StringType, DateType)
from pyspark.sql.functions import (count, desc, asc, col)


# Create Spark session
def create_spark_session():
    return (SparkSession.builder.master('local')
            .appName('json file')
            .config('spark.executor.memory', '4g')
            .config('spark.driver.memory', '4g')
            .getOrCreate())


# Define schema with correct column order
def read_json_to_dataframe(spark_ses, message_file_path1):
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("item", StringType(), True),
        StructField("rating", IntegerType(), True),
        StructField("datetime", DateType(), True)
    ])
    return spark_ses.read.json(message_file_path1, schema=schema)


# Function to filter out rows with valid JSON data from a DataFrame
def valid_json_dataframe(valid_data_df):
    return (valid_data_df.filter((col('id').isNotNull()) &
                                 (col('item').isNotNull()) &
                                 (col('rating').between(1, 5)) &
                                 (col('datetime').isNotNull())))


# Function to find the most popular titles based on item counts
# Most popular titles
def title_ratings_df(title_data_df):
    return (title_data_df.select('item').groupBy('item')
            .agg(count('item').alias('total_count'))
            .orderBy('total_count', ascending=False).limit(100))


# Function to group patrons by item and rating, and count the occurrences
def patron_group_df(patron_data_df):
    return (patron_data_df.groupBy('item', 'rating')
            .agg(count('item').alias('total_count'))
            .orderBy(asc('item'), desc('rating')).limit(100))


