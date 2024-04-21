# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number, avg, min

# Create a SparkSession
spark = SparkSession.builder.master('local[1]') \
    .appName('NBC') \
    .getOrCreate()

output_path = '../Output_data/final_result.csv'
input_path = "../Input_data/race_lap_time_file.txt"


# Define a function to read data from a CSV file into a DataFrame
def read_data(input_path):
    return spark.read.csv(input_path, header=True, inferSchema=True)


# Define a function to calculate the average time and fastest lap for each driver
def calculate_avg_min_time(input_df):
    return (
        input_df.groupBy('Driver').
        agg(format_number(avg('Time'), 2).alias('Avg_time'),
            min('Time').alias('Fastest_Lap')).
        orderBy('Avg_time', ascending=True)).limit(3)


# Define a function to write the final DataFrame to a CSV file
def write_data(final_data_df, output_path):
    final_data_df.write.option('header', 'True').csv(output_path, mode='overwrite')


if __name__ == "__main__":
    final_df = read_data(input_path)
    result_df = calculate_avg_min_time(final_df)
    write_data(result_df, output_path)
