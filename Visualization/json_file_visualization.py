# import necessary modules
import matplotlib.pyplot as plt
from json_file_assessment import create_spark_session,read_json_to_dataframe,valid_json_dataframe,title_ratings_df,patron_group_df


spark = create_spark_session()
# Path to the JSON file containing messages
message_file_path = ('../Input_data/messages.json')

# Reading JSON data into DataFrame
input_df = read_json_to_dataframe(spark, message_file_path)

# Filtering out rows with valid JSON data
valid_df = valid_json_dataframe(input_df)

# Finding the most popular titles and converting to Pandas DataFrame
title_df = title_ratings_df(valid_df)
pd_title_df = title_df.toPandas()

# Grouping patrons and converting to Pandas DataFrame
patron_df = patron_group_df(valid_df)
pd_patron_df = patron_df.toPandas()


def barchart_title_rating(my_title_df):
    plt.figure(figsize=(10, 5))
    plt.bar(my_title_df['item'], my_title_df['total_count'], color='skyblue')
    plt.title('Most popular titles', fontsize=10)
    plt.xlabel('Titles', fontsize=8)
    plt.ylabel('Title popularity', fontsize=8)
    plt.xticks(rotation=90, fontsize=6)
    plt.yticks(fontsize=6)
    plt.tight_layout()
    plt.show()


# Bar chart creation for Patrons groups
def barchart_patron_groups(my_patron_df):
    plt.figure(figsize=(10, 5))
    plt.bar(my_patron_df['item'] + ' - ' + my_patron_df['rating'].astype(str),
            my_patron_df['total_count'], color='skyblue')
    plt.title('Similar group of Patrons', fontsize=10)
    plt.xticks(rotation=90, fontsize=6)
    plt.yticks(fontsize=6)
    plt.xlabel('Title & rating', fontsize=8)
    plt.ylabel('Total_count', fontsize=8)
    plt.tight_layout()
    plt.show()


barchart_title_rating(pd_title_df)
barchart_patron_groups(pd_patron_df)