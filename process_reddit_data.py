#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 16 13:37:31 2019

@author: JeffHalley
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import  col, avg, sum, lag, to_date
from pyspark.sql.window import Window

def start_spark_session():
    spark = SparkSession \
    .builder \
    .appName("Reddit DataFrame") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    return spark

def get_reddit_df_from_parquet(reddit_parquet_path):
    reddit_df = spark.read.parquet(reddit_parquet_path)
    return reddit_df


def get_word_counts(reddit_df):                              
    #split comment body into indivdidual words at any nonword character, group by subreddit and day window 
    #reddit_df = reddit_df.orderBy(['word','day_window'],ascending=False)
    reddit_df = reddit_df\
    .groupBy('topic','day_window','word','date_time')\
    .count()
    return reddit_df


def get_word_counts_for_combined(reddit_df):   
    reddit_total_wc = reddit_df.groupby('word','day_window').sum()
    reddit_total_wc = reddit_total_wc.withColumnRenamed("sum(count)","count_per_day_all")
    reddit_df = reddit_df.join(reddit_total_wc, on = ['word','day_window'], how = 'left_outer')
    return reddit_df


def get_total_word_count_per_day_all(reddit_df):
    word_count_sum = reddit_df.groupBy('day_window').agg(sum('count'))
    word_count_sum = word_count_sum.withColumnRenamed("sum(count)","total_word_count_per_day_all")
    reddit_df = reddit_df.join(word_count_sum, on = ['day_window'], how = 'left_outer')
    return reddit_df


def get_total_word_count_per_day_topic(reddit_df):
    topic_count_sum = reddit_df.groupBy('day_window','topic').agg(sum('count'))
    topic_count_sum = topic_count_sum.withColumnRenamed("sum(count)","total_word_count_per_day_topic")
    reddit_df = reddit_df.join(topic_count_sum, on = ['day_window','topic'], how = 'left_outer')
    return reddit_df


def get_sub_freq_to_all_freq_ratio(reddit_df):
    #make sub_freq to all_freq ratio
    reddit_df = reddit_df.withColumn("sub_freq_to_all_freq_ratio", 
                         ((col("count")/col("total_word_count_per_day_topic"))/
                          (col("count_per_day_all")/col("total_word_count_per_day_all"))))
    return reddit_df


#def get_rolling_average_of_sub_freq_to_all_freq_ratio(reddit_df):
#    days = lambda i: i * 86400
#    reddit_df = reddit_df.withColumn('timestamp', reddit_df.date_time.cast('timestamp'))
#    w = (Window.orderBy(col('timestamp').cast('long')).rangeBetween(-days(5), 0))
#    reddit_df = reddit_df.withColumn('rolling_average', avg("sub_freq_to_all_freq_ratio").over(w))
#    reddit_df = reddit_df.drop('timestamp')
#    return reddit_df


#def get_change_in_rolling_average_per_day(reddit_df):
#    #make column with previous day adjusted frequency
#    #windowSpec = Window.orderBy(reddit_df['day_window'])
#    windowSpec = \
#     Window \
#     .partitionBy(reddit_df['topic'])\
#     .orderBy(reddit_df['day_window'])
#    reddit_df = reddit_df.withColumn('prev_day_rolling_average',
#                                    lag(reddit_df['rolling_average'])
#                                    .over(windowSpec))
#    reddit_df = reddit_df.withColumn('change_in_rolling_average', 
#                                     (col('rolling_average') - col('rolling_average')))
#    reddit_df = reddit_df.drop('prev_day_rolling_average')
#    return reddit_df

 
def get_date_column(reddit_df):
    #get just date
    reddit_df = reddit_df.withColumn("date", to_date(col("date_time")))
    #remove uneeded columns
    columns_to_drop = ["day_window","date_time"]
    reddit_df = reddit_df.drop(*columns_to_drop)
    return reddit_df

def write_to_database(reddit_df):
    url = "jdbc:postgresql://10.0.0.8:5431/word"
    properties = {
        "user": "jh",
        "password": "jh",
        "driver": "org.postgresql.Driver"
    }
    reddit_df.write.jdbc(url=url, table="reddit_results", mode= "append", properties=properties)
        
    #write it
    #reddit_df.write.partitionBy("topic","date").parquet("s3a://jeff-halley-s3/split_reddit_comments_2018_07/output_parquet_topic_date")
    
    #reddit_df.filter(reddit_df['word'] == 'lebron').show()
    #reddit_df.show()
    #reddit_df.count()
    #spark.stop()
 
if __name__ == "__main__":
    spark = start_spark_session()
    reddit_parquet_path = "s3a://jeff-halley-s3/split_reddit_comments_2018_07/output_parquet"
    reddit_df = get_reddit_df_from_parquet(reddit_parquet_path)
    reddit_df = get_word_counts(reddit_df)
    reddit_df = get_word_counts_for_combined(reddit_df)
    reddit_df = get_total_word_count_per_day_all(reddit_df)
    reddit_df = get_total_word_count_per_day_topic(reddit_df)
    reddit_df = get_sub_freq_to_all_freq_ratio(reddit_df)
    #reddit_df = get_rolling_average_of_sub_freq_to_all_freq_ratio(reddit_df)
    #reddit_df = get_change_in_rolling_average_per_day(reddit_df)
    reddit_df = get_date_column(reddit_df) 
    write_to_database(reddit_df)

###test stuff
#spark = start_spark_session()
#reddit_parquet_path = "/Users/JeffHalley/Downloads/split_reddit_comments_2018_07/output_parquet"
#reddit_df = get_reddit_df_from_parquet(reddit_parquet_path)
#reddit_df = get_word_counts(reddit_df)
#reddit_df = get_word_counts_for_combined(reddit_df)
#reddit_df = get_total_word_count_per_day_all(reddit_df)
#reddit_df = get_total_word_count_per_day_topic(reddit_df)
#reddit_df = get_sub_freq_to_all_freq_ratio(reddit_df)
#reddit_df = get_rolling_average_of_sub_freq_to_all_freq_ratio(reddit_df)
#reddit_df = get_change_in_rolling_average_per_day(reddit_df)
#reddit_df = get_date_column(reddit_df)
#reddit_df.show()
