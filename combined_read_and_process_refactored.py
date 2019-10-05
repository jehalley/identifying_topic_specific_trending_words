#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 13:55:37 2019
@author: JeffHalley
"""
import os
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, window, broadcast, lower, explode, split, avg, sum, lag, to_date
from pyspark.sql.window import Window


def start_spark_session():
    spark = SparkSession \
    .builder \
    .appName("Reddit DataFrame") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    return spark

def get_reddit_df(directory_path):
    #read in jsons as pyspark df
    reddit_df = spark.read.json(reddit_directory_path)
    return reddit_df

def select_relevant_columns(reddit_df):
    reddit_df = reddit_df.select('created_utc','body','permalink','score','subreddit')
    return reddit_df

def get_date_columns(reddit_df):  
    #Some of these dates might seem redundant but postgres takes a regular date
    #and many spark functions seem to just work with date_time structs
    
    #date_time is needed for a step in the tokenization where I drop duplicates to avoid cases where one comment is used multiple times in a single comment
    reddit_df = reddit_df.withColumn('date_time', from_unixtime('created_utc'))
    
    #bin comments to 1 day windows and make new column saving the window
    reddit_df = reddit_df.withColumn(
    'day_window',
    window(
         col('date_time'), 
         windowDuration= '1 day'
    ).cast("struct<start:string,end:string>")
    )
    
    #get week window
    reddit_df = reddit_df.withColumn(
    'week_window',
    window(
         col('date_time'), 
         windowDuration= '7 day'
    ).cast("struct<start:string,end:string>")
    )
    reddit_df = reddit_df.withColumn("date", to_date(col("date_time")))
    
    #get month window
    reddit_df = reddit_df.withColumn(
    'month_window',
    window(
         col('date_time'), 
         windowDuration= '30 day'
    ).cast("struct<start:string,end:string>")
    )
    reddit_df = reddit_df.withColumn("date", to_date(col("date_time")))
    return reddit_df


def get_subreddit_topics_df(subreddit_topics_csv):
    #convert subreddit topics csv to spark df
    subreddit_topics = spark.read.csv(subreddit_topics_csv, header='true', inferSchema='true')
    subreddit_topics = subreddit_topics.withColumn('subreddit', lower(col('subreddit')))
    return subreddit_topics


def get_subreddit_topics_column(reddit_df,subreddit_topics):
    #insert topic column into reddit_df
    reddit_df = reddit_df.withColumn('subreddit', lower(col('subreddit')))
    reddit_df = reddit_df.join(broadcast(subreddit_topics), on = 'subreddit', how = 'left_outer')
    
    #clean dataframe
    reddit_df = reddit_df.filter(reddit_df.topic. isNotNull())
    
    #for topics with multiple topics split into single topics
    reddit_df = reddit_df.withColumn('topic', explode(split(col('topic'), ',')))
    return reddit_df

### HERE IS WHERE IT SHOULD BE EXPORTED TO ELASTIC SEARCH

def get_partitioned_df(reddit_df):
    #partition by 13 years x 12 months and nearest multiple of number of cores (108)
    reddit_df = reddit_df.repartition(200,["topic","month_window"])
    return reddit_df

def get_tokenized_df(reddit_df):
    #split into individual words
    tokenizer = Tokenizer(inputCol='body', outputCol='words_token')
    reddit_df = tokenizer.transform(reddit_df).select('topic','week_window','month_window','day_window','date', 'words_token', 'date_time')
    
    #remove stop words
    remover = StopWordsRemover(inputCol='words_token', outputCol='words_no_stops')
    reddit_df = remover.transform(reddit_df).select('topic','month_window','week_window','day_window','words_no_stops','date','date_time')
    
    #remove punctuation
    reddit_df = reddit_df.withColumn('words_and_punct', explode('words_no_stops')).select('topic','week_window','month_window','day_window','words_and_punct','date', 'date_time')
    reddit_df = reddit_df.withColumn('word', explode(split(col('words_and_punct'), '[\W_]+'))).select('topic','week_window','month_window','day_window','word','date', 'date_time')
    reddit_df = reddit_df.withColumn('word', lower(col('word')))
    reddit_df = reddit_df.filter( reddit_df['word'].rlike('[a-zA-Z]'))
    
    #duplicates dropped to ignore cases of someone using a word in the same post
    reddit_df = reddit_df.dropDuplicates()
    return reddit_df

def get_word_counts(reddit_df):                              
    #split comment body into indivdidual words at any nonword character, group by subreddit and day window 
    #reddit_df = reddit_df.orderBy(['word','day_window'],ascending=False)
    reddit_df = reddit_df\
    .groupBy('topic','week_window','month_window','day_window','word','date')\
    .count()
    reddit_df = reddit_df.cache()
    return reddit_df


def get_sum_of_counts_for_word_in_all_topics(reddit_df):   
    reddit_total_wc = reddit_df.groupby('word','day_window').sum()
    reddit_total_wc = reddit_total_wc.withColumnRenamed("sum(count)",'count_per_day_all')
    reddit_total_wc = reddit_total_wc.repartition(200,['word','day_window'])
    reddit_df = reddit_df.join(reddit_total_wc, on = ['word','day_window'], how = 'left_outer')
    return reddit_df


def get_total_word_count_per_day_all(reddit_df):
    word_count_sum = reddit_df.groupBy('day_window').agg(sum('count'))
    word_count_sum = word_count_sum.withColumnRenamed('sum(count)','total_word_count_per_day_all')
    word_count_sum = word_count_sum.repartition(200,['day_window'])
    reddit_df = reddit_df.join(word_count_sum, on = ['day_window'], how = 'left_outer')
    return reddit_df


def get_total_word_count_per_day_topic(reddit_df):
    topic_count_sum = reddit_df.groupBy('day_window','topic').agg(sum('count'))
    topic_count_sum = topic_count_sum.withColumnRenamed("sum(count)","total_word_count_per_day_topic")
    topic_count_sum = topic_count_sum.repartition(200,['topic','day_window'])
    reddit_df = reddit_df.join(topic_count_sum, on = ['topic','day_window'], how = 'left_outer')
    return reddit_df


def get_topic_freq_and_topic_to_all_freq_ratio(reddit_df):
    #make sub_freq to all_freq ratio
    reddit_df = reddit_df.withColumn("sub_freq_to_all_freq_ratio", 
                         ((col("count")/col("total_word_count_per_day_topic"))/
                          (col("count_per_day_all")/col("total_word_count_per_day_all"))))
    
    reddit_df = reddit_df.withColumn("freq_in_topic", 
                         ((col("count")/col("total_word_count_per_day_topic"))
                         )
                         )
                         
    return reddit_df


def get_rolling_average_of_daily_freq(reddit_df):
    reddit_df = reddit_df.withColumn('timestamp', reddit_df.date.cast('timestamp'))
    days = lambda i: i * 86400
    
    windowSpec_day = \
    Window \
     .partitionBy(['topic','word'])\
     .orderBy(col('timestamp').cast('long'))\
     .rangeBetween(-days(5), 0)
     
    reddit_df = reddit_df.withColumn('daily_freq_rolling_average', avg("freq_in_topic").over(windowSpec_day))
    
#    windowSpec_week = \
#    Window \
#     .partitionBy(['topic','word'])\
#     .orderBy(col('timestamp').cast('long'))\
#     .rangeBetween(-days(7), 0)
#    
#    reddit_df = reddit_df.withColumn('weekly_freq_rolling_average', avg("freq_in_topic").over(windowSpec_week))
#
#    
#    windowSpec_month = \
#    Window \
#     .partitionBy(['topic','word'])\
#     .orderBy(col('timestamp').cast('long'))\
#     .rangeBetween(-days(30), 0)
#    
#    reddit_df = reddit_df.withColumn('monthly_freq_rolling_average', avg("freq_in_topic").over(windowSpec_month))
#
    reddit_df = reddit_df.drop('timestamp')
    
    return reddit_df


def get_changes_in_rolling_average(reddit_df):
    #make column with previous day adjusted frequency
    windowSpec_day = \
     Window \
     .partitionBy(['topic','word'])\
     .orderBy(reddit_df['day_window'])
     
    reddit_df = reddit_df.withColumn('prev_day_rolling_average',
                                    lag(reddit_df['daily_freq_rolling_average'])
                                    .over(windowSpec_day))
    reddit_df = reddit_df.withColumn('change_in_daily_average', 
                                     (col('daily_freq_rolling_average') - col('prev_day_rolling_average')))
    
    reddit_df = reddit_df.drop('prev_day_rolling_average', 'day_window', 'week_window', 'month_window')
    
#    windowSpec_week = \
#     Window \
#     .partitionBy(['topic','word'])\
#     .orderBy(reddit_df['week_window'])
#     
#    reddit_df = reddit_df.withColumn('prev_week_rolling_average',
#                                    lag(reddit_df['weekly_freq_rolling_average'])
#                                    .over(windowSpec_week))
#    reddit_df = reddit_df.withColumn('change_in_weekly_average', 
#                                     (col('weekly_freq_rolling_average') - col('prev_week_rolling_average')))
#    reddit_df = reddit_df.drop('prev_week_rolling_average','week_window')
#    
#    windowSpec_month = \
#     Window \
#     .partitionBy(['topic','word'])\
#     .orderBy(reddit_df['month_window'])
#     
#    reddit_df = reddit_df.withColumn('prev_month_rolling_average',
#                                    lag(reddit_df['monthly_freq_rolling_average'])
#                                    .over(windowSpec_month))
#    reddit_df = reddit_df.withColumn('change_in_monthly_average', 
#                                     (col('monthly_freq_rolling_average') - col('prev_month_rolling_average')))
#    reddit_df = reddit_df.drop('prev_month_rolling_average','month_window')
    
    return reddit_df



def write_to_database(reddit_df):
    url = "jdbc:postgresql://10.0.0.8:5431/word"
    properties = {
        "user": os.environ['db_login'],
        "password": os.environ['db_pw'],
        "driver": "org.postgresql.Driver",
        "batchsize": "10000"   
    }
    reddit_df.write.jdbc(url=url, table="reddit_results", mode= "overwrite", properties=properties)
   
    
if __name__ == "__main__":
    spark = start_spark_session()
    reddit_directory_path = 's3a://jeff-halley-s3/2019_comments/'
    comments_all_columns_df = get_reddit_df(reddit_directory_path)
    comments_df = select_relevant_columns(comments_all_columns_df)
    comments_with_date_df = get_date_columns(comments_df)
    
    subreddit_topics_csv = 's3a://jeff-halley-s3/split_reddit_comments_2018_07/subreddit_topics/subreddit_topics.csv'
    subreddit_topics_df = get_subreddit_topics_df(subreddit_topics_csv)
    comments_with_topics_df = get_subreddit_topics_column(comments_with_date_df,subreddit_topics_df)
    
    partitioned_comments_df = get_partitioned_df(comments_with_topics_df)
    tokenized_df = get_tokenized_df(partitioned_comments_df)
    word_counts_by_topic_and_date_df = get_word_counts(tokenized_df)
    sum_of_counts_for_word_in_all__topics_df = get_sum_of_counts_for_word_in_all_topics(word_counts_by_topic_and_date_df)
    total_count_for_all_words_df = get_total_word_count_per_day_all(sum_of_counts_for_word_in_all__topics_df)
    word_count_per_topic_per_day_df = get_total_word_count_per_day_topic(total_count_for_all_words_df)
    topic_freq_and_topic_to_all_freq_ratio_df = get_topic_freq_and_topic_to_all_freq_ratio(word_count_per_topic_per_day_df)
    rolling_average_of_daily_freq = get_rolling_average_of_daily_freq(topic_freq_and_topic_to_all_freq_ratio_df)
    complete_reddit_df = get_changes_in_rolling_average(rolling_average_of_daily_freq)
    
    write_to_database(complete_reddit_df)