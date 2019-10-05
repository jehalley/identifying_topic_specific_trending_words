#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 13:55:37 2019
@author: JeffHalley
"""
import os
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col,broadcast, lower, explode, 
split, avg, sum, lag, to_date, month
from pyspark.sql.window import Window


def start_spark_session():
    spark = SparkSession \
    .builder \
    .appName("Reddit DataFrame") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    return spark

def get_comments_df(directory_path):
    #read in jsons as pyspark df
    comments_all_columns_df = spark.read.json(reddit_directory_path)
    return comments_all_columns_df

def select_relevant_columns(comments_all_columns_df):
    #only selecting columns that are relevant to counting words in each topic
    comments_df = comments_all_columns_df.select('created_utc',
                                                 'body',
                                                 'permalink',
                                                 'score',
                                                 'subreddit')
    return comments_df

def get_date_columns(comments_df):  
    '''Some of these dates might seem redundant but postgres takes a regular 
    date and many spark functions only work with date_time structs, but these 
    cannot be written to the database'''
    
    #date_time is needed for a step in the tokenization where I drop duplicates to avoid cases where one comment is used multiple times in a single comment
    comments_with_date_time_df = comments_df.withColumn('date_time', from_unixtime('created_utc'))
    
    #date is needed to groupby date for daily word counts
    comments_with_date_time_and_date_df = comments_with_date_time_df.withColumn("date", to_date(col("date_time")))
    
    #get month column
    comments_df_with_date_columns = comments_with_date_time_and_date_df.withColumn("month", month(to_date(col("date_time"))))
  
    return comments_df_with_date_columns


def get_subreddit_topics_df(subreddit_topics_csv):
    #convert subreddit topics csv to spark df this will be used to get topics column
    subreddit_topics = spark.read.csv(subreddit_topics_csv, header='true', inferSchema='true')
    subreddit_topics = subreddit_topics.withColumn('subreddit', lower(col('subreddit')))
    return subreddit_topics


def get_subreddit_topics_column(comments_df_with_date_columns,subreddit_topics):
    #insert topic column into reddit_df
    comments_with_lower_case_subreddits_column_df = comments_df_with_date_columns.withColumn('subreddit', lower(col('subreddit')))
    comments_with_unfiltered_list_of_topics_column_df = comments_with_lower_case_subreddits_column_df.join(broadcast(subreddit_topics), on = 'subreddit', how = 'left_outer')
    
    #clean dataframe
    comments_with_filtered_list_of_topics_column_df = comments_with_unfiltered_list_of_topics_column_df.filter(comments_with_unfiltered_list_of_topics_column_df.topic.isNotNull())
    
    #for topics with multiple topics split into single topics
    comments_df_with_topics_column = comments_with_filtered_list_of_topics_column_df.withColumn('topic', explode(split(col('topic'), ',')))
    return comments_df_with_topics_column

### HERE IS WHERE IT SHOULD BE EXPORTED TO ELASTIC SEARCH

def get_partitioned_df(comments_df_with_topics_column):
    #partition by 13 years x 12 months and nearest multiple of number of cores (108)
    partitioned_comments_df = comments_df_with_topics_column.repartition(200,['topic','month'])
    return partitioned_comments_df

def get_tokenized_df(partitioned_comments_df):
    #split into individual words
    tokenizer = Tokenizer(inputCol='body', outputCol='words_token')
    tokenized_but_unsplit_still_has_stop_words_and_punctuation_df = tokenizer.transform(partitioned_comments_df).select('topic','date_time','month','date', 'words_token')
    
    #remove stop words
    remover = StopWordsRemover(inputCol='words_token', outputCol='words_no_stops')
    tokenized_but_unsplit_still_has_punctuation_df = remover.transform(tokenized_but_unsplit_still_has_stop_words_and_punctuation_df).select('topic','words_no_stops','date_time','month','date',)
    
    #remove punctuation
    tokenized_but_unsplit_df = tokenized_but_unsplit_still_has_punctuation_df.withColumn('words_and_punct', explode('words_no_stops')).select('topic','words_and_punct','date_time','month','date',)
    tokenized_and_split_but_still_has_nonletters_and_uppercase_df = tokenized_but_unsplit_df.withColumn('word', explode(split(col('words_and_punct'), '[\W_]+'))).select('topic','word','date_time','month','date',)
    tokenized_and_split_but_still_has_nonletters_df = tokenized_and_split_but_still_has_nonletters_and_uppercase_df.withColumn('word', lower(col('word')))
    tokenized_and_split_but_still_has_duplicates_df = tokenized_and_split_but_still_has_nonletters_df.filter(tokenized_and_split_but_still_has_nonletters_df['word'].rlike('[a-zA-Z]'))
    
    #duplicates dropped to ignore cases of someone using a word in the same post
    tokenized_df = tokenized_and_split_but_still_has_duplicates_df.dropDuplicates()
    return tokenized_df

def get_word_counts(tokenized_df):                              
    #split comment body into indivdidual words at any nonword character, group by subreddit and day window 
    tokenized_df_with_word_counts_by_topic_and_date = tokenized_df\
    .groupBy('topic','word','month','date')\
    .count()
    #reddit_df = reddit_df.cache()
    return tokenized_df_with_word_counts_by_topic_and_date


def get_sum_of_counts_for_word_in_all_topics(tokenized_df_with_word_counts_by_topic_and_date):   
    total_wc_df = tokenized_df_with_word_counts_by_topic_and_date.groupby('word','month','date').sum()
    total_wc_with_count_per_day_all_column_df = total_wc_df.withColumnRenamed('sum(count)','count_per_day_all')
    repartitioned_total_wc_df = total_wc_with_count_per_day_all_column_df.repartition(200,['word','date'])
    tokenized_df_with_sum_of_counts_for_word_in_all_topics = tokenized_df_with_word_counts_by_topic_and_date.join(repartitioned_total_wc_df, on = ['word','month','date'], how = 'left_outer')
    return tokenized_df_with_sum_of_counts_for_word_in_all_topics.drop('sum(month)')


def get_total_word_count_per_day_all(tokenized_df_with_sum_of_counts_for_word_in_all_topics):
    word_count_sum_df = tokenized_df_with_sum_of_counts_for_word_in_all_topics.groupBy('date').agg(sum('count'))
    word_count_sum_with_total_word_count_per_day_all_column_df = word_count_sum_df.withColumnRenamed('sum(count)','total_word_count_per_day_all')
    repartitioned_word_count_sum_df = word_count_sum_with_total_word_count_per_day_all_column_df.repartition(200,['date'])
    df_with_total_count_for_all_words = tokenized_df_with_sum_of_counts_for_word_in_all_topics.join(repartitioned_word_count_sum_df, on = ['date'], how = 'left_outer')
    return df_with_total_count_for_all_words


def get_total_word_count_per_day_and_topic(df_with_total_count_for_all_words):
    topic_count_sum_df = df_with_total_count_for_all_words.groupBy('date','topic').agg(sum('count'))
    topic_count_sum_with_total_word_count_per_day_topic_column_df = topic_count_sum_df.withColumnRenamed('sum(count)','total_word_count_per_day_topic')
    repartitioned_topic_count_sum_df = topic_count_sum_with_total_word_count_per_day_topic_column_df .repartition(200,['topic','date'])
    df_with_word_count_per_day_and_topic = df_with_total_count_for_all_words.join(repartitioned_topic_count_sum_df, on = ['topic','date'], how = 'left_outer')
    
    return df_with_word_count_per_day_and_topic


def get_topic_freq_and_topic_to_all_freq_ratio(df_with_word_count_per_day_and_topic):
    #make sub_freq to all_freq ratio
    df_with_word_freq_in_topic = df_with_word_count_per_day_and_topic.withColumn('freq_in_topic', 
                         ((col('count')/col('total_word_count_per_day_topic'))))
    
    df_with_topic_freq_and_topic_freq_to_all_freq_ratio = df_with_word_freq_in_topic.withColumn('sub_freq_to_all_freq_ratio', 
                         (col('freq_in_topic'))/
                          ((col('count_per_day_all')/col('total_word_count_per_day_all'))))
    
    return df_with_topic_freq_and_topic_freq_to_all_freq_ratio


def get_rolling_average_of_daily_freq(df_with_topic_freq_and_topic_freq_to_all_freq_ratio):
    df_with_topic_freq_and_topic_freq_to_all_freq_ratio_and_timestamp = df_with_topic_freq_and_topic_freq_to_all_freq_ratio.withColumn('timestamp', df_with_topic_freq_and_topic_freq_to_all_freq_ratio.date.cast('timestamp'))
    days = lambda i: i * 86400
    
    windowSpec_day = \
    Window \
     .partitionBy(['topic','word'])\
     .orderBy(col('timestamp').cast('long'))\
     .rangeBetween(-days(5), 0)
     
    df_with_rolling_average_of_daily_freq_and_timestamp = df_with_topic_freq_and_topic_freq_to_all_freq_ratio_and_timestamp.withColumn('daily_freq_rolling_average', avg("freq_in_topic").over(windowSpec_day))

    df_with_rolling_average_of_daily_freq = df_with_rolling_average_of_daily_freq_and_timestamp.drop('timestamp')
    
    return df_with_rolling_average_of_daily_freq


def get_changes_in_rolling_average(df_with_rolling_average_of_daily_freq):
    #make column with previous day adjusted frequency
    windowSpec_day = \
     Window \
     .partitionBy(['topic','word'])\
     .orderBy(df_with_rolling_average_of_daily_freq['date'])
     
    df_with_prev_day_rolling_avg = df_with_rolling_average_of_daily_freq.withColumn('prev_day_rolling_average',
                                    lag(df_with_rolling_average_of_daily_freq['daily_freq_rolling_average'])
                                    .over(windowSpec_day))
    
    df_with_change_in_rolling_avg = df_with_prev_day_rolling_avg.withColumn('change_in_daily_average', 
                                     (col('daily_freq_rolling_average') - col('prev_day_rolling_average')))
    
    complete_reddit_df = df_with_change_in_rolling_avg.drop('prev_day_rolling_average')
    return complete_reddit_df



def write_to_database(complete_reddit_df):
    url = "jdbc:postgresql://10.0.0.8:5431/word"
    properties = {
        "user": os.environ['db_login'],
        "password": os.environ['db_pw'],
        "driver": "org.postgresql.Driver",
        "batchsize": "10000"   
    }
    complete_reddit_df.write.jdbc(url=url, table="reddit_results_refactored", mode= "overwrite", properties=properties)
   
    
if __name__ == "__main__":
    spark = start_spark_session()
    reddit_directory_path = 's3a://jeff-halley-s3/2019_comments/'
    comments_all_columns_df = get_comments_df(reddit_directory_path)
    comments_df = select_relevant_columns(comments_all_columns_df)
    comments_df_with_date_columns = get_date_columns(comments_df)
    
    subreddit_topics_csv = 's3a://jeff-halley-s3/split_reddit_comments_2018_07/subreddit_topics/subreddit_topics.csv'
    subreddit_topics_df = get_subreddit_topics_df(subreddit_topics_csv)
    comments_df_with_topics_column = get_subreddit_topics_column(comments_df_with_date_columns,subreddit_topics_df)
    partitioned_comments_df = get_partitioned_df(comments_df_with_topics_column)
    tokenized_df = get_tokenized_df(partitioned_comments_df)
    tokenized_df_with_word_counts_by_topic_and_date = get_word_counts(tokenized_df)
    
    tokenized_df_with_sum_of_counts_for_word_in_all_topics = get_sum_of_counts_for_word_in_all_topics(tokenized_df_with_word_counts_by_topic_and_date)
    df_with_total_count_for_all_words = get_total_word_count_per_day_all(tokenized_df_with_sum_of_counts_for_word_in_all_topics)
    df_with_word_count_per_day_and_topic = get_total_word_count_per_day_and_topic(df_with_total_count_for_all_words)
    df_with_topic_freq_and_topic_freq_to_all_freq_ratio = get_topic_freq_and_topic_to_all_freq_ratio(df_with_word_count_per_day_and_topic)
    df_with_rolling_average_of_daily_freq = get_rolling_average_of_daily_freq(df_with_topic_freq_and_topic_freq_to_all_freq_ratio)
    complete_reddit_df = get_changes_in_rolling_average(df_with_rolling_average_of_daily_freq)
    
    write_to_database(complete_reddit_df)