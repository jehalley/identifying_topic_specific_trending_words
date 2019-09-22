#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 16 13:37:31 2019

@author: JeffHalley
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, window, broadcast, lower, explode, split

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


def drop_irrelevant_columns(reddit_df):
    columns_to_drop = ['archived',
                       'author',
                       'author_cakeday',
                       'author_flair_background_color',
                       'author_flair_css_class',
                       'author_flair_richtext',
                       'author_flair_template_id',
                       'author_flair_text',
                       'author_flair_text_color',
                       'author_flair_type',
                       'author_fullname',
                       'can_gild',
                       'can_mod_post',
                       'collapsed',
                       'collapsed_reason',
                       'controversiality',
                       'distinguished',
                       'edited',
                       'gilded',
                       'id',
                       'is_submitter',
                       'link_id',
                       'no_follow',
                       'parent_id',
                       'removal_reason',
                       'retrieved_on',
                       'score_hidden',
                       'send_replies',
                       'stickied',
                       'subreddit_id',
                       'subreddit_name_prefixed',
                       'subreddit_type',
                       ]
    reddit_df = reddit_df.drop(*columns_to_drop)
    return reddit_df



def get_date_time_window_column(reddit_df):  
    #convert created at utc to string date and make new column
    reddit_df = reddit_df.withColumn('date_time', from_unixtime('author_created_utc'))
    #bin comments to 1 day windows and make new column saving the window
    reddit_df = reddit_df.withColumn(
    'day_window',
    window(
         col('date_time'), 
         windowDuration= '1 day'
    ).cast("struct<start:string,end:string>")
    )
    return reddit_df


def get_subreddit_topics_df(subreddit_topics_csv):
    #convert subreddit topics csv to spark df
    subreddit_topics = spark.read.csv(subreddit_topics_csv, header='true', inferSchema='true')
    return subreddit_topics


def get_subreddit_topics_column(reddit_df,subreddit_topics):
    reddit_df = reddit_df.join(broadcast(subreddit_topics), on = 'subreddit', how = 'left_outer')
    #clean dataframe
    reddit_df = reddit_df.filter(reddit_df.topic. isNotNull())
    #for topics with multiple topics split into single topics
    reddit_df = reddit_df.withColumn('topic', explode(split(col('topic'), ',')))
    return reddit_df

### HERE IS WHERE IT SHOULD BE EXPORTED TO ELASTIC SEARCH


def get_tokenized_df(reddit_df):
    #make comments all lowercase
    reddit_df = reddit_df.withColumn('word', explode(split(col('body'), '[\W_]+')))
    reddit_df = reddit_df.withColumn('word', lower(col('word')))
    reddit_df = reddit_df.filter( reddit_df['word'].rlike('[a-zA-Z]'))
    #duplicates dropped to ignore cases of someone using a word in the same post
    reddit_df = reddit_df.dropDuplicates()
    return reddit_df
    
if __name__ == "__main__":
    spark = start_spark_session()
    reddit_directory_path = 's3a://jeff-halley-s3/split_reddit_comments_2018_07/xci'
    subreddit_topics_csv = 's3a://jeff-halley-s3/split_reddit_comments_2018_07/subreddit_topics.csv'
    reddit_df = get_reddit_df(reddit_directory_path)
    reddit_df = drop_irrelevant_columns(reddit_df)
    reddit_df = get_date_time_window_column(reddit_df)
    subreddit_topics = get_subreddit_topics_df(subreddit_topics_csv)
    reddit_df = get_subreddit_topics_column(reddit_df,subreddit_topics)
    reddit_df = get_tokenized_df(reddit_df)
    reddit_df.write.mode('append').parquet("s3a://jeff-halley-s3/split_reddit_comments_2018_07/output_parquet")


##test stuff
#spark = start_spark_session()
#reddit_directory_path = directory_path = '/Users/JeffHalley/Downloads/split_reddit_comments_2018_07/xci'
#subreddit_topics_csv = '/Users/JeffHalley/subreddit_topics.csv'
#reddit_df = get_reddit_df(reddit_directory_path)
#reddit_df = drop_irrelevant_columns(reddit_df)
#reddit_df = get_date_time_window_column(reddit_df)
#subreddit_topics = get_subreddit_topics_df(subreddit_topics_csv)
#reddit_df = get_subreddit_topics_column(reddit_df,subreddit_topics)
#reddit_df = get_individual_words(reddit_df)
#reddit_df.write.mode('append').parquet("/Users/JeffHalley/Downloads/split_reddit_comments_2018_07/output_parquet")

