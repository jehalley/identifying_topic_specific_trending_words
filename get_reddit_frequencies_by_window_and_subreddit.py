#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 16 13:37:31 2019

@author: JeffHalley
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, window, broadcast
import pyspark.sql.functions as f
from pyspark.sql.window import Window


def get_pyspark_df(directory_path):
    #start spark session
    spark = SparkSession \
    .builder \
    .appName("Reddit DataFrame") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
    #read in jsons as pyspark df
    reddit_df = spark.read.json(reddit_directory_path)
    
    #drop unneeded columns
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
                       'permalink',
                       'removal_reason',
                       'retrieved_on',
                       'score',
                       'score_hidden',
                       'send_replies',
                       'stickied',
                       'subreddit_id',
                       'subreddit_name_prefixed',
                       'subreddit_type',
                       ]
    reddit_df = reddit_df.drop(*columns_to_drop)
    
    #convert created at utc to string date and make new column
    reddit_df = reddit_df.withColumn('date_time', from_unixtime('author_created_utc'))
    
    #bin comments to 1 day windows and make new column saving the window
    reddit_df = reddit_df.withColumn(
    "day_window",
    window(
         col('date_time'), 
         windowDuration="1 day"
    ).cast("struct<start:string,end:string>")
    )
 
    #convert subreddit topics csv to spark df
    subreddit_topics = spark.read.csv(subreddit_topics_csv, header='true', inferSchema='true')
    #reddit_df = reddit_df.join(subreddit_topics, on = 'subreddit', how = 'left_outer')
    reddit_df = reddit_df.join(broadcast(subreddit_topics), on = 'subreddit', how = 'left_outer')
    
    #split comment body into indivdidual words at any nonword character, group by subreddit and day window 
    reddit_df = reddit_df.withColumn('word', f.explode(f.split(f.col('body'), '[\W_]+')))\
    .groupBy('topic','day_window','word','date_time')\
    .count()
    
    #clean dataframe
    reddit_df = reddit_df.filter(reddit_df.topic. isNotNull())
    reddit_df = reddit_df.dropDuplicates()
    
    #for topics with multiple topics split into single topics
    reddit_df = reddit_df.withColumn('topic', f.explode(f.split(f.col('topic'), ',')))
    
    #get total count of each word across all of reddit for a day and add it to the reddit_df
    reddit_total_wc = reddit_df.groupby('word','day_window').sum()
    reddit_total_wc = reddit_total_wc.withColumnRenamed("sum(count)","count_per_day_all")
    reddit_df = reddit_df.join(reddit_total_wc, on = ['word','day_window'], how = 'left_outer')
    
    #get total 
    word_count_sum = reddit_df.groupBy('day_window').agg(f.sum('count'))
    word_count_sum = word_count_sum.withColumnRenamed("sum(count)","total_word_count_per_day_all")
    reddit_df = reddit_df.join(word_count_sum, on = ['day_window'], how = 'left_outer')
    
    #get total word count for each topic per day
    topic_count_sum = reddit_df.groupBy('day_window','topic').agg(f.sum('count'))
    topic_count_sum = topic_count_sum.withColumnRenamed("sum(count)","total_word_count_per_day_topic")
    reddit_df = reddit_df.join(topic_count_sum, on = ['day_window','topic'], how = 'left_outer')
    
    #make sub_freq to all_freq ratio
    reddit_df = reddit_df.withColumn("sub_freq_to_all_freq_ratio", 
                         ((f.col("count")/f.col("total_word_count_per_day_topic"))/
                          (f.col("count_per_day_all")/f.col("total_word_count_per_day_all"))))
    
    #make column with previou day adjusted frequency
    #windowSpec = Window.orderBy(reddit_df['day_window'])
    
    windowSpec = \
     Window \
     .partitionBy(reddit_df['topic'])\
     .orderBy(reddit_df['day_window'])
    
    reddit_df = reddit_df.withColumn('prev_day_adjusted_freq',
                                    f.lag(reddit_df["sub_freq_to_all_freq_ratio"])
                                    .over(windowSpec))
    
    
    reddit_df = reddit_df.withColumn('change_in_frequency_day', 
                                     (f.col('sub_freq_to_all_freq_ratio') - f.col('prev_day_adjusted_freq')))
    
    #get just date
    reddit_df = reddit_df.withColumn("date", f.to_date(f.col("date_time")))
    
    #remove uneeded columns
    columns_to_drop = ["day_window","date_time"]
    reddit_df = reddit_df.drop(*columns_to_drop)
    
    #write it
    reddit_df.write.partitionBy("topic","date").parquet("s3a://jeff-halley-s3/split_reddit_comments_2018_07/output_parquet_topic_date")
    
    #reddit_df.filter(reddit_df['word'] == 'lebron').show()
    #reddit_df.show()
    #reddit_df.count()
    #spark.stop()
 
if __name__ == "__main__":
    subreddit_topics_csv = 's3a://jeff-halley-s3/split_reddit_comments_2018_07/subreddit_topics.csv'
    reddit_directory_path = 's3a://jeff-halley-s3/split_reddit_comments_2018_07/xaa'
    reddit_df = get_pyspark_df(reddit_directory_path)
    #reddit_df.toPandas().to_csv('reddit_df_from_pyspark.csv')
    
    #reddit_df.collect()
    #reddit_df.write.format("csv").save("s3a://jeff-halley-s3/split_reddit_comments_2018_07/output.csv")
    
    
    #subreddit_topics_csv = '/Users/JeffHalley/subreddit_topics.csv'
    #reddit_directory_path = '/Users/JeffHalley/Downloads/RC_2018-07_test3/xaa.json'
    
