#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep  8 19:19:32 2019

@author: JeffHalley
"""
from datetime import datetime
from nltk.corpus import stopwords
import os
import pandas as pd
from pandas import Timestamp, Series, date_range
import re
import string

from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, from_unixtime, to_date, to_timestamp, lit, col, window
import pyspark.sql.functions as f

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

#get list of all twitter archive files
def get_file_list(directory_path):
    extensions = ('.bz2','.json')
    file_list = []
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            if file.endswith(extensions):
                file_list.append(os.path.join(root, file))
    return file_list

##the code below will make the dataframe for S3 that can be streamed based on 1 hour time window
# make reddit spark df
reddit_directory_path = '/Users/JeffHalley/Downloads/RC_2018-07_test2'
reddit_df = spark.read.json(reddit_directory_path)
reddit_df.show()

#convert the utc to datetime 
reddit_df = reddit_df.withColumn('date', from_unixtime('author_created_utc'))

#possibility for creating chunks in 5 minute intervals https://stackoverflow.com/questions/37632238/how-to-group-by-time-interval-in-spark-sql
#reddit_df.groupBy("author", window("date", "5 minutes"))

reddit_df = reddit_df.withColumn(
    "minute_window",
    window(
         col("date"), 
         windowDuration="1 hour"
    ).cast("struct<start:string,end:string>")
)

reddit_df = reddit_df.withColumn(
    "day_window",
    window(
         col("date"), 
         windowDuration="1 day"
    ).cast("struct<start:string,end:string>")
)

reddit_df.sort('date').show()   

#make_dffor actual wordcounts
reddit_wc = spark.read.json(reddit_directory_path)
# counts words in each body cell - not what I want but maybe useful somewhere
#reddit_wc = reddit_wc.withColumn('wordCount', f.size(f.split(f.col('body'), ' ')))

# count for everything and split at anything that's not a word 
reddit_wc2 = reddit_wc.withColumn('word', f.explode(f.split(f.col('body'), '[\W_]+')))\
    .groupBy('word')\
    .count()\
    .sort('count', ascending=False)\
    .show()

#get word counts for each subreddit
reddit_subreddit_wc_total = reddit_wc.groupby('subreddit').count()

reddit_subreddit_wc_total.show()

#try to get each word count for each subreddit

reddit_total_wc = reddit_wc.withColumn('date', from_unixtime('author_created_utc'))


reddit_total_wc = reddit_wc.withColumn(
    "day_window",
    window(
         col('date'), 
         windowDuration="1 day"
    ).cast("struct<start:string,end:string>")
)

reddit_sr_total_wc = reddit_wc.groupby('subreddit','day_window').count()

reddit_sr_total_wc.show()


#try to get individual word counts for each subreddit
reddit_sr_total_wc = reddit_total_wc.groupby('subreddit','day_window').withColumn('word', f.explode(f.split(f.col('body'), '[\W_]+')))\
    .groupBy('word')\
    .count()\
    .sort('count', ascending=False)\
    .show()


reddit_sr_total_wc = reddit_total_wc.withColumn('word', f.explode(f.split(f.col('body'), '[\W_]+'))\
                                            

reddit_wc2 = reddit_wc.withColumn('word', f.explode(f.split(f.col('body'), '[\W_]+')))\
    .groupBy('subreddit','day_window','word')\
    .count()\
    .sort('count', ascending=False)

    
.groupBy('subreddit','day_window','word')\
    .count()



#return values matching a condition
test = reddit_sr_total_wc.filter(reddit_sr_total_wc['subreddit'] == 'nba')
