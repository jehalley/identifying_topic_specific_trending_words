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


# make reddit spark df
reddit_directory_path = '/Users/JeffHalley/Downloads/RC_2018-07_test2'
reddit_df = spark.read.json(reddit_directory_path)
reddit_df.show()

#convert the utc to datetime 
reddit_df = reddit_df.withColumn('date', from_unixtime('author_created_utc'))

#possibility for creating chunks in 5 minute intervals https://stackoverflow.com/questions/37632238/how-to-group-by-time-interval-in-spark-sql
#reddit_df.groupBy("author", window("date", "5 minutes"))

reddit_df = reddit_df.withColumn(
    "window",
    window(
         col("date"), 
         windowDuration="5 minutes"
    ).cast("struct<start:bigint,end:bigint>")
)

reddit_df.sort('date').show()   

