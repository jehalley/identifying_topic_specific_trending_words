#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 19 21:47:12 2019

@author: JeffHalley
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, window, broadcast
import pyspark.sql.functions as f
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("Reddit DataFrame") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

subreddit_topics_csv = 's3a://jeff-halley-s3/split_reddit_comments_2018_07/subreddit_topics.csv'
subreddit_topics = spark.read.csv(subreddit_topics_csv, header='true', inferSchema='true')

url = "jdbc:postgresql://10.0.0.8:5431/word"

properties = {
    "user": "jh",
    "password": "jh",
    "driver": "org.postgresql.Driver"
}

subreddit_topics.write.jdbc(url=url, table="test_table", mode= "overwrite", properties=properties)


psql -h 10.0.0.8 -d word -U jh -p 5431


subreddit_topics.write.format("jdbc").option("url", "jdbc:postgresql://10.0.0.8:5431/word").option("dbtable", "subreddit_topics").option("user", "jh").option("password", "jh").option("driver", 'com.mysql.jdbc.Driver').save()

subreddit_topics.write.jdbc.option("url", "jdbc:postgresql://10.0.0.8:5431/word").option("dbtable", "subreddit_topics").option("user", "jh").option("password", "jh").option("driver", 'com.mysql.jdbc.Driver')

