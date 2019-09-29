#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 28 14:45:46 2019

@author: JeffHalley
"""
from datetime import datetime
from nltk.corpus import stopwords
import os
from pandas import Timestamp, Series, date_range
import re
import string
import pandas as pd
import plotly
import psycopg2
import numpy as np

def make_stopwords_list():
    allStopwords = stopwords.words('english')
    newStopWords = ['like','rt','nan',' ','','im']
    allStopwords.extend(newStopWords)
    return allStopwords
    

connection = psycopg2.connect(host='18.237.35.222', port=5431, user='jh', password='jh', dbname='word')
#query = "SELECT topic, date, word, sub_freq_to_all_freq_ratio  FROM reddit_results WHERE topic = 'Basketball' AND '[2017-01-01, 2017-01-01]'::daterange @> date ORDER BY sub_freq_to_all_freq_ratio  DESC LIMIT 10;"
query = "SELECT word, count_per_day_all, total_word_count_per_day_all FROM reddit_results WHERE '[2017-01-01, 2017-02-01]'::daterange @> date  ;"
cursor = connection.cursor()
cursor.execute(query)
data = cursor.fetchall()
query_data = pd.DataFrame(data = data, columns=['word', 'count_per_day_all','total_word_count_per_day_all'])
connection.close()

word_list = query_data['word'].tolist()
word_list = list(set(word_list))
stop_words = make_stopwords_list()
no_stops = pd.DataFrame([word for word in word_list if word not in stop_words], columns = ['word'])
query_data_no_stops =  pd.merge(no_stops, query_data, how='left', on = 'word')

query_data_no_stops['frequency'] = query_data_no_stops['count_per_day_all']/query_data_no_stops['total_word_count_per_day_all']

average_frequency = pd.DataFrame(query_data_no_stops.groupby('word')['frequency'].mean())
bottom_50th_percentile_words = average_frequency[average_frequency.frequency > average_frequency.frequency.quantile(.50)]


            