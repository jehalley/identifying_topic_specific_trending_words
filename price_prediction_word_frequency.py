#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 28 14:45:46 2019

@author: JeffHalley
"""
import pandas as pd
import plotly
import psycopg2
import numpy as np

connection = psycopg2.connect(host='18.237.35.222', port=5431, user='jh', password='jh', dbname='word')
#query = "SELECT topic, date, word, sub_freq_to_all_freq_ratio  FROM reddit_results WHERE topic = 'Basketball' AND '[2016-01-01, 2017-01-01]'::daterange @> date ORDER BY sub_freq_to_all_freq_ratio  DESC LIMIT 10;"
query = "SELECT topic, date, word, count,sub_freq_to_all_freq_ratio, change_in_rolling_average FROM reddit_results_9_26 WHERE topic = '" + initial_topic + "' AND '[" + start_date_string + ", " + end_date_string + "]'::daterange @> date ORDER BY change_in_rolling_average DESC;"
cursor = connection.cursor()
cursor.execute(query)
data = cursor.fetchall()
query_data = pd.DataFrame(data = data, columns=['topic', 'date', 'word', 'count', 'sub_freq_to_all_freq_ratio','change_in_rolling_average'])
average_adj_freq = pd.DataFrame(query_data.groupby('word')['sub_freq_to_all_freq_ratio'].mean())
top_percentile_words = average_adj_freq[average_adj_freq.sub_freq_to_all_freq_ratio > average_adj_freq.sub_freq_to_all_freq_ratio.quantile(.50)]
query_data = pd.merge(top_percentile_words, query_data, how='left', on = 'word')
query_data = query_data.sort_values('change_in_rolling_average', ascending=False)
df = query_data.head(10)