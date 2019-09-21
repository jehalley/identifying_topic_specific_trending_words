#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 20 09:22:08 2019

@author: JeffHalley
"""

# test DB connectivity
import os
import pandas as pd
import psycopg2
import numpy as np

#postgres_host = os.environ['POSTGRES_HOST']
#postgres_username = os.environ['POSTGRES_USERNAME']
#postgres_password = os.environ['POSTGRES_PASSWORD']

if __name__ == "__main__":
    #test db connectivity
    try:
        connection = psycopg2.connect(host='35.162.89.159', port=5431, user='jh',
                          password='jh', dbname='word')
#        connection = psycopg2.connect(host = postgres_host,
#                                        port = '5432',
#                                        user = postgres_username,
#                                        password = postgres_password,
#                                        dbname = 'insightproject')
    
        cursor = connection.cursor()
        print('postgres db connect sucess')
    except:
        print('postgres db connect error, please recheck security group and POSTGRES login credentials')

    #test query result
    try:
        select_table = 'reddit_results'
        cursor.execute("SELECT topic, date, word, change_in_frequency_day FROM reddit_results WHERE topic = 'Basketball' AND '[2016-01-01, 2017-01-01]'::daterange @> date ORDER BY change_in_frequency_day DESC LIMIT 10".format(select_table))
        #cursor.execute("SELECT topic, date, word, sub_freq_to_all_freq_ratio FROM reddit_results WHERE topic = 'Basketball' AND '[2016-01-01, 2017-01-01]'::daterange @> date AND sub_freq_to_all_freq_ratio > 10 ORDER BY word, date".format(select_table))
        data = cursor.fetchall()
        query_data = pd.DataFrame(data = data, columns=['topic', 'date', 'word', 'change_in_frequency_over_previous_day'])
        #query_data = pd.DataFrame(data = data, columns=['topic', 'date', 'word', 'weighted_frequency'])

        print(data)
        print(query_data)
    except:
        print("DB error")


