#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 22 21:15:35 2019

@author: JeffHalley
"""

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
from dash.dependencies import Output
import requests
import dash_table as dt
import pandas as pd  
import plotly
import psycopg2

def generate_table():
    connection = psycopg2.connect(host='34.220.254.66', port=5431, user='jh',
                          password='jh', dbname='word')
    
    query = "SELECT topic, date, word, sub_freq_to_all_freq_ratio  FROM reddit_results WHERE topic = 'Basketball' AND '[2016-01-01, 2017-01-01]'::daterange @> date ORDER BY sub_freq_to_all_freq_ratio  DESC LIMIT 10"
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    query_data = pd.DataFrame(data = data, columns=['topic', 'date', 'word', 'change_in_frequency_over_previous_day'])
    
    table = dt.DataTable(
        rows = query_data.to_dict('records'),
        columns = query_data.columns,
        id = 'sample-table'
    )
    return table

app = dash.Dash()
app.layout = html.Div([generate_table(),
    dcc.Interval(
        id='interval-component',
        interval=5 * 1000,  # in milliseconds
    ),])


#@app.callback(Output('sample-table', 'rows'), events=[Event('interval-component', 'interval')])
#def generate_table():
#    connection = psycopg2.connect(host='35.162.89.159', port=5431, user='jh',
#                          password='jh', dbname='word')
#    
#    query = "SELECT topic, date, word, change_in_frequency_day FROM reddit_results WHERE topic = 'Basketball' AND '[2016-01-01, 2017-01-01]'::daterange @> date ORDER BY change_in_frequency_day DESC LIMIT 10"
#    cursor = connection.cursor()
#    cursor.execute(query)
#    data = cursor.fetchall()
#    query_data = pd.DataFrame(data = data, columns=['topic', 'date', 'word', 'change_in_frequency_over_previous_day'])
#    
#    table = dt.DataTable(
#        rows = query_data.to_dict('records'),
#        columns = query_data.columns,
#        id = 'sample-table'
#    )
#    return table