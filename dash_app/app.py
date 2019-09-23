#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 21 20:52:37 2019

@author: JeffHalley
"""
import ast
from datetime import datetime as dt
import dash
import dash_html_components as html
import dash_core_components as dcc
import pandas as pd


#this will be applied to the list of subreddits explained below
def topic_to_options_dict(x):
    options_dict = ast.literal_eval("{'label': '"+ x +"', 'value': '" + x + "'}")
    return options_dict

#the html translator below needs the topics as a list of dicts
def get_topics_as_options(subreddit_topics_csv):
    topics_df = pd.read_csv(subreddit_topics_csv)
    
    topics_df.set_index('subreddit').topic.str.split(',', expand=True).stack().reset_index('subreddit')
    
    topics_df = topics_df.set_index('subreddit')\
      .topic.str.split(',', expand=True)\
      .stack()\
      .reset_index('subreddit')\
      .rename(columns={0:'topic'})\
      .sort_values('topic')[['topic', 'subreddit']]\
      .reset_index(drop=True)
    
    topics = topics_df['topic'].tolist()
    topics = list(set(topics))
    topics.sort()
    topics_as_options = [topic_to_options_dict(topic) for topic in topics]
    return topics_as_options

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
subreddit_topics_csv = '/Users/JeffHalley/subreddit_topics.csv'
topics_as_options = get_topics_as_options(subreddit_topics_csv)

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.layout = html.Div([
    dcc.Dropdown(
        id='my-dropdown',
        options = topics_as_options, value = 'Basketball'
    ),
    html.Div(id='output-container'),
    
    dcc.DatePickerRange(
        id='my-date-picker-range',
        min_date_allowed=dt(2005, 12, 12),
        max_date_allowed=dt(2017, 7, 1 ),
        initial_visible_month=dt(2017, 1, 1),
        end_date=dt(2016, 1, 1)
    ),
    html.Div(id='output-container-date-picker-range')
])


@app.callback(
    dash.dependencies.Output('output-container', 'children'),
    [dash.dependencies.Input('my-dropdown', 'value')])
def update_topic_selections(value):
    return 'You have selected "{}"'.format(value)

@app.callback(
    dash.dependencies.Output('output-container-date-picker-range', 'children'),
    [dash.dependencies.Input('my-date-picker-range', 'start_date'),
     dash.dependencies.Input('my-date-picker-range', 'end_date')])
def update_output(start_date, end_date):
    string_prefix = 'You have selected: '
    if start_date is not None:
        start_date = dt.strptime(start_date, '%Y-%m-%d')
        start_date_string = start_date.strftime('%B %d, %Y')
        string_prefix = string_prefix + 'Start Date: ' + start_date_string + ' | '
    if end_date is not None:
        end_date = dt.strptime(end_date, '%Y-%m-%d')
        end_date_string = end_date.strftime('%B %d, %Y')
        string_prefix = string_prefix + 'End Date: ' + end_date_string
    if len(string_prefix) == len('You have selected: '):
        return 'Select a date to see it displayed here'
    else:
        return string_prefix

print("hello")




if __name__ == '__main__':
    app.run_server(debug=True)
    
