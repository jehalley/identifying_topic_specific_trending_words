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
import dash_table
import pandas as pd
import plotly
import psycopg2
import numpy as np


def get_days_between(d1, d2):
    d1 = dt.strptime(d1, "%Y-%m-%d")
    d2 = dt.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)

def get_query(topic,start_date_string, end_date_string):
    days_between = get_days_between(start_date_string,end_date_string)
    if days_between > 14:
        query = "SELECT topic, date, word, count, freq_in_topic, sub_freq_to_all_freq_ratio, change_in_monthly_average FROM reddit_results_test WHERE topic = '" + initial_topic + "' AND '[" + start_date_string + ", " + end_date_string + "]'::daterange @> date ORDER BY change_in_monthly_average DESC;"
    
    if days_between < 14 and days_between > 2:
        query = "SELECT topic, date, word, count, freq_in_topic, sub_freq_to_all_freq_ratio, change_in_weekly_average FROM reddit_results_test WHERE topic = '" + initial_topic + "' AND '[" + start_date_string + ", " + end_date_string + "]'::daterange @> date ORDER BY change_in_weekly_average DESC;"
        
    if days_between < 2:
        query = "SELECT topic, date, word, count, freq_in_topic, sub_freq_to_all_freq_ratio, change_in_daily_average FROM reddit_results_test WHERE topic = '" + initial_topic + "' AND '[" + start_date_string + ", " + end_date_string + "]'::daterange @> date ORDER BY change_in_daily_average DESC;"
    
    return query


def get_query_data_df(query):
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    
    unprocessed_query_df = pd.DataFrame(data = data, columns=['topic', 
                                                          'date',
                                                          'word',
                                                          'count',
                                                          'freq_in_topic',
                                                          'topic_relevance',
                                                          'change_in_relevance', 
                                                          ])
    
    unprocessed_query_df['adjusted_change_in_freq'] = unprocessed_query_df['change_in_relevance']/unprocessed_query_df['freq_in_topic']
    
    #calculate average relevance score so only most relevant words will be considered
    average_relevance = pd.DataFrame(unprocessed_query_df.groupby('word')['topic_relevance'].mean())
    most_relevant_words = average_relevance[average_relevance.topic_relevance > average_relevance.topic_relevance.quantile(.50)]
    
    query_data_for_most_relevant_words = pd.merge(most_relevant_words, unprocessed_query_df, how='left', on = 'word')
    query_data_df = query_data_for_most_relevant_words.sort_values('adjusted_change_in_freq', ascending=False)
    
    return query_data_df
    
def get_table_df(query_data_df):
    table_df_preprocessed = query_data_df.head(15)
    table_df = table_df_preprocessed[['topic','word','topic_relevance_y','adjusted_change_in_freq']].copy()
    
    return table_df
    
      

connection = psycopg2.connect(host='52.25.211.129', port=5431, user='jh', password='jh', dbname='word')

initial_topic = 'Basketball'
initial_start_date_string = "2018-07-01"
initial_end_date_string = "2018-07-8"

initial_query = get_query(initial_topic, initial_start_date_string, initial_end_date_string)    

query_data_df = get_query_data_df(initial_query)

table_df = get_table_df(query_data_df)

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

subreddit_topics_csv = 'subreddit_topics.csv'
topics_as_options = get_topics_as_options(subreddit_topics_csv)

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.layout = html.Div([
    dcc.Dropdown(
        id='my-dropdown',
        options = topics_as_options, value = 'Basketball'
    ),
    html.Div(id='output-container'),
    
    dcc.DatePickerRange(
        id='my-date-picker-range',
        min_date_allowed=dt(2018, 7, 1),
        max_date_allowed=dt(2018, 7, 31 ),
        initial_visible_month=dt(2018, 7, 1),
        start_date = dt(2018, 7, 7),
        end_date=dt(2018, 7, 14)
    ),
    html.Div(id='output-container-date-picker-range'),
    
    dash_table.DataTable(
        id='datatable-interactivity',
        columns=[
            {"name": i, "id": i, "deletable": True, "selectable": True} for i in table_df.columns
        ],
        data=table_df.to_dict('records'),
        editable=True,
        filter_action="native",
        sort_action="native",
        sort_mode="multi",
        column_selectable="single",
        row_selectable="multi",
        row_deletable=True,
        selected_columns=[],
        selected_rows=[],
        page_action="native",
        page_current= 0,
        page_size= 10,
    ),
    html.Div(id='datatable-interactivity-container'),
    
    dcc.Graph(id='graph'),
    
    html.Div(id='topic_from_pulldown', style={'display': 'none'}),
    html.Div(id='chosen_date_range_string', style={'display': 'none'}),
    html.Div(id='query_results', style={'display': 'none'})
    
])


@app.callback(
    dash.dependencies.Output('output-container', 'children'),
    [dash.dependencies.Input('my-dropdown', 'value')])
def update_topic_selections(value):
    return 'You have selected "{}"'.format(value)

@app.callback(
        dash.dependencies.Output('topic_from_pulldown', 'children'), 
        [dash.dependencies.Input('my-dropdown', 'value')])
def get_topic(value):
    topic = value 
    return topic


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

@app.callback(
    dash.dependencies.Output('chosen_date_range_string', 'children'),
    [dash.dependencies.Input('my-date-picker-range', 'start_date'),
     dash.dependencies.Input('my-date-picker-range', 'end_date')])
def get_date_range(start_date, end_date):
    if start_date is not None:
        start_date = dt.strptime(start_date, '%Y-%m-%d')
        start_date_string = start_date.strftime('%Y-%m-%d')
        
    if end_date is not None:
        end_date = dt.strptime(end_date, '%Y-%m-%d')
        end_date_string = end_date.strftime('%Y-%m-%d')
    date_range = start_date_string + ", " + end_date_string
    return date_range


@app.callback(
    [dash.dependencies.Output('datatable-interactivity', 'data'),
     dash.dependencies.Output('query_results', 'children')],
    [dash.dependencies.Input('topic_from_pulldown', 'children'),
     dash.dependencies.Input('my-date-picker-range', 'start_date'),
     dash.dependencies.Input('my-date-picker-range', 'end_date')])
def update_table(topic, start_date, end_date):
    #global topic
    query = get_query(topic, start_date, end_date)    
    query_data_df = get_query_data_df(query)
    table_df = get_table_df(query_data_df)
    data = table_df.to_dict('records')
    
    return data, query_data_df.to_json(date_format='iso', orient='split')

@app.callback(dash.dependencies.Output('graph', 'figure'), [dash.dependencies.Input('query_results', 'children')])
def update_graph(jsonified_query_data):
    # json.loads(jsonified_cleaned_data)
    query_data = pd.read_json(jsonified_query_data, orient='split')
    
    df = query_data.head(10)
    words = df.word.unique().tolist()
    fig = plotly.subplots.make_subplots(rows=3, cols=1, shared_xaxes=True,vertical_spacing=0.009,horizontal_spacing=0.009)
    fig['layout']['margin'] = {'l': 30, 'r': 10, 'b': 50, 't': 25}
    for word in words:
        word_results =  query_data[query_data['word'] == word][['count','date']].sort_values(by=['date'])
        fig.append_trace({'x':word_results['date'].tolist(),'y':word_results['count'].tolist(),'type':'scatter','name': word},1,1)
    return fig


if __name__ == '__main__':
    app.run_server(debug=False)
    #app.run_server(debug=False, host='0.0.0.0')