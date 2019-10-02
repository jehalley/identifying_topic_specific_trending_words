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


initial_topic = 'Basketball'
start_date_string = "2018-07-01"
end_date_string = "2018-07-02"

connection = psycopg2.connect(host='52.25.211.129', port=5431, user='jh', password='jh', dbname='word')
#query = "SELECT topic, date, word, sub_freq_to_all_freq_ratio  FROM reddit_results WHERE topic = 'Basketball' AND '[2016-01-01, 2017-01-01]'::daterange @> date ORDER BY sub_freq_to_all_freq_ratio  DESC LIMIT 10;"
query = "SELECT topic, date, word, count, freq_in_topic, sub_freq_to_all_freq_ratio, change_in_monthly_average FROM reddit_results_test WHERE topic = '" + initial_topic + "' AND '[" + start_date_string + ", " + end_date_string + "]'::daterange @> date ORDER BY change_in_monthly_average DESC;"
cursor = connection.cursor()
cursor.execute(query)
data = cursor.fetchall()
query_data = pd.DataFrame(data = data, columns=['topic', 
                                                          'date',
                                                          'word',
                                                          'count',
                                                          'freq_in_topic',
                                                          'topic_relevance',
                                                          'change_in_relevance', 
                                                          ])

#get change in frequency adjusted by frequency
start_day_freq = query_data[query_data['date'] == dt.strptime(start_date_string,'%Y-%m-%d').date()][['word','freq_in_topic']]
start_day_freq.columns = ['word','start_day_freq']
end_day_freq = query_data[query_data['date'] == dt.strptime(end_date_string,'%Y-%m-%d').date()][['word','freq_in_topic']]
end_day_freq.columns = ['word','end_day_freq']
change_in_freq = pd.merge(start_day_freq,end_day_freq, how = 'left', on = 'word')
change_in_freq['change_in_freq'] = change_in_freq['end_day_freq'] - change_in_freq['start_day_freq']
change_in_freq['change_in_adjusted_freq'] = change_in_freq['change_in_freq']/change_in_freq['start_day_freq']  
change_in_adjusted_freq = pd.DataFrame(change_in_freq[['word','change_in_adjusted_freq']]) 

#sort_values('change_in_adjusted_freq', ascending = False)



#    change_in_freq['adjusted_end_freq'] = change_in_freq['topic_relevance_y']/change_in_freq['end_day_freq'] 
#    change_in_freq['adjusted_start_freq'] = change_in_freq['topic_relevance_x']/change_in_freq['start_day_freq']
#    change_in_freq['change_in_adjusted_freq'] = change_in_freq['adjusted_end_freq'] - change_in_freq['adjusted_start_freq']
#     

#query_data['adjusted_change_in_freq'] = query_data['change_in_relevance']/query_data['freq_in_topic']
average_relevance = pd.DataFrame(query_data.groupby('word')['topic_relevance'].mean())
top_percentile_words = average_relevance[average_relevance.topic_relevance > average_relevance.topic_relevance.quantile(.80)]
query_data = pd.merge(top_percentile_words, query_data, how='left', on = 'word')

query_data = pd.merge(change_in_adjusted_freq, query_data, how='left', on = 'word')

query_data = query_data.sort_values('change_in_adjusted_freq', ascending=False)

#query_data = query_data['topic', 'date', 'word', 'count', 'sub_freq_to_all_freq_ratio_y','change_in_rolling_average']
df_with_extra_columns = query_data.head(10)
df = df_with_extra_columns[['topic','word','topic_relevance_y','change_in_adjusted_freq']]
df.columns = ['topic','word','topic_relevance','change_in_freq']

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
subreddit_topics_csv = 'subreddit_topics.csv'
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
        min_date_allowed=dt(2018, 7, 1),
        max_date_allowed=dt(2018, 7, 31 ),
        initial_visible_month=dt(2018, 7, 1),
        start_date = dt(2018, 7, 1),
        end_date=dt(2018, 7, 2)
    ),
    html.Div(id='output-container-date-picker-range'),
    
    dash_table.DataTable(
        id='datatable-interactivity',
        columns=[
            {"name": i, "id": i, "deletable": True, "selectable": True} for i in df.columns
        ],
        data=df.to_dict('records'),
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
     dash.dependencies.Input('chosen_date_range_string', 'children')])
def update_table(topic, date_range):
    #global topic
    query = "SELECT topic, date, word, count, freq_in_topic, sub_freq_to_all_freq_ratio, change_in_monthly_average FROM reddit_results_test WHERE topic = '" + topic + "' AND '[" + date_range + "]'::daterange @> date ORDER BY change_in_monthly_average;"
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    query_data = pd.DataFrame(data = data, columns=['topic', 
                                                          'date',
                                                          'word',
                                                          'count',
                                                          'freq_in_topic',
                                                          'topic_relevance',
                                                          'change_in_relevance', 
                                                          ])
    
    #get change in frequency adjusted by frequency
    start_day_freq = query_data[query_data['date'] == dt.strptime(start_date_string,'%Y-%m-%d').date()][['word','freq_in_topic']]
    start_day_freq.columns = ['word','start_day_freq']
    end_day_freq = query_data[query_data['date'] == dt.strptime(end_date_string,'%Y-%m-%d').date()][['word','freq_in_topic']]
    end_day_freq.columns = ['word','end_day_freq']
    change_in_freq = pd.merge(start_day_freq,end_day_freq, how = 'left', on = 'word')
    change_in_freq['change_in_freq'] = change_in_freq['end_day_freq'] - change_in_freq['start_day_freq']
    change_in_freq['change_in_adjusted_freq'] = change_in_freq['change_in_freq']/change_in_freq['start_day_freq']  
    change_in_adjusted_freq = pd.DataFrame(change_in_freq[['word','change_in_adjusted_freq']]) 
    
    #sort_values('change_in_adjusted_freq', ascending = False)
    
    
    
#    change_in_freq['adjusted_end_freq'] = change_in_freq['topic_relevance_y']/change_in_freq['end_day_freq'] 
#    change_in_freq['adjusted_start_freq'] = change_in_freq['topic_relevance_x']/change_in_freq['start_day_freq']
#    change_in_freq['change_in_adjusted_freq'] = change_in_freq['adjusted_end_freq'] - change_in_freq['adjusted_start_freq']
#     
    
    #query_data['adjusted_change_in_freq'] = query_data['change_in_relevance']/query_data['freq_in_topic']
    average_relevance = pd.DataFrame(query_data.groupby('word')['topic_relevance'].mean())
    top_percentile_words = average_relevance[average_relevance.topic_relevance > average_relevance.topic_relevance.quantile(.80)]
    query_data = pd.merge(top_percentile_words, query_data, how='left', on = 'word')
    
    query_data = pd.merge(change_in_adjusted_freq, query_data, how='left', on = 'word')

    query_data = query_data.sort_values('change_in_adjusted_freq', ascending=False)
    
    #query_data = query_data['topic', 'date', 'word', 'count', 'sub_freq_to_all_freq_ratio_y','change_in_rolling_average']
    df_with_extra_columns = query_data.head(10)
    df = df_with_extra_columns[['topic','word','topic_relevance_y','change_in_adjusted_freq']]
    df.columns = ['topic','word','topic_relevance','change_in_freq']
    
    data=df.to_dict('records')
    return data, query_data.to_json(date_format='iso', orient='split')

@app.callback(dash.dependencies.Output('graph', 'figure'), [dash.dependencies.Input('query_results', 'children')])
def update_graph(jsonified_query_data):
    # json.loads(jsonified_cleaned_data)
    query_data = pd.read_json(jsonified_query_data, orient='split')
    
    df = query_data.head(10)
    words = df.word.unique().tolist()
    fig = plotly.subplots.make_subplots(rows=3, cols=1, shared_xaxes=False,vertical_spacing=0.009,horizontal_spacing=0.009)
    fig['layout']['margin'] = {'l': 30, 'r': 10, 'b': 50, 't': 25}
    for word in words:
        word_results =  query_data[query_data['word'] == word][['count','date']].sort_values(by=['date'])
        word_results.columns = ['counts','date']
        fig.append_trace({'x':word_results.date,'y':word_results.counts,'type':'scatter','name': word},1,1)
    return fig


if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0')