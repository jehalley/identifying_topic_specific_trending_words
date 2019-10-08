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
import os
import pandas as pd
import plotly
import psycopg2
import numpy as np

def get_query(topic,start_date_string,end_date_string):
    query = "SELECT topic, date, word, count, freq_in_topic,"\
    +"sub_freq_to_all_freq_ratio, daily_freq_rolling_average FROM "\
    +"reddit_results WHERE topic = '" + topic + "' AND '["\
    + start_date_string + ", " + end_date_string + "]'::daterange @> date;"
    
    return query

def get_requested_data(query):
    connection = psycopg2.connect(host='127.0.0.1', 
                                  port=5431, 
                                  user=os.environ['db_login'], 
                                  password=os.environ['db_pw'], 
                                  dbname='word')

    cursor = connection.cursor()
    cursor.execute(query)
    requested_data = cursor.fetchall()
    connection.close()
    
    return requested_data
    
def get_query_data_df(requested_data,start_date_string, end_date_string):
    query_data_prefilter = pd.DataFrame(
            data = requested_data, columns=['topic',
                                            'date',
                                            'word',
                                            'count',
                                            'freq_in_topic',
                                            'topic_relevance',
                                            'freq_rolling_average'])
    
    #subreddit names seem to pop up occassionaly with the prefix 2f
    query_data_filtered = query_data_prefilter[~query_data_prefilter['word']\
                                      .str.contains("2f")]
    
    #get average relevance to filter the most relevant words
    average_relevance = pd.DataFrame(query_data_filtered\
                                     .groupby('word')['topic_relevance']\
                                     .mean())
    
    top_percentile_words = average_relevance[average_relevance\
                                             .topic_relevance > 
                                             average_relevance\
                                             .topic_relevance.quantile(.80)]
    
    #filter out bottom 80th percentile words by merging
    query_data_top_20prcnt = pd.merge(top_percentile_words, 
                                         query_data_filtered, 
                                         how='left', on = 'word')
    
    
    '''get change in frequency weight by dividing starting frequency this will 
    be used to find the most trending words'''
    start_day_freq = query_data_top_20prcnt[
            query_data_top_20prcnt['date'] == dt\
            .strptime(start_date_string,'%Y-%m-%d')\
            .date()][['word','freq_rolling_average']]
    start_day_freq.columns = ['word','start_day_freq']
    
    end_day_freq = query_data_top_20prcnt[
            query_data_top_20prcnt['date'] == dt\
            .strptime(end_date_string,'%Y-%m-%d')\
            .date()][['word','freq_rolling_average']]
    end_day_freq.columns = ['word','end_day_freq']
    
    change_in_freq = pd.merge(start_day_freq,end_day_freq, 
                              how = 'left', on = 'word')
    
    change_in_freq['change_in_freq'] = change_in_freq['end_day_freq']\
    - change_in_freq['start_day_freq']
    
    change_in_freq['change_in_adjusted_freq'] = change_in_freq[
            'change_in_freq']/change_in_freq['start_day_freq']
    
    change_in_adjusted_freq = pd.DataFrame(change_in_freq[
            ['word','change_in_adjusted_freq','end_day_freq']]) 
    
    #add trending data, this will be used to sort later
    query_data = pd.merge(query_data_top_20prcnt,change_in_adjusted_freq, 
                          how='left', on = 'word')

    return query_data


def get_table_df(query_data):
    '''the following makes the table of just the top 10 words in terms of 
    change in freq without most of the other data'''
    df_with_excess_clmns_rows = query_data[['topic',
                                            'word',
                                            'topic_relevance_x',
                                            'change_in_adjusted_freq',
                                            'end_day_freq']]
    
    df_with_extra_columns = df_with_excess_clmns_rows\
    .groupby(['topic',
              'word',
              'topic_relevance_x',
              'end_day_freq'], as_index=False)\
    .sum()
    
    df_with_adjtd_freq = df_with_extra_columns\
    .sort_values('change_in_adjusted_freq', ascending=False)\
    .head(10)
    
    sorted_df_with_adjtd_freq = df_with_adjtd_freq\
    .sort_values('end_day_freq', ascending=False)
    
    sorted_df_with_adjtd_freq['rank'] = 1\
    + np.arange(len(sorted_df_with_adjtd_freq))
    
    df = sorted_df_with_adjtd_freq[['rank',
                                    'topic',
                                    'word',
                                    'topic_relevance_x',
                                    'end_day_freq']]
    
    df.columns = ['rank',
                  'topic',
                  'word',
                  'topic_relevance',
                  'ending_frequency']
    
    return df
   
#this will be applied to the list of subreddits explained below
def topic_to_options_dict(x):
    options_dict = ast.literal_eval("{'label': '"+ x +"', 'value': '" + x + "'}")
    return options_dict

#the html translator below needs the topics as a list of dicts
def get_topics_as_options(subreddit_topics_csv):
    topics_df = pd.read_csv(subreddit_topics_csv)
    
    topics_df.set_index('subreddit').topic.str.split(',', expand=True)\
    .stack().reset_index('subreddit')
    
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

#this block of code is for setting up the table that shows when the page loads 
initial_topic = 'Aquariums'
start_date_string = "2019-05-01"
end_date_string = "2019-05-30"

query = get_query(initial_topic,start_date_string,end_date_string)
requested_data = get_requested_data(query)
query_data = get_query_data_df(requested_data,start_date_string,end_date_string)
df = get_table_df(query_data)

#the pulldown menu will use the subreddit_topics_csv to generate options
subreddit_topics_csv = 'subreddit_topics.csv'
topics_as_options = get_topics_as_options(subreddit_topics_csv)

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
colors = {
    'background': '#000000',
    'text': '#7FDBFF',
    "blue": "#4285f4",
    "white": "#fff2ff"
}

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(
                children=[
                        
                    html.H3('Welcome to WordEdge', 
                            style={"backgroundColor": colors["blue"],
                                   'textAlign': 'center',
                                   'color': colors['text']}),
                    html.H4('Helping you get an edge in your Ad Words Auctions', 
                            style={'textAlign': 'center',
                                   'color': colors['blue']}),
                    html.H6('Select a topic related to your product', 
                            style={'textAlign': 'left',
                                   'color': colors['blue']}),
                    
                    #this is the pulldown menu
                    html.Div(
                    [
                            #html.Label('Select Topic'),
                            dcc.Dropdown(
                                    id = 'my-dropdown',
                                    options = topics_as_options, 
                                    value = 'Aquariums')
                    ]),
                            
                    html.Div(id='output-container'),
                    
                    html.H6('Select a date range for us to analyze', 
                            style={'textAlign': 'left',
                                   'color': colors['blue']}),
                    
                    #this is the calendar
                    dcc.DatePickerRange(
                        id='my-date-picker-range',
                        min_date_allowed=dt(2019, 3, 1),
                        max_date_allowed=dt(2019, 5, 30 ),
                        initial_visible_month=dt(2019, 5, 1)
                    ),
                    
                    html.Div(id='output-container-date-picker-range'),
                    
                    html.H6('Your Top 10 best Choices for AdWords are:', 
                            style={'textAlign': 'center',
                                   'color': colors['blue']}),
                    
                    dash_table.DataTable(
                        id='datatable-interactivity',
                        columns=[
                            {"name": i, 
                             "id": i, 
                             "deletable": True, 
                             "selectable": True} for i in df.columns
                        ],
                        style_data_conditional=[
                                {'if': {'row_index': 'odd'},
                                 'backgroundColor': 'rgb(248, 248, 248)'
                                 }],
                        style_header={
                                'backgroundColor': 'rgb(230, 230, 230)',
                                'fontWeight': 'bold'
                                },        
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
                    
                    
                    #this displays what topic is selected
                    html.Div(id='datatable-interactivity-container'),
                    
                    html.H6('5-Day Rolling Average of Word Frequency in Topic', 
                            style={'textAlign': 'center',
                                   'color': colors['blue']}),
                            
                    dcc.Graph(id='graph_rolling_average_frequency'),
                    
                    html.H6('Word Frequency in Topic', 
                            style={'textAlign': 'center',
                                   'color': colors['blue']}),
                    
                            dcc.Graph(id='graph_frequency'),
                    
                    #this allows the other modules to read the selected topic
                    html.Div(id='topic_from_pulldown', 
                             style={'display': 'none'}),
                     
                    #this allows the other modules to read the selected date
                    html.Div(id='chosen_date_range_string', 
                             style={'display': 'none'}),
                    
                    #this allows other modules to use the query_results_df       
                    html.Div(id='query_results', 
                             style={'display': 'none'}),
                    
                    #this allows other modules to read the df that lists top 10
                    html.Div(id='top_10_results', 
                             style={'display': 'none'})
                    
                    ])

#displays topic after user chooses it
@app.callback(
    dash.dependencies.Output('output-container', 'children'),
    [dash.dependencies.Input('my-dropdown', 'value')])
def update_topic_selections(value):
    return 'You have selected "{}"'.format(value)

#stores topic in the hidden div so that other modules can use it
@app.callback(
        dash.dependencies.Output('topic_from_pulldown', 'children'), 
        [dash.dependencies.Input('my-dropdown', 'value')])
def get_topic(value):
    topic = value 
    return topic

#displays date range after user chooses it
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

#stores date_range in the hidden div so that other modules can use it
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

#uses user input to query db and make results df to generate table and graph
@app.callback(
    [dash.dependencies.Output('datatable-interactivity', 'data'),
     dash.dependencies.Output('query_results', 'children'),
     dash.dependencies.Output('top_10_results', 'children')],
    [dash.dependencies.Input('topic_from_pulldown', 'children'),
     dash.dependencies.Input('chosen_date_range_string', 'children')])
def update_table(topic, date_range):
    start_date_string = date_range.split(",")[0]
    end_date_string = date_range.split(" ")[1]
    query = get_query(topic, start_date_string, end_date_string )    
    requested_data = get_requested_data(query)
    query_data = get_query_data_df(requested_data,start_date_string,end_date_string)
    df = get_table_df(query_data)
   
    data=df.to_dict('records')
    
    jsonified_query_data = query_data.to_json(date_format='iso', orient='split')
    jsonified_df = df.to_json(date_format='iso', orient='split')
    
    return data,jsonified_query_data, jsonified_df

#makes rolling average graph
@app.callback(dash.dependencies.Output('graph_rolling_average_frequency', 'figure'), 
              [dash.dependencies.Input('query_results', 'children'),
               dash.dependencies.Input('top_10_results', 'children')])
def update_rolling_average_frequency(jsonified_query_data,jsonified_df):
    query_data = pd.read_json(jsonified_query_data, orient='split')
    df = pd.read_json(jsonified_df, orient='split')
    
    words = df.word.unique().tolist()
    
    fig = plotly.subplots.make_subplots(rows=3, 
                                        cols=1, 
                                        shared_xaxes=False,
                                        vertical_spacing=0.009,
                                        horizontal_spacing=0.009)
    
    fig['layout']['margin'] = {'l': 30, 'r': 10, 'b': 50, 't': 25}
    for word in words:
        word_results =  query_data[query_data['word'] == word][[
                'freq_rolling_average',
                'date']]\
                .sort_values(by=['date'])
                
        word_results.columns = ['frequency_5_day_average','date']
        
        fig.append_trace({'x':word_results.date,
                          'y':word_results.frequency_5_day_average,
                          'type':'scatter',
                          'name': word},1,1)
 
    return fig

#makes raw frequency graph
@app.callback(dash.dependencies.Output('graph_frequency', 'figure'), 
              [dash.dependencies.Input('query_results', 'children'),
               dash.dependencies.Input('top_10_results', 'children')])
def update_frequency_graph(jsonified_query_data,jsonified_df):
    query_data = pd.read_json(jsonified_query_data, orient='split')
    df = pd.read_json(jsonified_df, orient='split')
    
    words = df.word.unique().tolist()
    
    fig = plotly.subplots.make_subplots(rows=3, 
                                        cols=1, 
                                        shared_xaxes=False,
                                        vertical_spacing=0.009,
                                        horizontal_spacing=0.009)
    
    fig['layout']['margin'] = {'l': 30, 'r': 10, 'b': 50, 't': 25}
    for word in words:
        word_results =  query_data[query_data['word'] == word][[
                'freq_in_topic',
                'date']]\
                .sort_values(by=['date'])
                
        word_results.columns = ['frequency','date']
        fig.append_trace({'x':word_results.date,
                          'y':word_results.frequency,
                          'type':'scatter',
                          'name': word},1,1)

    return fig


if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0')
    