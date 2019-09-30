#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 20 09:18:10 2019

@author: JeffHalley
"""

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from datetime import date, timedelta
import plotly.graph_objs as go
import pandas as pd
import os
import psycopg2
import json
import textwrap



#initial dash obj
app = dash.Dash()

#types of data
activity = ['Aggregate','Push','Watch','Fork']

#initial app layout
app.layout = html.Div([

    html.Div(
        ' ',
        style={'color':'blue', 'padding-right':10, 'width':'50%', 'display':'inline-block'}
        ),
        html.H2(
           children='GitTrends',
           style={
               'textAlign': 'center',
               'color': 'Black'
           }
        ),
        html.H4(
           children='20 Most popular repositories',
           style={
               'textAlign': 'center'
           }
        ),

        html.Div([
            dcc.Dropdown(
                id='activity',
                options = [
                    {'label': 'Aggregate Activity', 'value': 'aggregate'},
                    {'label': 'Push Events', 'value': 'push'},
                    {'label': 'Star Events', 'value': 'watch'},
                    {'label': 'Fork Events', 'value': 'fork'}
                ],
                value='aggregate'
            )
        ],
        style={'width': '30%', 'padding-left': '35%', 'padding-top':'5%', 'padding-bottom':'5%'}
        ),

    html.Div([
        dcc.Slider(
            id='slider_updatetime',
            min = 0,
            max = 100,
            step = None,
            marks={
                0: {'label': 'Last Week', 'style': {'color': '#77b0b1'}},
                10: {'label': 'Last Month'},
                50: {'label': 'Last Three Months'},
                100: {'label': 'Last Six Months', 'style': {'color': '#f50'}}
                },
            ),
    ],
    style={'width': '90%','padding-left':'5%', 'padding-right':'5%', 'padding-bottom':'5%'}),

    html.Div([
        dcc.Graph(
            id='barplot-topactivity',
        )
    ], style={'width': '75%', 'display': 'inline-block', 'padding': '0 20'}),

    html.Div([
        dcc.Markdown(children='''
            repository info
            ''', id='repo-stats'),
        ], style={'display': 'inline-block', 'width': '25%', 'vertical-align':'top', 'padding-top':'8%'}),


])

#utility function for converting slider to time
def getSliderTime(slider_updatetime):
    today = date.today()
    start_time = today
    if slider_updatetime == 0:
        start_time = today - timedelta(days = 7)
    elif slider_updatetime == 10:
        start_time = today - timedelta(days = 30)
    elif slider_updatetime == 50:
        start_time = today - timedelta(days = 90)
    elif slider_updatetime == 100:
        start_time = today - timedelta(days = 180)
    return start_time

#call back function for updating the barplot
@app.callback(
    Output('barplot-topactivity', 'figure'),
    [Input('activity', 'value'),
     Input('slider_updatetime', 'value')])
def update_graph(activity, slider_updatetime):
    # Get DATA from postgres
    postgres_host = os.environ['POSTGRES_HOST']
    postgres_username = os.environ['POSTGRES_USERNAME']
    postgres_password = os.environ['POSTGRES_PASSWORD']
    connection = psycopg2.connect(host=postgres_host,
                                  port ='5432',
                                  dbname='insightproject',
                                  user=postgres_username,
                                  password=postgres_password)
    cursor = connection.cursor()
    table_name = activity + 'event_table'
    start_time = getSliderTime(slider_updatetime)
    y_label = "label"
    if activity == 'aggregate':
        y_label = "aggregate score"
        cursor.execute("select repo_name, sum(score)  from {} where create_date >= \'{}\' group by repo_name ORDER BY sum(score) desc LIMIT 20".format(table_name, start_time))
    else:
        y_label = "no. of " + activity.lower() + " events"
        y_label = "no. of " + activity.lower() + " events"
        cursor.execute("select repo_name, sum(count)  from {} where create_date >= \'{}\' group by repo_name ORDER BY sum(count) desc LIMIT 20".format(table_name, start_time))
    data = cursor.fetchall()
    repo_list = []
    count_list = []
    for i in range(len(data)):
        repo_list.append(data[i][0])
        count_list.append(data[i][1])
    return {
        'data': [go.Bar(
            x=repo_list,
            y=count_list,
            opacity= 0.5,
            marker={'color':'#800080'}
        )],
        'layout': go.Layout(
            yaxis={'title': '{}'.format(y_label)},
            legend={'x': 0, 'y': 1},
            hovermode='closest',
            plot_bgcolor='rgb(240,248,255)'
        )
    }

# callback for updating the repository information
@app.callback(
    Output('repo-stats', 'children'),
    [Input('barplot-topactivity', 'clickData'),
     Input('slider_updatetime', 'value')])
def display_click_data(clickData, slider_updatetime):
    if clickData:
        resp = str(clickData)
        repo_name = resp.split(':')[5].split(',')[0]
        postgres_host = 'ec2-54-214-118-72.us-west-2.compute.amazonaws.com'  #os.environ['POSTGRES_HOST']
        postgres_username = 'db_select'  #os.environ['POSTGRES_USERNAME']
        postgres_password = 'db_select' #os.environ['POSTGRES_PASSWORD']
        connection = psycopg2.connect(host=postgres_host,
                                  port ='5432',
                                  dbname='insightproject',
                                  user=postgres_username,
                                  password=postgres_password)
        cursor = connection.cursor()
        start_time = getSliderTime(slider_updatetime)
        # Get the user list and create markup if user info exists
        cursor.execute("select user_name, sum(count) from repo_users where repo_name = {} AND create_date >= \'{}\' group by user_name ORDER BY sum(count) desc LIMIT 5".format(repo_name, start_time))
        data = cursor.fetchall()
        r_name = repo_name.strip()
        r_name = r_name[1:len(r_name)-1]
        out_str = '#### REPOSITORY : ' + r_name + '\n'
        if len(data) > 0:
            out_str = out_str + 'USERS LIST:'
        if len(data) > 0:
            out_str = out_str + '\n'
        for i in range(len(data)):
            out_str = out_str + '- ' + data[i][0] + '\n'
        if len(data) > 0:
            out_str = out_str + '\n'
        # Get repo info and add to markup if repo details exist
        cursor.execute("select * from repo_details where repo_name={} order by create_date desc LIMIT 1".format(repo_name))
        data = cursor.fetchall()
        info = False
        if len(data)>0:
            out_str = out_str + '\n REPOSITORY INFO: \n'
            info = True
            forks = data[0][3]
            languages = data[0][5]
            lstr = "".join(x for x in languages)
            out_str = out_str + '- Languages used:' + lstr + '\n'
            out_str = out_str + '- Has Wiki:' + str(data[0][4]) + '\n'
            out_str = out_str + '- No. of Forks:' + str(data[0][3]) + '\n'
            out_str = out_str + '- No. of Open Issues:' + str(data[0][6]) + '\n'
            out_str = out_str + '- No. of Stars:' + str(data[0][7]) + '\n'
            out_str = out_str + '- No. of Watchers:' + str(data[0][8]) + '\n'
            out_str = out_str + '\n'
        # add repo url to the markup
        url = 'https://github.com/'+r_name + '/'
        out_str = out_str + 'visit ' + '[here](' + url + ')' +' for more details \n'
        return textwrap.dedent(out_str)


if __name__ == '__main__':
    app.css.append_css({'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'})
    app.run_server(debug = True, host='0.0.0.0', port=80)