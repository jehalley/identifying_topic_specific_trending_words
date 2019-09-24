#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 22 21:15:35 2019

@author: JeffHalley
"""

import pandas as pd
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table as dt
import dash_table
from dash.dependencies import Input, Output, State
import psycopg2


connection = psycopg2.connect(host='34.223.211.92', port=5431, user='jh', password='jh', dbname='word')
query = "SELECT topic, date, word, sub_freq_to_all_freq_ratio  FROM reddit_results WHERE topic = 'Basketball' AND '[2016-01-01, 2017-01-01]'::daterange @> date ORDER BY sub_freq_to_all_freq_ratio  DESC LIMIT 10;"
cursor = connection.cursor()
cursor.execute(query)
data = cursor.fetchall()
query_data = pd.DataFrame(data = data, columns=['topic', 'date', 'word', 'change_in_frequency_over_previous_day'])


app = dash.Dash(__name__)

app.layout = dash_table.DataTable(
    id='table',
    columns=[{"name": i, "id": i} for i in query_data.columns],
    data=query_data.to_dict('records'),
)

if __name__ == '__main__':
    app.run_server(debug=True)


#app = dash.Dash(__name__)
#application = app.server
#
#app.layout = html.Div([
#    dt.DataTable(
#        id = 'dt1', 
#        columns =  [{"name": i, "id": i,} for i in (query_data.columns)],
#    data = []
#    ),
#    html.Div([
#        html.Button(id='submit-button',                
#                children='Submit'
#        )
#    ]),    
#
#])
#
#print(query_data)
#
#@app.callback(Output('dt1','data'),
#            [Input('submit-button','n_clicks')],
#                [State('submit-button','n_clicks')])
#
#def update_datatable(n_clicks,csv_file):            
#    if n_clicks:                            
#        dfgb = query_data.groupby(['word']).sum()
#        data_1 = dfgb.to_dict('rows')
#        return data_1

#if __name__ == '__main__':
#    application.run(debug=False, port=8080)