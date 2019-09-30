#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 20 09:30:53 2019

@author: JeffHalley
"""

from flask import Flask
import psycopg2
app = Flask(__name__)

@app.route("/")
def hello():
    db = psycopg2.connect(host='35.162.89.159', port=5431, user='jh',
                          password='jh', dbname='word')
    return "Hello World from Flask"

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)