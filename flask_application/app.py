#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 21 16:25:04 2019

@author: JeffHalley
"""
from flask import Flask
from models import db

app = Flask(__name__)

POSTGRES = {
    'user': 'jh',
    'pw': 'jh',
    'db': 'word',
    'host': '54.245.175.187',
    'port': '5431',
}

app.config['DEBUG'] = True
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://%(user)s:\
%(pw)s@%(host)s:%(port)s/%(db)s' % POSTGRES


@app.route("/")
def main():
    return 'Hello World !'

if __name__ == '__main__':
    app.run()