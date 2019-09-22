#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 21 16:34:23 2019

@author: JeffHalley
"""

from flask_sqlalchemy import SQLAlchemy
import datetime

db = SQLAlchemy()

class BaseModel(db.Model):
    """Base data model for all objects"""
    __abstract__ = True

    def __init__(self, *args):
        super().__init__(*args)

    def __repr__(self):
        """Define a base way to print models"""
        return '%s(%s)' % (self.__class__.__name__, {
            column: value
            for column, value in self._to_dict().items()
        })

    def json(self):
        """
                Define a base way to jsonify models, dealing with datetime objects
        """
        return {
            column: value if not isinstance(value, datetime.date) else value.strftime('%Y-%m-%d')
            for column, value in self._to_dict().items()
        }


class Station(BaseModel, db.Model):
    """Model for the word table"""
    __tablename__ = 'reddit_results'

    id = db.Column(db.Integer, primary_key = True)
    topic = db.Column(db.String)
    word = db.Column(db.String)
    count = db.Column(db.BigInteger)
    count_per_day_all = db.Column(db.BigInteger)
    total_word_count_per_day_all = db.Column(db.BigInteger)
    total_word_count_per_day_topic = db.Column(db.BigInteger)
    sub_freq_to_all_freq_ratio = db.Column(db.Float)
    sub_freq_to_all_freq_ratio = db.Column(db.Float)
    change_in_rolling_average = db.Column(db.Float)
    date = db.Column(db.Date)