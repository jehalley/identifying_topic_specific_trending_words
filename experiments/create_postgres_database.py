#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 19 17:06:10 2019

@author: JeffHalley
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT # <-- ADD THIS LINE

con = psycopg2.connect(dbname='word',
      user=self.user_name, host='',
      password=self.password)

con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) # <-- ADD THIS LINE

cur = con.cursor()

# Use the psycopg2.sql module instead of string concatenation 
# in order to avoid sql injection attacs.
cur.execute(sql.SQL("CREATE DATABASE {}").format(
        sql.Identifier(self.db_name))
    )

conn = psycopg2.connect(host="localhost",database="word", user="postgres", password="postgres")