#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 24 12:49:43 2020

@author: JeffHalley
"""

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime.datetime(2019, 10, 19, 10, 00, 00),
        'email': [os.environ['email']],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 4,
        'retry_delay': timedelta(minutes=15),
      }

with DAG('update_data',
         catchup=False,
         default_args=default_args,
         schedule_interval= "@daily",
         # schedule_interval=None,
         ) as dag:
    opr_update_files = BashOperator(task_id='update_files',
                             bash_command= 'python3 /identifying_trending_topics_on_social_media/update_data_airflow/check_for_new_reddit_comments.py')

    opr_process_reddit_data = BashOperator(task_id='process_data',
                             bash_command= 'spark-submit --master spark://10.0.0.16:7077 --packages org.apache.hadoop:hadoop-aws:2.7.3, --packages org.postgresql:postgresql:42.2.5 /identifying_trending_topics_on_social_media/process_data/process_reddit_comments.py')



opr_update_files >> opr_process_reddit_data