#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 24 12:49:43 2020

@author: JeffHalley
"""

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from bs4 import BeautifulSoup
import boto3
from boto.s3.connection import S3Connection
from datetime import datetime, timedelta
import os
import pathlib
import requests
import zstandard


# below are utility functions to check whether S3 contains the most up to date comments from Reddit
def get_newest_comments_file_name():
    page = requests \
        .get('https://files.pushshift.io/reddit/comments/').text
    soup = BeautifulSoup(page, 'html.parser')
    a_tags = soup.find_all('a')
    latest_comment_file_name = ''
    for i in a_tags:
        if i['href'][2:7] == 'RC_20':
            latest_comment_file_name = i['href'][2:]
    return latest_comment_file_name


def check_if_up_to_date(latest_comment_file_name):
    conn = S3Connection(os.environ['aws_access'], os.environ['aws_secret_key'])
    bucket = conn.get_bucket('s3a://jeff-halley-s3/')
    for key in bucket.list(prefix='2019_comments/'):
        if key == latest_comment_file_name:
            return True

    return False

def get_newest_comments_file(latest_comment_file_name):
    # download file
    url = 'https://files.pushshift.io/reddit/comments/' + latest_comment_file_name
    target_path = '/reddit_comments/compressed/' + latest_comment_file_name
    response = requests.get(url, stream=True)
    handle = open(target_path, "wb")
    for chunk in response.iter_content(chunk_size=512):
        if chunk:  # filter out keep-alive new chunks
            handle.write(chunk)
    # decompress
    input_file = pathlib.Path(target_path)
    destination_dir = '/reddit_comments/compressed/'
    with open(input_file, 'rb') as compressed:
        decomp = zstandard.ZstdDecompressor()
        output_path = pathlib.Path(destination_dir) / input_file.stem
        with open(output_path, 'wb') as destination:
            decomp.copy_stream(compressed, destination)

    return output_path


def upload_newest_comments_file(file_path):
    bucket_name = 'jeff-halley-s3/'
    file_name = file_path.split(sep='/')[-1]
    key = '2019_comments/' + file_name
    s3 = boto3.client('s3')
    s3.upload_file(key, bucket_name, file_name)

def should_run():
    '''this function checks the Reddit comments aggregator for a newer file than is present in the S3. If it is not
    present then the data has not be analyzed and so the spark job will run on it
    '''
    latest_comment_file_name = get_newest_comments_file_name()
    already_up_to_date = check_if_up_to_date(latest_comment_file_name)
    if already_up_to_date is False:
        file_path = get_newest_comments_file(latest_comment_file_name)
        upload_newest_comments_file(file_path)
        return "start_spark_cluster"
    else:
        return "data_is_already_up_to_date"

default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2019, 10, 19, 10, 00, 00),
        'email': [os.environ['email']],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 4,
        'retry_delay': timedelta(minutes=15),
      }

with DAG('update_data_if_not_already_up_to_date',
         catchup=False,
         default_args=default_args,
         schedule_interval="@daily",
         ) as dag:

    cond = BranchPythonOperator(
        task_id='check_if_data_is_up_to_date',
        python_callable=should_run,
        dag=dag,
    )

    data_is_already_up_to_date = DummyOperator(task_id='data_is_already_up_to_date')

    start_spark_cluster = BashOperator(task_id='start_spark_cluster',
                                       bash_command='/usr/local/spark/sbin/start-all.sh')

    process_reddit_data = BashOperator(task_id='process_data',
                             bash_command='/usr/local/spark/sbin/spark-submit --master spark://10.0.0.16:7077 --packages org.apache.hadoop:hadoop-aws:2.7.3, --packages org.postgresql:postgresql:42.2.5 /identifying_trending_topics_on_social_media/process_data/process_reddit_comments.py')

    stop_spark_cluster = BashOperator(task_id='stop_spark_cluster',bash_command='/usr/local/spark/sbin/stop-all.sh')


cond >> start_spark_cluster >> process_reddit_data >> stop_spark_cluster
cond >> data_is_already_up_to_date

