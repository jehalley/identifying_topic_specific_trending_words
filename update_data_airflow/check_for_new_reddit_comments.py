#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 24 09:07:02 2020

@author: JeffHalley
"""
from bs4 import BeautifulSoup
import boto3
from boto.s3.connection import S3Connection
import os
import pathlib
import requests
import zstandard


def get_newest_comments_file_name():
    page = requests\
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
    #download file
    url = 'https://files.pushshift.io/reddit/comments/' + latest_comment_file_name
    target_path = '/reddit_comments/compressed/' + latest_comment_file_name
    response = requests.get(url, stream=True)
    handle = open(target_path, "wb")
    for chunk in response.iter_content(chunk_size=512):
        if chunk:  # filter out keep-alive new chunks
            handle.write(chunk)
    #decompress
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
    file_name = file_path.split(sep = '/')[-1]
    key = '2019_comments/' + file_name
    s3 = boto3.client('s3')
    s3.upload_file(key,bucket_name, file_name)

if __name__ == '__main__':   
    latest_comment_file_name = get_newest_comments_file_name()
    already_up_to_date = check_if_up_to_date(latest_comment_file_name)
    if already_up_to_date is False:
        file_path = get_newest_comments_file(latest_comment_file_name)
        upload_newest_comments_file(latest_comment_file_name)
        print('file successfully uploaded to s3')
    else:
        print('s3 already contains most up to date comment file')
        