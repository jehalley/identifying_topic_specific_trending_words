#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 11 08:39:16 2019

@author: JeffHalley
"""

from bs4 import BeautifulSoup
from bs4 import NavigableString
import requests
import pandas as pd

def get_general_topics_df(page):
    soup = BeautifulSoup(open(page), 'html.parser')
    subreddit_topics = pd.DataFrame(columns=['topic', 'subreddit'])
    h2s_and_h3s = soup.find_all(['h2','h3'])
    for h2 in h2s_and_h3s:
        header = h2.strong
        if header is not None:
            topic = header.text
        
        node = h2.next_element.next_element.next_element.next_element
        if isinstance(node, NavigableString):
            continue
        if node is not None:
            subreddit = node.text.splitlines()
        subreddits_df = pd.DataFrame(subreddit, columns = ['subreddit'])
        subreddits_df['topic'] = topic
        subreddit_topics = pd.concat([subreddit_topics, subreddits_df],
                                     ignore_index=True, sort=True)
    
    for h2 in h2s_and_h3s:
        header = h2.em
        if header is not None:
            topic = header.text
        
        node = h2.next_element.next_element.next_element.next_element
        if isinstance(node, NavigableString):
            continue
        if node is not None:
            subreddit = node.text.splitlines()
        subreddits_df = pd.DataFrame(subreddit, columns = ['subreddit'])
        subreddits_df['topic'] = topic
        subreddit_topics = pd.concat([subreddit_topics, subreddits_df],
                                     ignore_index=True, sort=True)
    return subreddit_topics

def get_vg_topic_df(vg_page):
    vg_subreddits = []
    vg_soup = BeautifulSoup(open(vg_page), 'html.parser')
    wiki = vg_soup.find("div", {"class": "md wiki"})
     
    for p in wiki.find_all('p'):                         
        for a in p.find_all('a', {'rel': 'nofollow'}):   
     
            tag_text = str(a.text)                       
     
            if tag_text.startswith('/r/'):               
                vg_subreddits.append(tag_text)                 
    
    vg_subs = pd.DataFrame(columns=['topic', 'subreddit'])
    vg_subs['subreddit'] = vg_subreddits
    vg_subs['topic'] = "Video Games"
    return vg_subs

def get_subreddit_topics_df(general_topics_df, vg_subs_df):
    subreddit_topics_df = pd.concat([general_topics_df, vg_subs_df], 
                                    ignore_index=True, sort=True)
    subreddit_topics_df = subreddit_topics_df.groupby('subreddit')['topic']\
    .apply(','.join)
    subreddit_topics_df = subreddit_topics_df.to_frame()
    subreddit_topics_df.reset_index(level=0, inplace=True)
    
    return subreddit_topics_df
    
def clean_subreddit_topics_df(subreddit_topics_df):
    #this gets rid of the r/ and / in front of subreddit names
    subreddit_topics_df['subreddit'] = subreddit_topics_df['subreddit']\
    .str.replace('r/','')
    
    subreddit_topics_df['subreddit'] = subreddit_topics_df['subreddit']\
    .str.replace('/','')
    
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('"','')
    
    #for some reason the scraping put wallpapers on a lot of things that it doesn't belong on
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace(',Wallpapers','')
    
    #Netflix related got put on a lot of things that are not netflix related
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('TV,Netflix Related','TV,NR')
    
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace(',Netflix Related','')
    
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('TV,NR','TV,Netflix Related')
    
    #automotive classified as writing automotive
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Automotive,Writing','Automotive')
    
    #outdoors classified as outdoors car companies
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Outdoors,Car companies','Outdoors')
    
    #Design/carcompanies
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Design,Car companies','Design')
    
    #nostaliga/time has politics in it
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Nostalgia/Time,Politics','Nostalgia/Time')
    
    #photography film all have car companies
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Photography/Film,Car companies','Photography/Film')
    
    #cryptocurrency in a lot of nonrelated things
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace(',CryptoCurrency','')
    
    #self improvement in a lot of thing it shouln't be in
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Self-Improvement,Sex','Self-Improvement')
    
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Self-Improvement,Programming','Programming,Self-Improvement')
    
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Self-Improvement,','')
    
    #politics in things it shouldn't be in Unexpected,Politics
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Shitty,Politics','Shitty')
    
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Unexpected,Politics','Unexpected')
    
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Visually Appealing,Politics','Visually Appealing')
    
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Categorize Later,Politics','Categorize Later')

    #instruments in with sports
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Sports,Instruments','Sports')
    
    #scary weird mixed in with support
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Support,Scary/Weird','Support')
    
    #scary weird mixed in with support
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Tech Related,Car companies','Tech Related')

    #Tech Support
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Tools,Tech Support','Tools')
    
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Travel,Tech Support','Travel')

    #TV,Soccer
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('TV,Soccer','TV')
    
    #Neckbeard mixed with cute
    #Cute,Neckbeard
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Cute,Neckbeard','Cute')
    
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Dogs,Neckbeard','Dogs')
    
    #Conspiracy,Dogs
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Conspiracy,Dogs','Conspiracy')
    
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Cringe,Dogs','Cringe')
    
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Facebook,Dogs','Cringe')
    
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Meta,Dogs','Cringe')
    
    #CringScifi
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('Cringe,Sci-fi','Cringe')
    
    #General,Physics
    subreddit_topics_df['topic'] = subreddit_topics_df['topic']\
    .str.replace('General,Physics','General')

    
    return subreddit_topics_df

    
#page = requests\.get('http://www.reddit.com/r/ListOfSubreddits/wiki/listofsubreddits')
    #started getting bad responses so you might have to download directly
page = "/Users/JeffHalley/Downloads/list_of_subreddits.htm"
general_topics_df = get_general_topics_df(page)
#vg_page = requests.get('https://www.reddit.com/r/ListOfSubreddits/wiki/games50k')
vg_page = "/Users/JeffHalley/Downloads/video_game_list_reddit.htm"
vg_subs_df = get_vg_topic_df(vg_page)
subreddit_topics_df = get_subreddit_topics_df(general_topics_df, vg_subs_df)
clean_subreddit_topics_df = clean_subreddit_topics_df(subreddit_topics_df)
clean_subreddit_topics_df.to_csv('subreddit_topics.csv')