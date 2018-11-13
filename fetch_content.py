#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr  1 08:14:31 2018

@author: kaushik
"""

import sys
import requests
from bs4 import BeautifulSoup
import tweepy
import time
import datetime

if(len(sys.argv) < 2):
    print("Enter the topics as arguments")
    print("Eg: fetch_content.py soccer basketball")
    sys.exit()

topics = sys.argv[1:]

path = '/home/charushi/MS/DIC/Lab3/Data/'
path=path +'Lab_3_Data/'
date_time = datetime.datetime.fromtimestamp(time.time()).strftime('%Y_%m_%d_%H_%M_%S')
nyt_path = path + 'nyt/'
tweet_path = path + 'tweets/'

tweet_count=200000

#to collect NYT data
nyt_url="https://api.nytimes.com/svc/search/v2/articlesearch.json"
api_param={'api-key':'36757bb00d214d5c90439a9da5bf1a9c'} #Change Credentials
#nyt_remove_content=[re.compile(regex) for regex in ['Advertisement','Supported by','Op-Ed Contributor','By.+','Collapse','SEE MY OPTIONS','Follow The New York Times.+','Opinion.*']]

#tweepy #Change credentials
consumer_key='CtljJh4AnNSgX2SMEmeAT5zi4'
consumer_secret='N13VIcErsmNWuU4FwWRBsM1EtZg1NAnXKUOSPJINIhFQqUkKpO'
access_token_key='75787947-ah5VJZAazd3pnzjeTubrKA8vhiucmtLEGq6QArpcz'
access_token_secret='a5SuYFhw8Ok6tTtDGGk9808yAJ6qKRd9M0rNRcR3m9pa0'
# Authenticate
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token_key, access_token_secret)
api=tweepy.API(auth,wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

def get_web_text_content(url):
    try:
        time.sleep(2)
        webpage = requests.get(url)
        soup = BeautifulSoup(webpage.content, 'html.parser')
        content = '\n'.join([t for t in [''.join(node.find_all(text=True)) for node in soup.find_all('p')] if len(t) > 0])
        print("Fetched content from ", url)
    except:
        print("Exception occurred while fetching ", url)
        return ''
    else:
        return content
    
def fetch_nyt(topic):
    for i in range(0,100):
        print('Starting page ', i+1)
        params = dict(api_param)
        params['q'] = topic
        params['page'] = str(i)
        try:
            response = requests.get(nyt_url,params=params)
        except:
            print('API Error: Error while fetching NYT metadata')
            return
        extract_nyt_article_content(response,topic,i)
        print('End of page ', i+1)
    
def extract_nyt_article_content(response,topic,page_number):
    #handle errors
    count = page_number * 10
    for r in response.json()['response']['docs']:
        content = get_web_text_content(r['web_url'])
        nytfile = open(path+topic+'/nytdata_' + topic+'_' + "%04d" % count + '_'+ date_time+'.txt','w+')
        nytfile.write(content)
        nytfile.close()
        count = count + 1
    
def fetch_tweets(topic):
    try:
        stream=tweepy.Cursor(api.search, q=topic ,lang='en', tweet_mode='extended',count=10000).pages()
        #tweets = api.search(q=topic,lang='en', tweet_mode='extended',count=tweet_count)
    except:
        print('API Error: Error while fetching tweets')
        return
    tweet_data=[]
    count=0
    twitter_file = open(tweet_path+'twitter_data_' + topic +'_'+ date_time +'.txt' , 'w+')
    try:
        for tweet_search_results in stream:
            for tweet_info in tweet_search_results:
                print('Collecting tweet ', count)
                if 'retweeted_status' in dir(tweet_info):
                    tweet=tweet_info.retweeted_status.full_text
                else:
                    tweet=tweet_info.full_text
                tweet.replace('\n',' ')
                #tweet_data.append(tweet)
                count = count+1
                twitter_file.write(tweet+'\n')
            if(count > tweet_count):
                break
    except:
        print('API Error: Error while streaming tweets')
    #content = '\n'.join(tweet_data)
    #twitter_file = open(tweet_path+'twitter_data_' + topic +'_'+ date_time +'.txt' , 'w+')
    #twitter_file.write(content)
    twitter_file.close()

for topic in topics:
    print("Fetching NYT Data for topic ", topic)
    fetch_nyt(topic)
    print("Fetching tweets for topic ", topic)
    #fetch_tweets(topic)
    print('Completed data fetching')
    print('Files available at ', path)
