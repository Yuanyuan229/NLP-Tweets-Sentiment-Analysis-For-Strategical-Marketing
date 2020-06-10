import boto3
import json
import twint
import pandas as pd
import mysql.connector
from pandas.io import sql
from sqlalchemy import create_engine
import pymysql


### configure the tweet scrape
keyword = "valorant" # set search keywords
c = twint.Config()
c.Search = keyword
c.Limit = 100 # set search limits
c.Lang = "en" # set search language
c.Since = "2020-05-10 00:00:00" # set time interval
c.Pandas = True
c.Resume = "" # appoint a .txt file to store the scrape progress

### Scrape Tweet
twint.run.Search(c)

### Convert Tweets to Pandas df
df = twint.storage.panda.Tweets_df

def twint_to_pandas(columns):
    return twint.output.panda.Tweets_df[columns]

df = twint_to_pandas(['id', 'date', 'username', 'tweet'])


### Get sentiment score
# need to fill in your AWS credential
comprehend = boto3.client(\
    service_name='comprehend',\
    region_name='us-east-1',\
    aws_access_key_id="",\
    aws_secret_access_key="")

def get_sentiment(df):
    positive = []
    neutral = []
    negative = []
    mixed = []
    for tweet in df['tweet']:
        sentiment = comprehend.detect_sentiment(Text=tweet, LanguageCode='en')
        positive.append(sentiment['SentimentScore']['Positive'])
        neutral.append(sentiment['SentimentScore']['Neutral'])
        negative.append(sentiment['SentimentScore']['Negative'])
        mixed.append(sentiment['SentimentScore']['Mixed'])
    df['positive'] = positive
    df['neutral'] = neutral
    df['negative'] = negative
    df['mixed'] = mixed
    return df

processed_df = get_sentiment(df)


### Create database
connection = mysql.connector.connect(
    host="127.0.0.1",
    port=3306,
    user="root",
    password="")
cursor = connection.cursor()

cursor.execute("CREATE DATABASE IF NOT EXISTS tweet_sentiment;")
cursor.execute("USE tweet_sentiment")
cursor.execute("""
               CREATE TABLE IF NOT EXISTS {}_sentiment 
               (id VARCHAR(20) PRIMARY KEY, 
               date DATETIME, 
               username VARCHAR(30), 
               tweet VARCHAR(1000),
               positive FLOAT(4,3),
               neutral FLOAT(4,3),
               negative FLOAT(4,3),
               mixed FLOAT(4,3))""".format(keyword))

### Write result to database
database_username = 'root'
database_password = ''
database_ip = '127.0.0.1:3306'
database_name = 'tweet_sentiment'
engine = create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.
                       format(database_username, database_password, 
                           database_ip, database_name))

conn = engine.connect()

processed_df.to_sql(con=conn, name="{}_sentiment".format(keyword), if_exists='append', index=False)