from pymongo import MongoClient
from bson import json_util, ObjectId
import pandas as pd
from pandas import DataFrame
from pandas.io.json import json_normalize
import numpy as np
import requests
from requests_oauthlib import OAuth1
import json
import configparser

'''
Altered/Updated example from the O'Reilly book 
Python for Data Analysis by Wes McKinney
'''

# load mongo connection details and Twitter API auth details from a config file
config = configparser.ConfigParser()
config.read('./configs/twitter_mongo.ini')

CLIENT_KEY = config.get('Twitter', 'client_key')
CLIENT_SECRET = config.get('Twitter', 'client_secret')
RESOURCE_OWNER_KEY = config.get('Twitter', 'resource_owner_key')
RESOURCE_OWNER_SECRET = config.get('Twitter', 'resource_owner_secret')

client = MongoClient(config.get('Mongo', 'host'), int(config.get('Mongo', 'port')))

# create tweets collection
tweets = client.db.tweets

# get oauth using Twitter details
def get_oauth():
    oauth = OAuth1(CLIENT_KEY,
                client_secret=CLIENT_SECRET,
                resource_owner_key=RESOURCE_OWNER_KEY,
                resource_owner_secret=RESOURCE_OWNER_SECRET)
    return oauth

# get tweets relating to python from the Twitter API
oauth = get_oauth()
url = 'https://api.twitter.com/1.1/search/tweets.json?q=python'
data = json.loads(requests.get(url, auth=oauth).text)

for tweet in data['statuses']:
    tweets.insert_one(tweet)

#mongo_data = tweets.find({'user.screen_name': 'username'}) # find tweets relating to one user
mongo_data = tweets.find({}) # find all tweets

# load MongoDB data as JSON data and flatten using json_normalize
sanitized = json.loads(json_util.dumps(mongo_data))
normalized = json_normalize(sanitized)

# create data frame containing four columns and remove duplicates
df = pd.DataFrame(normalized, columns=['user.screen_name', 'user.name', 'created_at', 'text'])
df.columns=['Twitter Username','Twitter Name','Tweet Created','Tweet Text']
df = df.drop_duplicates()

print(df)
