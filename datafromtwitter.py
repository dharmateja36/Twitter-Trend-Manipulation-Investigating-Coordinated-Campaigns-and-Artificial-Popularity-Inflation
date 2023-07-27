#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sys
import numpy as np 
import os 
import pandas as pd 
import snscrape.modules.twitter as Sntwitter
import boto3
from io import StringIO 

# Query ='(#HBDBelovedAlluArjun) until:2018-04-08 since:2018-04-07 -filter:replies'
Query ='(#NTRBdayFestBegins) until:2020-05-05 since:2020-04-30 -filter:replies'
df = []
for i,tweet in enumerate(Sntwitter.TwitterSearchScraper(Query).get_items()) : 
    if i > 3 : 
        break
    df.append([tweet.card,tweet.cashtags,tweet.content,tweet.conversationId,tweet.coordinates,
               tweet.date,tweet.hashtags,tweet.id,tweet.inReplyToTweetId,tweet.inReplyToUser,tweet.json,tweet.lang,
               tweet.likeCount,tweet.links,tweet.mentionedUsers,tweet.place,tweet.quoteCount,tweet.quotedTweet,
               tweet.rawContent,tweet.renderedContent,tweet.replyCount,tweet.retweetCount,tweet.retweetedTweet,
               tweet.source,tweet.user,tweet.username,tweet.vibe,tweet.viewCount 
              ])
data = pd.DataFrame(df,columns=['card','cashtags','content','conversationId','coordinates',
                                'date','hashtags','id','inReplyToTweetId','inReplyToUser','json',
                               
                               'lang','likeCount','links','mentionedUsers','place','quoteCount','quotedTweet'
                                
                                ,'rawContent','renderedContent','replyCount','retweetCount','retweetedTweet',
                                
                                'source','user','username','vibe','viewCount'
                               
                               ])
bucket = 'twitterdatadownload' # already created on S3
# csv_buffer = StringIO()
# data.to_csv(csv_buffer)
# s3_resource = boto3.resource('s3')
# s3_resource.Object(bucket, 'NTRBdayFestBegins.csv').put(Body=csv_buffer.getvalue())

json_buffer = StringIO()
data.to_json(json_buffer, orient='records')
s3_resource = boto3.resource('s3')
s3_resource.Object(bucket, 'NTRBdayFestBegins.json').put(Body=json_buffer.getvalue())

