#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, MapType

from pyspark.ml.feature import Tokenizer

from pyspark.sql.window import Window

from pyspark.sql.functions import lit,col,to_timestamp,count,isnan, when,lower,trim,split, explode, regexp_replace,collect_list,struct,sum,lit, array, explode,concat_ws,countDistinct,avg,lag,unix_timestamp

# create a spark session
spark = SparkSession.builder.appName("Twitter Trends Analysis").getOrCreate()

# create a list of file paths and their corresponding source hashtags
file_sources = [
    ("s3://twitterdatadownload/AdvanceHBDMaheshBabu.json", "AdvanceHBDMaheshBabu"),
    ("s3://twitterdatadownload/gobackmodi.json", "gobackmodi"),
    ("s3://twitterdatadownload/HBDBelovedAlluArjun.json", "HBDBelovedAlluArjun"),
    ("s3://twitterdatadownload/HappyBirthdayNTR.json", "HappyBirthdayNTR"),
    ("s3://twitterdatadownload/HBDDarlingPrabhas.json", "HBDDarlingPrabhas"),
    ("s3://twitterdatadownload/HBDLeaderPawanKalyan.json", "HBDLeaderPawanKalyan"),
    ("s3://twitterdatadownload/HBDMaheshBabu.json", "HBDMaheshBabu"),
    ("s3://twitterdatadownload/NTRBdayFestBegins.json", "NTRBdayFestBegins"),
    ("s3://twitterdatadownload/tnwelcomesmodi.json", "tnwelcomesmodi")
]

schema = StructType([
    StructField("Unnamed: 0", LongType(), True),
    StructField("card", StringType(), True),
    StructField("cashtags", StringType(), True),
    StructField("content", StringType(), True),
    StructField("conversationId", LongType(), True),
    StructField("coordinates", StringType(), True),
    StructField("date", StringType(), True),
    StructField("hashtags", StringType(), True),
    StructField("id", LongType(), True),
    StructField("inReplyToTweetId", StringType(), True),
    StructField("inReplyToUser", StringType(), True),
    StructField("json", StringType(), True),
    StructField("lang", StringType(), True),
    StructField("likeCount", LongType(), True),
    StructField("links", StringType(), True),
    StructField("mentionedUsers", StringType(), True),
    StructField("place", StringType(), True),
    StructField("quoteCount", LongType(), True),
    StructField("quotedTweet", StringType(), True),
    StructField("rawContent", StringType(), True),
    StructField("renderedContent", StringType(), True),
    StructField("replyCount", LongType(), True),
    StructField("retweetCount", LongType(), True),
    StructField("retweetedTweet", StringType(), True),
    StructField("source", StringType(), True),
    StructField("user", StringType(), True),
    StructField("username", StringType(), True),
    StructField("vibe", StringType(), True),
    StructField("viewCount", StringType(), True),
    StructField("sourcehashtag", StringType(), True)
])

# create an empty dataframe to store the combined data
twitterdataanalysis = spark.createDataFrame([], schema=schema)

# iterate over the files and append the data to the combined dataframe
for file_path, source in file_sources:
    df = spark.read.json(file_path)
    df = df.withColumn('sourcehashtag', lit(source))
    twitterdataanalysis = twitterdataanalysis.unionAll(df)


# In[2]:


# check the number of missing values in each column for the twitterdataanalysis dataframe
twitterdataanalysis.groupBy("sourcehashtag").agg(*[count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in twitterdataanalysis.columns]).show()


# In[3]:


spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# drop the 'Unnamed: 0' column
twitterdataanalysis = twitterdataanalysis.drop('Unnamed: 0')

# cast the 'date' column to a timestamp data type
twitterdataanalysis = twitterdataanalysis.withColumn('date', to_timestamp(col('date'), 'yyyy-MM-dd HH:mm:ss'))


# In[4]:


# set timeParserPolicy to LEGACY to parse timestamp in yyyy-MM-dd HH:mm:ss format
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# drop unwanted columns
columns_to_drop = ['Unnamed: 0', 'card', 'cashtags', 'coordinates', 'inReplyToTweetId', 'inReplyToUser',
                   'json', 'links', 'mentionedUsers', 'quotedTweet', 'rawContent', 'renderedContent',
                   'retweetedTweet', 'source', 'viewCount','place', 'vibe']
twitterdataanalysis = twitterdataanalysis.drop(*columns_to_drop)

# cast the 'date' column to a timestamp data type
twitterdataanalysis = twitterdataanalysis.withColumn('date', to_timestamp(col('date'), 'yyyy-MM-dd HH:mm:ss'))


# In[5]:


# fill missing values in numeric columns with 0
twitterdataanalysis = twitterdataanalysis.fillna({'likeCount': 0, 'quoteCount': 0, 'replyCount': 0, 'retweetCount': 0})

# replace empty strings with None values
twitterdataanalysis = twitterdataanalysis.withColumn('content', when(twitterdataanalysis['content'] == '', None).otherwise(twitterdataanalysis['content']))


# In[6]:


# Convert text to lowercase
twitterdataanalysis = twitterdataanalysis.withColumn("content", lower(twitterdataanalysis.content))

# Remove leading/trailing spaces and duplicate whitespaces
twitterdataanalysis = twitterdataanalysis.withColumn("content", trim(regexp_replace(twitterdataanalysis.content, " +", " ")))


# In[7]:


twitterdataanalysis = twitterdataanalysis.withColumn("content", lower(trim(twitterdataanalysis.content)))


# In[8]:


tokenizer = Tokenizer(inputCol="content", outputCol="tokens")
twitterdataanalysis = tokenizer.transform(twitterdataanalysis)


# In[9]:


# Remove square brackets and quotes from hashtags
df = twitterdataanalysis.withColumn('hashtags', regexp_replace('hashtags', "[\[\]']", ''))

# Split the hashtags string into a list of hashtags
df = df.withColumn('hashtags', split(df.hashtags, ', '))

# Explode the hashtags list into individual rows
df = df.select(explode(df.hashtags).alias('hashtag'), 'sourcehashtag')

# Count the occurrences of each hashtag for each sourcehashtag
df = df.groupBy('sourcehashtag', 'hashtag').agg(count('*').alias('count'))

# Sort the dataframe in descending order by count
df = df.orderBy('sourcehashtag', df['count'].desc())

# Extract the top 10 hashtags and their counts for each sourcehashtag
top_hashtags = df.groupBy('sourcehashtag').agg(collect_list(struct(df['hashtag'], df['count'])).alias('top_hashtags'))


# In[10]:


# Group by sourcehashtag and sum up the counts
aggregations = twitterdataanalysis.groupBy('sourcehashtag') \
    .agg(sum('likeCount').alias('sum_of_likes'), \
         sum('quoteCount').alias('sum_of_quotes'), \
         sum('replyCount').alias('sum_of_replies'), \
         sum('retweetCount').alias('sum_of_retweets'))

# Add the metric column
aggregations = aggregations.select('sourcehashtag', \
                                   concat_ws('_', lit('sum_of_likes'), lit('likes')).alias('metric'), \
                                   aggregations['sum_of_likes'].alias('value')) \
                           .union( \
                          aggregations.select('sourcehashtag', \
                                              concat_ws('_', lit('sum_of_quotes'), lit('quotes')).alias('metric'), \
                                              aggregations['sum_of_quotes'].alias('value'))) \
                           .union( \
                          aggregations.select('sourcehashtag', \
                                              concat_ws('_', lit('sum_of_replies'), lit('replies')).alias('metric'), \
                                              aggregations['sum_of_replies'].alias('value'))) \
                           .union( \
                          aggregations.select('sourcehashtag', \
                                              concat_ws('_', lit('sum_of_retweets'), lit('retweets')).alias('metric'), \
                                              aggregations['sum_of_retweets'].alias('value')))


# In[11]:


distinct_users_df = twitterdataanalysis.groupBy("sourcehashtag") \
    .agg(countDistinct(col("user")).alias("value")) \
    .withColumn("metric", lit("distinct_users")) \
    .select("sourcehashtag", "metric", "value")

# Add dummy column to make number of columns same as aggregations DataFrame
distinct_users_df = distinct_users_df.select("sourcehashtag", "metric", "value")


# In[12]:


# Average retweets per account
avg_retweets_per_account = twitterdataanalysis \
    .groupBy("sourcehashtag", "user") \
    .agg(avg("retweetCount").alias("avg_retweets_per_user")) \
    .groupBy("sourcehashtag") \
    .agg(avg("avg_retweets_per_user").alias("value")) \
    .withColumn("metric", lit("avg_retweets_per_account")) \
    .select("sourcehashtag", "metric", "value")


# In[13]:


# total tweets for each hashtag
total_tweets_df = twitterdataanalysis.groupBy("sourcehashtag") \
    .agg(count("*").alias("value")) \
    .withColumn("metric", lit("total_tweets")) \
    .select("sourcehashtag", "metric", "value")


# In[14]:


# Calculate total number of tweets for each sourcehashtag category
total_tweets = twitterdataanalysis.groupBy('sourcehashtag').agg(count('*').alias('total_tweets'))

# Calculate number of tweets where like count is less than retweet count for each sourcehashtag category
less_likes_than_retweets = twitterdataanalysis.filter(twitterdataanalysis.likeCount < twitterdataanalysis.retweetCount)
less_likes_than_retweets = less_likes_than_retweets.groupBy('sourcehashtag').agg(count('*').alias('less_likes_than_retweets'))

# Join the two dataframes and calculate the percentage for each sourcehashtag category
percentage_df = total_tweets.join(less_likes_than_retweets, on='sourcehashtag', how='left_outer')
percentage_df = percentage_df.withColumn('percentage', (percentage_df.less_likes_than_retweets / percentage_df.total_tweets) * 100)

# Add a new column "metric" with the constant value "less_likes_than_retweets"
percentage_df = percentage_df.withColumn("metric", lit("less_likes_than_retweets"))

# Select the required columns in the required order
percentage_df = percentage_df.select("sourcehashtag", "metric", "percentage")


# In[15]:


# Define a window function partitioned by user and ordered by date
window = Window.partitionBy("user").orderBy(col("date"))

# Calculate the time difference between consecutive tweets for each user
time_diff = (unix_timestamp(col("date")) - unix_timestamp(lag("date").over(window)))*1.00 / 60.0

# Calculate the average time difference between tweets for each user and sourcehashtag
avg_time_diff = twitterdataanalysis.select("sourcehashtag", time_diff.alias("time_diff")) \
                                    .groupBy("sourcehashtag") \
                                    .agg({"time_diff": "avg"}) \
                                    .withColumnRenamed("avg(time_diff)", "avg_time_diff") \
                                    .orderBy(["sourcehashtag", "avg_time_diff"], ascending=[True, True])

# Add the metric and value columns
avg_time_diff = avg_time_diff.withColumn("metric", lit("avg_time_diff")) \
                            .withColumn("value", avg_time_diff["avg_time_diff"])
                             
# Show the output
avg_time_diff=avg_time_diff.drop('avg_time_diff')


# In[16]:


# Group by sourcehashtag and content of the tweet and count the number of unique users
duplicates = twitterdataanalysis.groupBy("sourcehashtag", "content").agg(countDistinct("username").alias("count"))

# Filter only tweets with more than one user tweeting the same content
duplicates = duplicates.filter(duplicates["count"] > 1)

# Group by sourcehashtag and count the number of duplicate tweets
duplicates = duplicates.groupBy("sourcehashtag").agg(sum("count").alias("count"))

# Sort the dataframe in descending order of the count column
duplicates = duplicates.sort("count", ascending=False)

# Create a new dataframe with the required columns
duplicates_df = duplicates.selectExpr("sourcehashtag as sourcehashtag", "'duplicates' as metric", "count as value")


# In[17]:


datasets_count_total=twitterdataanalysis.groupBy("sourcehashtag").agg(count("*").alias("count"))


# In[18]:


# Inner join datasets_count_total and duplicates_df on sourcehashtag column
joined_df = datasets_count_total.join(duplicates_df, "sourcehashtag", "inner")

# Calculate the percentage of duplicate tweets in total tweets
percentage_df_duplicates = joined_df.withColumn("value", (col("value")/col("count"))*100 ) \
                        .withColumn("metric", lit("percentage of duplicate tweets in total tweets")) \
                        .select("sourcehashtag", "metric", "value")


# In[20]:


# Union the two DataFrames
result = aggregations.union(distinct_users_df).union(avg_retweets_per_account).union(total_tweets_df).union(percentage_df).union(avg_time_diff).union(duplicates_df).union(percentage_df_duplicates)
result = result.withColumn("value", col("value").cast("float"))


# In[22]:


result.write.format("parquet").mode("overwrite").save("s3://twitterdatanalytics/twitter_data_analytics_metrcs.parquet")


# In[44]:


# Group by sourcehashtag and content of the tweet and count the number of unique users
duplicates = twitterdataanalysis.groupBy("sourcehashtag", "content").agg(countDistinct("username").alias("count"))

# Filter only tweets with more than one user tweeting the same content
duplicates = duplicates.filter(duplicates["count"] > 1)

# Group by sourcehashtag and count the number of duplicate tweets
duplicates = duplicates.groupBy("sourcehashtag","content").agg(sum("count").alias("count"))

# Sort the dataframe in descending order of the count column
duplicates = duplicates.sort("count", ascending=False)


# In[24]:


duplicates.write.format("parquet").mode("overwrite").save("s3://twitterdatanalytics/twitter_data_duplicates.parquet")


# In[25]:


# Join the duplicates dataframe with the original dataframe to get all duplicate tweets
duplicate_tweets = duplicates.join(twitterdataanalysis, on=["content","sourcehashtag"], how="inner")

# Group by username and sourcehashtag and count the number of duplicate tweets tweeted by each user
top_duplicate_tweet_users = duplicate_tweets.groupBy("username", "sourcehashtag").agg(count("content").alias("count"))


# In[26]:


top_duplicate_tweet_users.write.format("parquet").mode("overwrite").save("s3://twitterdatanalytics/twitter_data_duplicates_users.parquet")


# In[27]:


distinct_users = twitterdataanalysis.groupBy('sourcehashtag').agg(countDistinct('username').alias('total_distinct_users'))


# In[28]:


# Get the usernames for each hashtag
hb_users = twitterdataanalysis.filter(twitterdataanalysis["sourcehashtag"].isin(["AdvanceHBDMaheshBabu", "HBDMaheshBabu"])) \
    .groupBy("sourcehashtag", "username") \
    .agg(countDistinct("id").alias("count")) \
    .groupBy("username") \
    .agg(countDistinct(when(col("sourcehashtag") == "AdvanceHBDMaheshBabu", col("username"))).alias("AdvanceHBDMaheshBabu_count"),
         countDistinct(when(col("sourcehashtag") == "HBDMaheshBabu", col("username"))).alias("HBDMaheshBabu_count")) \
    .na.fill(0)

# Get the common usernames and the sum of distinct usernames
common_users = hb_users.filter((col("AdvanceHBDMaheshBabu_count") > 0) & (col("HBDMaheshBabu_count") > 0))
common_count = common_users.count()

distinct_users_sum = hb_users.selectExpr("sum(AdvanceHBDMaheshBabu_count + HBDMaheshBabu_count) as distinct_users_sum") \
    .collect()[0]["distinct_users_sum"]

# Show the results
# common_users.show()
print(f"Number of common usernames: {common_count}")
print(f"Sum of distinct usernames who participated in both categories: {distinct_users_sum}")
print(f"Percentage of users in repeated participation of trends:",(common_count/distinct_users_sum)*100)


# In[29]:


# Get the usernames for each hashtag
ntr_users = twitterdataanalysis.filter(twitterdataanalysis["sourcehashtag"].isin(["HappyBirthdayNTR", "NTRBdayFestBegins"])) \
    .groupBy("sourcehashtag", "username") \
    .agg(countDistinct("id").alias("count")) \
    .groupBy("username") \
    .agg(countDistinct(when(col("sourcehashtag") == "HappyBirthdayNTR", col("username"))).alias("HappyBirthdayNTR_count"),
         countDistinct(when(col("sourcehashtag") == "NTRBdayFestBegins", col("username"))).alias("NTRBdayFestBegins_count")) \
    .na.fill(0)

# Get the common usernames and the sum of distinct usernames
common_users = ntr_users.filter((col("HappyBirthdayNTR_count") > 0) & (col("NTRBdayFestBegins_count") > 0))
common_count = common_users.count()

distinct_users_sum = ntr_users.selectExpr("sum(HappyBirthdayNTR_count + NTRBdayFestBegins_count) as distinct_users_sum") \
    .collect()[0]["distinct_users_sum"]

print(f"Number of common usernames: {common_count}")
print(f"Sum of distinct usernames who participated in both categories: {distinct_users_sum}")
print(f"Percentage of users in repeated participation of trends:",(common_count/distinct_users_sum)*100)


# In[30]:


from pyspark.sql.functions import collect_set

# Filter tweets related to tnwelcomesmodi and gobackmodi hashtags
tnwelcomesmodi_users = twitterdataanalysis.filter(twitterdataanalysis.sourcehashtag == "tnwelcomesmodi").select("username")
gobackmodi_users = twitterdataanalysis.filter(twitterdataanalysis.sourcehashtag == "gobackmodi").select("username")

# Find common and distinct usernames between tnwelcomesmodi and gobackmodi tweets
common_users = tnwelcomesmodi_users.intersect(gobackmodi_users).distinct().count()
distinct_users = tnwelcomesmodi_users.union(gobackmodi_users).agg(collect_set('username')).collect()[0][0]

# Calculate counts
common_user_count = common_users
distinct_user_count = len(distinct_users)

# Print the results
print("Common usernames between tnwelcomesmodi and gobackmodi: {}".format(common_user_count))
print("Distinct usernames in tnwelcomesmodi and gobackmodi: {}".format(distinct_user_count))
print(f"Percentage of users in repeated participation of trends:",(common_user_count/distinct_user_count)*100)

