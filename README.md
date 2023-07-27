# Twitter-Trend-Manipulation-Investigating-Coordinated-Campaigns-and-Artificial-Popularity-Inflation
## Introduction
Social media platforms, including Twitter, have become an integral part of our daily lives. Analyzing Twitter trends can offer valuable insights into global opinions and interests, with millions of tweets posted daily. This project delves into analyzing ten different Twitter trends, with a primary focus on the hashtag #advancehbdmaheshbabu, encompassing a dataset of 302,595 tweets. Additionally, it explores trends in political campaigns during the Indian General Election in February 2019, highlighting two Twitter trends: #Welcomemodi in support of Modi, and #Gobackmodi against him. Utilizing PySpark, data processing and analytics were carried out on the collected tweets, leading to key findings and results. The objective of this project is to aid the intended audience and the broader community in comprehending the significance and implications of Twitter trends, while also developing tools, such as a dashboard, to monitor trends and differentiate between genuine trends and artificially inflated ones. This report provides an overview of the project's background, objectives, methodology, key findings, and implications.
## Dataset Description
A detailed explanation of each column that was pulled during the data collection process:
- `tweet.card`: The Twitter card type of the tweet.
- `tweet.cashtags`: The cashtags used in the tweet.
- `tweet.content`: The text content of the tweet.
- `tweet.conversationId`: The ID of the conversation thread that the tweet is a part of.
- `tweet.coordinates`: The geolocation coordinates associated with the tweet, if any.
- `tweet.date`: The date and time when the tweet was created.
- `tweet.hashtags`: The hashtags used in the tweet.
- `tweet.id`: The unique ID of the tweet.
- `tweet.inReplyToTweetId`: The ID of the tweet that this tweet is in reply to, if any.
- `tweet.inReplyToUser`: The username of the user that this tweet is in reply to, if any.
- `tweet.json`: The raw JSON data of the tweet.
- `tweet.lang`: The language of the tweet.
- `tweet.likeCount`: The number of likes that the tweet has received.
- `tweet.links`: The URLs included in the tweet.
- `tweet.mentionedUsers`: The usernames of the users mentioned in the tweet.
- `tweet.place`: The name of the place associated with the tweet, if any.
- `tweet.quoteCount`: The number of times that the tweet has been quoted.
- `tweet.quotedTweet`: The ID of the tweet that this tweet has quoted, if any.
- `tweet.rawContent`: The raw content of the tweet.
- `tweet.renderedContent`: The rendered content of the tweet.
- `tweet.replyCount`: The number of times that the tweet has been replied to.
- `tweet.retweetCount`: The number of times that the tweet has been retweeted.
- `tweet.retweetedTweet`: The ID of the tweet that this tweet has retweeted, if any.
- `tweet.source`: The source from which the tweet was posted (e.g., Twitter for iPhone).
- `tweet.user`: The ID of the user who posted the tweet.
- `tweet.username`: The username of the user who posted the tweet.
- `tweet.vibe`: The sentiment score of the tweet.
- `tweet.viewCount`: The number of times that the tweet has been viewed.

## Data Cleaning and Data Preprocessing
After consolidating all datasets, the Twitter data underwent essential data cleaning and preprocessing steps. Initially, missing values in each column were identified and the 'Unnamed: 0' column was removed. Subsequently, the 'date' column was cast to a timestamp data type, and other unwanted columns like card, cashtags, and coordinates were dropped. Numeric columns with missing values were filled with 0, and empty strings were replaced with 'None' values. For text data preparation, all text was converted to lowercase, leading/trailing spaces and duplicate whitespaces were eliminated, and any remaining multiple whitespaces were replaced with single whitespaces. These critical data cleaning and preprocessing measures ensured that the data was in a usable format for analysis. By eliminating unnecessary columns and filling missing values, the noise in the dataset was reduced, guaranteeing that the analysis was based on a complete dataset.
![image](https://github.com/dharmateja36/Twitter-Trend-Manipulation-Investigating-Coordinated-Campaigns-and-Artificial-Popularity-Inflation/assets/117693500/069aee52-4218-4f55-8bf8-18210762d7e1)

## Project Architecture
The project commenced with the objective of analyzing Twitter data associated with a specific hashtag to gain valuable insights. To handle the substantial data volume and required processing, a cloud-based architecture was chosen. Initially, Snscrape and Sntwitter were utilized, followed by TwitterSearchScraper to retrieve Twitter data related to the hashtag, storing it in an S3 bucket in JSON format. S3 was selected due to its scalability and cost-effectiveness for storing vast amounts of unstructured data.

Next, PySpark facilitated data cleaning, analysis, and structuring on AWS EMR, after which the cleaned data was stored in a structured form using Parquet format in S3. Parquet was preferred as it optimizes performance and efficiently manages extensive datasets through its columnar storage format.

Subsequently, the structured data from S3 was loaded into AWS Redshift, a cloud-based data warehousing solution renowned for high-speed querying and efficient handling of structured data. Finally, AWS Quicksight was integrated with Redshift to create an interactive dashboard that offers real-time insights into user sentiment and behavior related to the hashtag.
<img width="951" alt="image" src="https://github.com/dharmateja36/Twitter-Trend-Manipulation-Investigating-Coordinated-Campaigns-and-Artificial-Popularity-Inflation/assets/117693500/d0625185-4e40-4030-ba68-189da944bafd">
## Data Engineering
Data engineering played a crucial role in the project. Processing and cleaning the raw data retrieved from twitter were focused on. The snscrape Python module was used to extract data and dump it into an S3 bucket in JSON format. From there, PySparkwas used to run the scripts and perform the necessary data cleaning and analysis steps to structure the data in the required format. The cleaned data was then stored in Parquet format back in the S3 bucket. The AWS Redshift was used to load the data from S3. Overall, data engineering helped to prepare the data for the machine learning model and gain valuable insights from the twitter data.
## Platform Engineering
For this project, Amazon EMR (Elastic MapReduce) which is a cloud-based big data platform was used to run the PySpark scripts. A Virtual Private Cloud (VPC) with limited access to the project team members was also set up.
To access and manage the cloud resources, different methods such as AWS Console, AWS CLI (Command Line Interface), and AWS-Python based packages were used. Additionally, the EMR clusters were hosted on Amazon EC2 instances with a defined schedule for starting and stopping to optimize resource utilization and reduce costs. Overall, the platform engineering aspect of this project helped in efficiently managing the cloud resources and performing data processing tasks at scale.
## Project Workflow
The project flow starts with the retrieval of data from Twitter using the snscrape and sntwitter libraries. The retrieved data is then dumped into an S3 bucket in JSON format
<img width="574" alt="image" src="https://github.com/dharmateja36/Twitter-Trend-Manipulation-Investigating-Coordinated-Campaigns-and-Artificial-Popularity-Inflation/assets/117693500/ce0eff49-f7fd-43c1-b127-8d5df0bf1e8e">

Once the data is in S3, a PySpark script on top of the JSON files is run to perform data cleaning, analysis, and structure the data in the required form. The cleaned and structured data is stored in Parquet format in S3.
<img width="589" alt="image" src="https://github.com/dharmateja36/Twitter-Trend-Manipulation-Investigating-Coordinated-Campaigns-and-Artificial-Popularity-Inflation/assets/117693500/cb386292-a0fb-406e-9e1d-264e41851165">

From S3, the data is loaded into AWS Redshift, a data warehousing service, where it is organized into tables for efficient querying.
<img width="941" alt="image" src="https://github.com/dharmateja36/Twitter-Trend-Manipulation-Investigating-Coordinated-Campaigns-and-Artificial-Popularity-Inflation/assets/117693500/93389dcc-5af0-4caa-83b0-05be347c89cc">

AWS QuickSight is then connected to Redshift to visualize the results of the analytics performed on the data in a dashboard. 
<img width="800" alt="image" src="https://github.com/dharmateja36/Twitter-Trend-Manipulation-Investigating-Coordinated-Campaigns-and-Artificial-Popularity-Inflation/assets/117693500/97b25f55-234e-4a8c-9b16-606cd7f3a943">
Throughout the process, various AWS services, such as Amazon S3, AWS Redshift, and AWS QuickSight, along with PySpark and other Python libraries were used to perform data engineering and analysis tasks. The project flow ensures that the data is processed efficiently and effectively, leading to better insights and decision-making.
## Data Analysis
### Twitter Trends Analysis: #Gobackmodi vs #Welcomemodi
During the Indian General Election in February 2019, two distinct Twitter trends emerged: #Welcomemodi in support of Modi and #Gobackmodi against him. Employing data engineering and analytics techniques, data related to these trends were extracted and processed to gain insights into user engagement and sentiment.

For the anti-Modi campaign (#Gobackmodi), a total of 57,202 tweets, 188,825 retweets, and 289,639 likes were observed. Among the tweets, 18% had more retweets than likes, and 19% were duplicates. There were 14,162 distinct users, averaging four retweets per account, with an average time gap between tweets of 2,248. Additionally, 17,658 quote tweets and 24,039 replies were identified, with only 2.86% of common users in the trends.

In contrast, the pro-Modi campaign (#Welcomemodi) saw 32,493 tweets analyzed, along with 75,053 retweets and 114,410 likes. Similarly, 18% of tweets had more retweets than likes, and 40% were duplicates. The number of distinct users was 5,992, averaging three retweets per account, with an average time gap between tweets of 455. Furthermore, 7,490 quote tweets and 10,014 replies were discovered, with 2.86% of common users in the trends.

<img width="738" alt="image" src="https://github.com/dharmateja36/Twitter-Trend-Manipulation-Investigating-Coordinated-Campaigns-and-Artificial-Popularity-Inflation/assets/117693500/febe6702-17c9-4e85-99ba-b46df0f42749">

<img width="738" alt="image" src="https://github.com/dharmateja36/Twitter-Trend-Manipulation-Investigating-Coordinated-Campaigns-and-Artificial-Popularity-Inflation/assets/117693500/f5cd95cb-5e00-494b-8425-fabb9a5fab5c">

## Conclusion
In conclusion, this project successfully achieved its objectives of uncovering coordinated campaigns by analyzing artificial inflation of trend popularity on Twitter. By analyzing a dataset of over 300,000 tweets, the presence of coordinated campaigns that artificially inflated trend popularity on Twitter was identified. Notably, interesting trends and patterns were observed, including a high percentage of likes compared to retweets and a significant number of duplicate tweets.

Despite facing limitations and technical challenges, the project developed end-to-end data engineering and data visualization pipelines using AWS technologies like S3, EMR, Redshift, and QuickSight. The findings of this project hold the potential to benefit researchers, social media platforms, and policymakers in their understanding and efforts to address the issue of coordinated campaigns on social media platforms.
## Future Work
As previously mentioned, due to pricing restrictions, gathering all the data for certain hashtags was not possible. To improve future analyses, cost-effective solutions could be explored to collect a larger volume of data for more accurate insights. Currently, batch data processing was utilized for the analysis, but in the future, investigating real-time streaming data processing could enable more timely analysis and provide quicker insights.

To enhance the analysis, advanced machine learning and deep learning techniques, such as sentiment analysis, network analysis, and anomaly detection, can be explored. These approaches can reveal more nuanced patterns and potentially detect coordinated campaigns with higher accuracy. Additionally, the use of graph databases could be explored to analyze user connections and identify potential coordinated campaigns or networks of bots.

In conclusion, this project offers valuable insights into the issue of coordinated campaigns on Twitter and showcases the potential of data engineering and data visualization techniques for social media analysis. By exploring the mentioned future improvements, the analysis can be further enhanced and refined.
