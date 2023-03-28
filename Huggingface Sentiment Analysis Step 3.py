# Databricks notebook source
# MAGIC %md
# MAGIC # Twitter Sentiment Analysis using Huggingface Step 3

# COMMAND ----------

# MAGIC %md
# MAGIC * [jump to Twitter-Stream-Azure notebook]($./TwitterStream to Azure blob DBFS - Step 1)
# MAGIC * [jump to Twitter-SentimentAnalysis notebook]($./Huggingface Sentiment Analysis Step 3)
# MAGIC * [Pipeline](https://adb-3234447377967728.8.azuredatabricks.net/?o=3234447377967728#joblist/pipelines/c7029259-25b6-4c56-83bd-ca0b5254db9c/updates/8035cda4-bf92-4cb5-896a-12e94ac36f3d)

# COMMAND ----------

!pip install transformers  emoji wordcloud

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hugging Face Sentiment Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC for more details about [Hugging Face](https://huggingface.co/) on Databricks, check out the [Databricks blog posting](https://databricks.com/blog/2021/10/28/gpu-accelerated-sentiment-analysis-using-pytorch-and-huggingface-on-databricks.html)

# COMMAND ----------

from transformers import pipeline
import pandas as pd

# COMMAND ----------

df = spark.read.format("delta").table("twittervers1.silver")
tweets = df.toPandas()

# COMMAND ----------

# sentiment analysis is easy with huggingface on Databricks
#
# default model for analysis is "sentiment-analysis"
# but "finiteautomata/bertweet-base-sentiment-analysis" is even better tuned or tweets! 

sentiment_pipeline = pipeline(model="finiteautomata/bertweet-base-sentiment-analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sentiment analysis interactively! (how it works)

# COMMAND ----------

sentiment_pipeline([" :-)",
                   "I love Lakehouses", "I have a broken notebook, programming error?"])

# COMMAND ----------

sentiments = sentiment_pipeline(tweets.text.to_list()[:127])

# COMMAND ----------

# add sentiments as new column to df
tweets = pd.concat([tweets, pd.DataFrame(sentiments)], axis=1)

# COMMAND ----------

# most positive tweets 
#pd.set_option('display.max_colwidth', None)  
tweets.query('label == "POS"').sort_values(by=['score'], ascending=False)[:15]

# COMMAND ----------

# most neg tweets, maybe don't use that for public presentation 
# pd.set_option('display.max_colwidth', None)  

# tweets.query('label == "NEG"').sort_values(by=['score'], ascending=False)[:5].text

# COMMAND ----------

from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
 

# Wordcloud with frequent words in positive tweets
# note, that these words are not necessarily the positive keywords, but the frequent ones
# you can experiment with the size of the x list for better graphics

stop_words = ["https", "RT","how"] + list(STOPWORDS)

x = tweets.query('label == "POS"').sort_values(by=['score'], ascending=False)[:30].text

positive_wordcloud = WordCloud(max_font_size=150, max_words=50, background_color="white", stopwords = stop_words).generate(str(x))
plt.figure()
plt.title("postive tweets")
plt.imshow(positive_wordcloud)
plt.axis("off")
plt.show()


# COMMAND ----------

# Let's count the number of tweets by sentiments
sentiment_counts = tweets.groupby(['label']).size()
print(sentiment_counts)

# visualize the sentiments
fig = plt.figure(figsize=(6,6), dpi=100)
ax = plt.subplot(111)
sentiment_counts.plot.pie(ax=ax, autopct='%1.1f%%',  fontsize=12, label="")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Languages ...

# COMMAND ----------

# MAGIC %sql
# MAGIC -- there should be only EN in tweets.silver
# MAGIC 
# MAGIC select lang, count(*) from tweets.silver group by lang

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Geolocation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- you can c&p these coordinates to show the origin of tweets on google maps
# MAGIC select geo  from tweets.silver where geo is not null  limit 25
