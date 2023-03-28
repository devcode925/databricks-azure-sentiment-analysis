# Databricks notebook source
# MAGIC %md
# MAGIC # TwitterStream to Azure blob / DBFS - Step 1
# MAGIC 
# MAGIC 
# MAGIC * [jump to Twitter-Stream-Azure notebook]($./TwitterStream to Azure blob DBFS - Step 1)
# MAGIC * [jump to Twitter-SentimentAnalysis notebook]($./Huggingface Sentiment Analysis Step 3)
# MAGIC * [Pipeline](https://adb-3234447377967728.8.azuredatabricks.net/?o=3234447377967728#joblist/pipelines/c7029259-25b6-4c56-83bd-ca0b5254db9c/updates/8035cda4-bf92-4cb5-896a-12e94ac36f3d)

# COMMAND ----------


# Mount ADLS Gen2 or Blob Storage with ABFS ## https://docs.databricks.com/dbfs/mounts.html#mount-adls-gen2-or-blob-storage-with-abfs
# first create a keyvault, then generate a secret.
# create mnt here holder dbutils.fs.mkdirs("/mnt/tweet-holder")
#https://docs.databricks.com/security/aad-storage-service-principal.html
# create a blob to hold the tweets.
## for a principal you need to set the scope. for this one it's https://adb-3234447377967728.8.azuredatabricks.net/?o=3234447377967728#secrets/createScope otherwise it's
## Go to https://<databricks-instance>#secrets/createScope 
# dbutils.secrets.listScopes() to see the name in case you forget.


# COMMAND ----------

#dbutils.secrets.listScopes()

# COMMAND ----------

#dbutils.fs.refreshMounts()
#dbutils.fs.mkdirs("/mnt/tweet-holder")
#dbutils.fs.mounts()
# client id for configs is the applicationid in for my service principal 704692c5-b46c-402d-bed0-ebf417f68948

# COMMAND ----------

# MAGIC %md
# MAGIC -- for databricks file system(dbfs) https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts
# MAGIC   
# MAGIC   -- need to create service principal https://learn.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal
# MAGIC 
# MAGIC -- create database connector https://portal.azure.com/?quickstart=true#view/HubsExtension/BrowseResource/resourceType/Microsoft.Databricks%2FaccessConnectors
# MAGIC 
# MAGIC 
# MAGIC f7eae66d-6509-4d5d-a514-cac8fa836db0
# MAGIC   "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="azure-storage",key="blob-storage-databricks"),

# COMMAND ----------

'''
# depreciated and didn't work
dbutils.fs.mount(
source = "wasbs://twittercatcher@twitterblobber.blob.core.windows.net",
mount_point = "/mnt/tweet-holder2",
extra_configs = {"fs.azure.account.key.twitterblobber.blob.core.windows.net":dbutils.secrets.get(scope="azure-storage",key="blob-storage-databricks")})

df = spark.read.text("/mnt/tweet-holder/test.json")

df.show()
'''

# COMMAND ----------

''' created the first time -- #used service principal, and secret, scopt didn't work for me.
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "***",
          "fs.azure.account.oauth2.client.secret": "your-secret",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/3ff9b47e-eced-43d3-b1f2-7315f4ec788c/oauth2/token"}
dbutils.fs.mount( source = "abfss://twittercatcher@twitterblobber.dfs.core.windows.net/", mount_point = "/mnt/tweet-holder", extra_configs = configs)
'''

# COMMAND ----------

# should use databricks secrets and the CLI to store and retrieve those keys in a safe way.
#
# for a first try, you can setup you twitter keys here
consumer_key = "pQCeK1M8Q6hnwpPQV8Uzs7QG8"
consumer_secret = "08qXyo3EoZInFbkPHTxGftPi8cW0bODuogUmgWXtGpPfncT61j"
access_token = "1440828778955956233-8mU4smuEPIOCne5uqoX9jJB3R6mHOt"
access_token_secret = "bPNPvtCFuhAxNJJvlzMmaPlEDkUX3UzYaLz4HfXLxa289"

# in my demo, I read in the keys from another notebook in the cell below (which can be savely removed or commented out)


# COMMAND ----------

!pip install tweepy jsonpickle

# COMMAND ----------

import tweepy
import calendar
import time
import jsonpickle
import sys


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth, wait_on_rate_limit=True, timeout=60)
print(f'Twitter screen name: {api.verify_credentials().screen_name}')


# Subclass Stream  
class TweetStream(tweepy.Stream):

    def __init__(self, filename):
        tweepy.Stream.__init__(self, consumer_key=consumer_key, consumer_secret=consumer_secret,
                             access_token=access_token, access_token_secret=access_token_secret)
        self.filename = filename
        self.text_count = 0
        self.tweet_stack = []


    def on_status(self, status):
        #print('*'+status.text)
        self.text_count = self.text_count + 1
        self.tweet_stack.append(status)
    
        # when to print
        if (self.text_count % 1 == 0):
            print(f'retrieving tweet {self.text_count}: {status.text}')

        # how many tweets to batch into one file
        if (self.text_count % 10 == 0):
            self.write_file()
            self.tweet_stack = []

        # hard exit after collecting n tweets
        if (self.text_count == 30):
            raise Exception("Finished job")

    def write_file(self):
        file_timestamp = calendar.timegm(time.gmtime())
        fname = self.filename + '/tweets_' + str(file_timestamp) + '.json'


        f = open(fname, 'w')
        for tweet in self.tweet_stack:
            f.write(jsonpickle.encode(tweet._json, unpicklable=False) + '\n')
        f.close()
        print("Wrote local file ", fname)

    def on_error(self, status_code):
        print("Error with code ", status_code)
        sys.exit()


# Initialize instance of the subclass
tweet_stream = TweetStream("/dbfs/mnt/tweet-holder")

# Filter realtime Tweets by keyword
try:
    tweet_stream.filter(languages=["en"],
                        track=["Target", "Target Superstore", "Target grocery"
                               "Minnesota Grocer"])



except Exception as e:
    print("some error ", e)
    print("Writing out tweets file before I have to exit")
    tweet_stream.write_file()
finally:
    print("Downloaded tweets ", tweet_stream.text_count)
    tweet_stream.disconnect()


# COMMAND ----------

dbutils.notebook.exit("stop")

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ### create a DBFS directory, check for number of files, deletes a certain number of files ...

# COMMAND ----------

# create a directory to buffer the streamed data
!mkdir "/dbfs/mnt/tweet-holder/tweets"

# COMMAND ----------

# count files in that directory - compare with #files in DLT bronze
#!ls -l /dbfs/mnt/tweet-holder/twitter_dataeng2 | wc

# COMMAND ----------

# disk usage
#!du -h  dbfs/mnt/tweet-holder/twitter_dataeng2

# COMMAND ----------

'''# remove n files. use this to trim demo
files = dbutils.fs.ls("/data/twitter_dataeng2")
del = 400
print(f'number of files: {len(files)}')
print(f'number of files to delete: {del}')


for x, file in enumerate(files):
  # delete n files from directory
  if x < del :
    # print(x, file)
    dbutils.fs.rm(file.path)

    
# use dbutils to copy over files... 
# dbutils.fs.cp("/data/twitter_dataeng/" +f, "/data/twitter_dataeng2/")
'''
