# Databricks-Azure-Sentiment-Analysis

Using Databricks with Azure, to handle live streaming of Tweets. Analyzing sentiment based on keywords search.

### TwitterStream to Azure blob DBFS notebook - this notebook sets up the connection between Databricks and Azure storage. 
  - Subclass Stream which is from Tweepy API module. 
  - Use tweet_stream.filter to find tweets with keywords we are searching for. Then save them to the mounted azure storage.
### Delta Live Tables Twitter notebook - Using Delta Live Tables and SQL create tables to store bronze and silver data.
  - create a workflow to create a pipeline to run the delta table SQL. Optionally add the other two notebooks. up to 40 CPUs maybe needed from Azure to run this.

![image](https://user-images.githubusercontent.com/12418101/228108177-0bffd4bf-ea4e-4416-bdce-96d5581be784.png)


### Huggingface Sentiment Analysis notebook -  
  - Use Huggingface transformers "from transformers import pipeline". Which gives us access to pre-trained models for sentimement analysis.


