# Databricks-Azure-Sentiment-Analysis

I watched a demo from Databricks and wanted to give it try. They used Amazon S3 instead of Azure. Plus used Databricks trial rather than creating a Databricks workspace in Azure. Other changes is fully working out mounting azure storage to Databricks. 
- required for setup is an Azure account, which will need to be a paid account to support the CPU's needed to run Databricks.

https://github.com/databricks/delta-live-tables-notebooks/tree/main/twitter-dlt-huggingface-demo

Using Databricks with Azure, to handle live streaming of Tweets. Analyzing sentiment based on keywords search.

### TwitterStream to Azure blob DBFS notebook - this notebook sets up the connection between Databricks and Azure storage. 
  - Subclass Stream which is from Tweepy API module. 
  - Use tweet_stream.filter to find tweets with keywords we are searching for. Then save them to the mounted azure storage.
### Delta Live Tables Twitter notebook - Using Delta Live Tables and SQL create tables to store bronze and silver data.
  - create a workflow to create a pipeline to run the delta table SQL. Optionally add the other two notebooks. up to 40 CPUs maybe needed from Azure to run this.

![image](https://user-images.githubusercontent.com/12418101/228108177-0bffd4bf-ea4e-4416-bdce-96d5581be784.png)


### Huggingface Sentiment Analysis notebook -  
  - Use Huggingface transformers "from transformers import pipeline". Which gives us access to pre-trained models for sentimement analysis.


