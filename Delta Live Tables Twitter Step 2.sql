-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Live Tables Twitter Step 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC * [jump to Twitter-Stream-Azure notebook]($./TwitterStream to Azure blob DBFS - Step 1)
-- MAGIC * [jump to Twitter-SentimentAnalysis notebook]($./Huggingface Sentiment Analysis Step 3)
-- MAGIC * [Pipeline](https://adb-3234447377967728.8.azuredatabricks.net/?o=3234447377967728#joblist/pipelines/c7029259-25b6-4c56-83bd-ca0b5254db9c/updates/8035cda4-bf92-4cb5-896a-12e94ac36f3d)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC --This Delta Live Tables query is syntactically valid, but you must create a pipeline in order to define and populate your table.
-- MAGIC 
-- MAGIC 1. create a workflow

-- COMMAND ----------



-- COMMAND ----------

-- streaming ingest + schema inference with Auto Loader
CREATE OR REFRESH STREAMING LIVE TABLE bronze
AS SELECT * FROM cloud_files(
  "dbfs:/mnt/tweet-holder", "json"
)

-- COMMAND ----------

-- constraints policies: track #badrecords/ drop record/ abort processing record 
CREATE OR REFRESH STREAMING LIVE TABLE silver 
(CONSTRAINT valid_language EXPECT (lang == "en") ON VIOLATION DROP ROW,
CONSTRAINT valid_id EXPECT (id != "") ON VIOLATION DROP ROW)
COMMENT 'data is cleansed - other languages than EN are dropped'
AS
  SELECT id, geo, lang, text FROM STREAM (LIVE.bronze)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE languages
COMMENT 'table for statistics of different languages
that showed up in the pipeline' 
AS
  SELECT lang, count(*)  AS count FROM STREAM (LIVE.bronze) GROUP BY lang ORDER BY count

-- COMMAND ----------


