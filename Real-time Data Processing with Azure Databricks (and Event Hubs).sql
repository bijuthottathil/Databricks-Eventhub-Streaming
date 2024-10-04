-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Bronze Layer
-- MAGIC 1. Set up Azure Event hubs connection string.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC connectionString = "Endpoint=sb://ehubnamespace-dev.servicebus.windows.net/;SharedAccessKeyName=ehpolicy;SharedAccessKey=/Omqagaey1kLCTgZqjFR7TsC+cyDzBIMm+AEhIbPp1s=;EntityPath=eh-temperaturedata"
-- MAGIC eventHubName = "eh-temperaturedata"
-- MAGIC
-- MAGIC ehConf = {
-- MAGIC   'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString),
-- MAGIC   'eventhubs.eventHubName': eventHubName
-- MAGIC }

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Reading and writing the stream to the bronze layer

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.readStream \
-- MAGIC     .format("eventhubs") \
-- MAGIC     .options(**ehConf) \
-- MAGIC     .load()
-- MAGIC
-- MAGIC display(df)
-- MAGIC
-- MAGIC df.writeStream \
-- MAGIC     .option("checkpointLocation", "/mnt/streaming-catalog/bronze/weather") \
-- MAGIC     .outputMode("append") \
-- MAGIC     .format("delta") \
-- MAGIC     .start("/mnt/streaming-catalog/bronze/weather")

-- COMMAND ----------

-- Create a temporary view for the Delta table
CREATE OR REPLACE TEMP VIEW weather_view
AS
SELECT * FROM delta.`/mnt/streaming-catalog/bronze/weather`;

-- Query the temporary view
SELECT * FROM weather_view;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Silver Layer
-- MAGIC 1 Defining the schema for the JSON object.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC weather_schema = StructType([
-- MAGIC     StructField("country", StringType(), True),
-- MAGIC     StructField("city", StringType(), True),
-- MAGIC     StructField("date", StringType(), True),  # Alternatively, use DateType() if the date format can be parsed
-- MAGIC     StructField("temperature", IntegerType(), True),
-- MAGIC     StructField("humidity", IntegerType(), True),
-- MAGIC     StructField("windSpeed", IntegerType(), True),
-- MAGIC     StructField("windDirection", StringType(), True),
-- MAGIC     StructField("precipitation", IntegerType(), True),
-- MAGIC     StructField("cloudCover", IntegerType(), True),
-- MAGIC     StructField("visibility", IntegerType(), True),
-- MAGIC     StructField("pressure", IntegerType(), True),
-- MAGIC     StructField("dewPoint", IntegerType(), True),
-- MAGIC     StructField("uvIndex", IntegerType(), True),
-- MAGIC     StructField("sunrise", StringType(), True),
-- MAGIC     StructField("sunset", StringType(), True),
-- MAGIC     StructField("moonrise", StringType(), True),
-- MAGIC     StructField("moonset", StringType(), True),
-- MAGIC     StructField("moonPhase", StringType(), True),
-- MAGIC     StructField("conditions", StringType(), True),
-- MAGIC     StructField("icon", StringType(), True)
-- MAGIC ])
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Reading, transforming and writing the stream from the bronze to the silver layer.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC df = spark.readStream\
-- MAGIC     .format("delta")\
-- MAGIC      .load("/mnt/streaming-catalog/bronze/weather")\
-- MAGIC     .withColumn("body", col("body").cast("string"))\
-- MAGIC     .withColumn("body",from_json(col("body"), weather_schema))\
-- MAGIC     .select("body.country","body.city","body.temperature", "body.humidity", "body.windSpeed", "body.windDirection", "body.precipitation", "body.conditions", col("enqueuedTime").alias('timestamp'))
-- MAGIC
-- MAGIC df.display()
-- MAGIC
-- MAGIC
-- MAGIC df.writeStream \
-- MAGIC     .option("checkpointLocation", "/mnt/streaming-catalog/silver/weather") \
-- MAGIC     .outputMode("append") \
-- MAGIC     .format("delta") \
-- MAGIC     .start("/mnt/streaming-catalog/silver/weather")

-- COMMAND ----------

SELECT * FROM delta.`/mnt/streaming-catalog/silver/weather`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Gold Layer
-- MAGIC 1 .Reading, aggregating and writing the stream from the silver to the gold layer.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.readStream\
-- MAGIC     .format("delta")\
-- MAGIC     .load("/mnt/streaming-catalog/silver/weather")\
-- MAGIC     .withWatermark("timestamp", "5 minutes") \
-- MAGIC     .groupBy(window("timestamp", "5 minutes")) \
-- MAGIC     .agg(avg("temperature").alias('temperature'), avg("humidity").alias('humidity'), avg("windSpeed").alias('windSpeed'), avg("precipitation").alias('precipitation'))\
-- MAGIC 	.select('window.start', 'window.end', 'temperature', 'humidity', 'windSpeed', 'precipitation')
-- MAGIC
-- MAGIC # Displaying Aggregated Stream: Visualize aggregated data for insights into weather trends
-- MAGIC df.display()
-- MAGIC
-- MAGIC # Writing Aggregated Stream: Store the aggregated data in 'streaming.gold.weather_aggregated' with checkpointing for data integrity
-- MAGIC df.writeStream\
-- MAGIC     .option("checkpointLocation", "/mnt/streaming-catalog/weather_summary")\
-- MAGIC     .outputMode("append")\
-- MAGIC     .format("delta")\
-- MAGIC     .start("/mnt/streaming-catalog/gold/weather_summary")
-- MAGIC

-- COMMAND ----------

SELECT * FROM delta.`/mnt/streaming-catalog/gold/weather_summary`;

-- Create a temporary view for the Delta table
CREATE OR REPLACE TEMP VIEW weather_Summary_view
AS
SELECT * FROM delta.`/mnt/streaming-catalog/gold/weather_summary`;

-- Query the temporary view
SELECT * FROM weather_Summary_view;
