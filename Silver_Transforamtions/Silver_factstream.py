# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Reading ###

# COMMAND ----------

## factstream = spark.table('spotify_cata.bronze.factstream_raw')

# COMMAND ----------

## factstream.printSchema()

# COMMAND ----------

## display(factstream)

# COMMAND ----------

# count_records = factstream.count()
# display(count_records)

# COMMAND ----------

bronze_path = "abfss://bronze@spotifyashishstorage.dfs.core.windows.net/factStream/"

schema_location = "abfss://silver@spotifyashishstorage.dfs.core.windows.net/schema/factstream/"

checkpoint_path = "abfss://silver@spotifyashishstorage.dfs.core.windows.net/checkpoints/factstream/"


# COMMAND ----------

factstream = spark.readStream.format('cloudFiles').option('cloudFiles.format','parquet').\
    option('cloudFiles.schemaLocation',schema_location).\
        load(bronze_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Transformations ##

# COMMAND ----------

def transform_factstream(factstream):

    ## Schema Enforcement - Cast columns to correct data types
    factstream_clean = factstream.select(
    col('stream_id').cast("long"),
    col("user_id").cast("string"),
    col("track_id").cast('string'),
    col("artist_id").cast('string'),
    col("stream_timestamp").cast("timestamp"),
    col("ms_played").cast("int"),
    col("skipped").cast('boolean'),
    col("platform").cast("string"),
    col("created_at").cast("timestamp"),
    col("watermark_ts").cast("timestamp"),
    col("ingestion_date").cast("date")
    )

    ## Null Handling - Fill null values for platform and  ms_played

    fact_clean = factstream_clean.filter(col('stream_id').isNotNull())
    fact = fact_clean.withColumn('platform', lower(col('platform')))

    ## mode_platform = fact.groupBy('platform').count().orderBy(col('count').desc()).first()[0]
    fact_stream = fact.fillna({'platform': 'Unknown'})

    fac_str = fact_stream.fillna({'ms_played':0,})
    fact_final = fac_str.withColumn('play_minutes',round(col("ms_played")/60000,2)).withColumn('play_seconds',floor(col("ms_played")/1000))

    # ## Deduplication 

    # window = Window.partitionBy('stream_id').orderBy(col('watermark_ts').desc())
    # fact_final = facts.withColumn('rank',row_number().over(window)).filter(col('rank')==1).drop('rank')

    return fact_final


# COMMAND ----------

factstream_processed = transform_factstream(factstream)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists spotify_cata.silver.factstream_silver(
# MAGIC     stream_id BIGINT,
# MAGIC     user_id STRING,
# MAGIC     track_id STRING,
# MAGIC     artist_id STRING,
# MAGIC     stream_timestamp TIMESTAMP,
# MAGIC     ms_played INT,
# MAGIC     play_minutes DOUBLE,
# MAGIC     play_seconds LONG,
# MAGIC     skipped BOOLEAN,
# MAGIC     platform STRING,
# MAGIC     created_at TIMESTAMP,
# MAGIC     watermark_ts TIMESTAMP,
# MAGIC     ingestion_date DATE
# MAGIC )
# MAGIC using delta 
# MAGIC location 'abfss://silver@spotifyashishstorage.dfs.core.windows.net/factstream/'

# COMMAND ----------

spark.createDataFrame([], factstream_processed.schema) \
    .write \
    .format("delta") \
    .mode("append") \
    .save("abfss://silver@spotifyashishstorage.dfs.core.windows.net/factstream/")

# COMMAND ----------

display(dbutils.fs.ls("abfss://silver@spotifyashishstorage.dfs.core.windows.net/factstream/"))

# COMMAND ----------

def upsert_to_silver(microBatchDF, batchId):

    window = Window.partitionBy("stream_id").orderBy(col("watermark_ts").desc())

    dedup_df = (
        microBatchDF
        .withColumn("rank", row_number().over(window))
        .filter(col("rank") == 1)
        .drop("rank")
    )

    silver_table = DeltaTable.forName(
        spark,
        "spotify_cata.silver.factstream_silver"
    )

    (
        silver_table.alias("target")
        .merge(
            dedup_df.alias("source"),
            "target.stream_id = source.stream_id"
        )
        .whenMatchedUpdateAll(
            condition="source.watermark_ts > target.watermark_ts"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

query = factstream_processed.writeStream.foreachBatch(upsert_to_silver).\
    option('checkpointLocation',checkpoint_path).\
        trigger(availableNow=True).\
            start()
query.awaitTermination()

# COMMAND ----------

display(
    spark.table("spotify_cata.silver.factstream_silver")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enforcing Schema ###
# MAGIC

# COMMAND ----------

# factstream_clean = factstream.select(
#     col('stream_id').cast("long"),
#     col("user_id").cast("string"),
#     col("track_id").cast('string'),
#     col("artist_id").cast('string'),
#     col("stream_timestamp").cast("timestamp"),
#     col("ms_played").cast("int"),
#     col("skipped").cast('boolean'),
#     col("platform").cast("string"),
#     col("created_at").cast("timestamp"),
#     col("watermark_ts").cast("timestamp"),
#     col("ingestion_date").cast("date")
# )

# COMMAND ----------

# fact_clean = factstream_clean.filter(col('stream_id').isNotNull())

# COMMAND ----------

# display(fact_clean.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Null Handling ###

# COMMAND ----------

# fact = fact_clean.withColumn('platform', lower(col('platform')))
# display(fact)

# COMMAND ----------

# mode_platform = fact.groupBy('platform').count().orderBy(col('count').desc()).first()[0]
# fact_stream = fact.fillna(mode_platform, ['platform'])

# COMMAND ----------

# display(fact_stream)

# COMMAND ----------

# fac_str = fact_stream.fillna({'ms_played':0,})

# COMMAND ----------

# facts = fac_str.withColumn('play_minutes',round(col("ms_played")/60000,2)).withColumn('play_seconds',floor(col("ms_played")/1000))

# COMMAND ----------

# facts.printSchema()
# display(facts.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deduplicaton ###
# MAGIC

# COMMAND ----------

# window = Window.partitionBy('stream_id').orderBy(col('watermark_ts').desc())
# fact_final = facts.withColumn('rank',row_number().over(window)).filter(col('rank')==1).drop('rank')
# display(fact_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ### FactStream Delta Tables ###

# COMMAND ----------

# %sql
# DROP TABLE spotify_cata.silver.factstream_silver

# COMMAND ----------

# %sql
# create table if not exists spotify_cata.silver.factstream_silver(
#     stream_id BIGINT,
#     user_id STRING,
#     track_id STRING,
#     artist_id STRING,
#     stream_timestamp TIMESTAMP,
#     ms_played INT,
#     play_minutes DOUBLE,
#     play_seconds LONG,
#     skipped BOOLEAN,
#     platform STRING,
#     created_at TIMESTAMP,
#     watermark_ts TIMESTAMP,
#     ingestion_date DATE
# )
# using delta 
# location 'abfss://silver@spotifyashishstorage.dfs.core.windows.net/factstream/'

# COMMAND ----------

# silver_table = DeltaTable.forName(
#     spark, "spotify_cata.silver.factstream_silver"
# )
# silver_table.alias('target').merge(fact_final.alias('source'),"target.stream_id = source.stream_id").\
# whenMatchedUpdateAll(condition = "source.watermark_ts > target.watermark_ts").\
# whenNotMatchedInsertAll().execute()

# COMMAND ----------

# df = spark.table('spotify_cata.silver.factstream_silver')
# display(df)

# COMMAND ----------

# silver_table = DeltaTable.forName(
#     spark,
#     "spotify_cata.silver.factstream"
# )

# (
#     silver_table.alias("target")
#     .merge(
#         df_dedup.alias("source"),
#         "target.stream_id = source.stream_id"
#     )
#     .whenMatchedUpdate(
#         condition="source.watermark_ts > target.watermark_ts",
#         set={"*": "source.*"}
#     )
#     .whenNotMatchedInsertAll()
#     .execute()
# )