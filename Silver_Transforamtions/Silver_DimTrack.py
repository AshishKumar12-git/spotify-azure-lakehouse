# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

bronze_path = 'abfss://bronze@spotifyashishstorage.dfs.core.windows.net/dimTrack'
schema_location = 'abfss://silver@spotifyashishstorage.dfs.core.windows.net/schema/dimtrack'
checkpoint_location = 'abfss://silver@spotifyashishstorage.dfs.core.windows.net/checkpoints/dimtrack'

# COMMAND ----------

track = spark.readStream.format('cloudFiles').option('cloudFiles.format','parquet').\
    option('cloudFiles.schemaLocation',schema_location).load(bronze_path)

# COMMAND ----------

def transform_track(track):

    ## Schema Enforcement
    dim_track = track.select(
    col('track_key').cast('integer'),
    col('track_id').cast('string'),
    col('track_name').cast('string'),
    col('artist_id').cast('string'),
    col('duration_ms').cast('integer'),
    col('album_name').cast('string'),
    col('release_date').cast('date'),
    col('explicit_flag').cast('boolean'),
    col('created_at').cast('timestamp'),
    col('updated_at').cast('timestamp'),
    col('watermark_ts').cast('timestamp'),
    col('ingestion_date').cast('date')
    )
    # ## Filter out null values
    # dim_track = dim_track.filter(col('track_id').isNotNull()).count()

    ## Applying Regular Expression to fixing out the name of the track and album
    dimtrack = dim_track.withColumn('track_name',regexp_replace(col('track_name'),r'Track ','Track-')).\
    withColumn('album_name',regexp_replace(col('album_name'),r'Album ','Album_'))

    ## Adding duration in mins and secs
    dimtrack = dimtrack.withColumn('duration_mins',round(col('duration_ms')/60000,2)).withColumn('duration_secs',floor(col('duration_ms')/1000))

    ##  casting duration_secs to integer
    dimtrack = dimtrack.withColumn('duration_secs',col('duration_secs').cast('integer'))

    return dimtrack




# COMMAND ----------

fact_processed = transform_track(track)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists spotify_cata.silver.dimtrack
# MAGIC (
# MAGIC   track_key BIGINT,
# MAGIC   track_id STRING,
# MAGIC   track_name STRING,
# MAGIC   artist_id STRING,
# MAGIC   duration_ms INTEGER,
# MAGIC   album_name STRING,
# MAGIC   release_date DATE,
# MAGIC   explicit_flag BOOLEAN,
# MAGIC   created_at timestamp,
# MAGIC   updated_at timestamp,
# MAGIC   watermark_ts timestamp,
# MAGIC   ingestion_date DATE,
# MAGIC   duration_mins DOUBLE,
# MAGIC   duration_secs int
# MAGIC )
# MAGIC using delta 
# MAGIC location 'abfss://silver@spotifyashishstorage.dfs.core.windows.net/dimtrack/'

# COMMAND ----------

def upsert_to_silver(MicroBatchDF,batchID):
    window = Window.partitionBy('track_id').orderBy(col('watermark_ts').desc())
    track_dedup = MicroBatchDF.withColumn('rank',row_number().over(window)).filter(col('rank')==1).drop(col('rank'))
    silver_table = DeltaTable.forName(
        spark,'spotify_cata.silver.dimtrack'
    )
    silver_table.alias('t').merge(
        track_dedup.alias('s'),'t.track_id = s.track_id'
    ).whenMatchedUpdateAll(condition='s.watermark_ts > t.watermark_ts').whenNotMatchedInsertAll().execute()
    

# COMMAND ----------

query = fact_processed.writeStream.foreachBatch(upsert_to_silver).\
    option('checkpointLocation',checkpoint_location).\
        trigger(availableNow=True).start()
query.awaitTermination()

# COMMAND ----------

# df = spark.table('spotify_cata.bronze.dimtrack_raw')
# display(df)

# COMMAND ----------

# df.printSchema()

# COMMAND ----------

# df1 = df.select(
#     col('track_key').cast('integer'),
#     col('track_id').cast('string'),
#     col('track_name').cast('string'),
#     col('artist_id').cast('string'),
#     col('duration_ms').cast('integer'),
#     col('album_name').cast('string'),
#     col('release_date').cast('date'),
#     col('explicit_flag').cast('boolean'),
#     col('created_at').cast('timestamp'),
#     col('updated_at').cast('timestamp'),
#     col('watermark_ts').cast('timestamp'),
#     col('ingestion_date').cast('date')
# )

# COMMAND ----------

# df_1 = df.filter(col('track_id').isNotNull()).count()
# display(df_1)

# COMMAND ----------

# df_2 = df.withColumn('track_name',regexp_replace(col('track_name'),r'Track ','Track-')).\
#     withColumn('album_name',regexp_replace(col('album_name'),r'Album ','Album_'))
# display(df_2)


# COMMAND ----------

# df_3 = df_2.withColumn('duration_mins',round(col('duration_ms')/60000,2)).withColumn('duration_secs',floor(col('duration_ms')/1000))
# display(df_3)

# COMMAND ----------

# df_3.printSchema()

# COMMAND ----------

# df4 = df_3.withColumn('duration_secs',col('duration_secs').cast('integer'))
# display(df4)

# COMMAND ----------

# %sql
# create table if not exists spotify_cata.silver.dimtrack
# (
#   track_key BIGINT,
#   track_id STRING,
#   track_name STRING,
#   artist_id STRING,
#   duration_ms INTEGER,
#   album_name STRING,
#   release_date DATE,
#   explicit_flag BOOLEAN,
#   created_at timestamp,
#   updated_at timestamp,
#   watermark_ts timestamp,
#   ingestion_date DATE,
#   duration_mins DOUBLE,
#   duration_secs int
# )

# COMMAND ----------

