# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql.functions import regexp_replace

# COMMAND ----------

bronze_path = 'abfss://bronze@spotifyashishstorage.dfs.core.windows.net/dimArtist'
schema_location = 'abfss://silver@spotifyashishstorage.dfs.core.windows.net/schema/dimuartist'
checkpoint_location = 'abfss://silver@spotifyashishstorage.dfs.core.windows.net/checkpoints/dimartist'

# COMMAND ----------

dimartist = spark.readStream.format('cloudFiles').option('cloudFiles.format','parquet').\
    option('cloudFiles.schemaLocation',schema_location).\
        load(bronze_path)

# COMMAND ----------

def transform_dimartist(dimartist):
    artist = dimartist.select(
    col('artist_key').cast('long'),
    col('artist_id').cast('string'),
    col('artist_name').cast('string'),
    col('genre').cast('string'),
    col('popularity').cast('integer'),
    col('created_at').cast('timestamp'),
    col('updated_at').cast('timestamp'),
    col('watermark_ts').cast('timestamp'),
    col('ingestion_date').cast('date')
    )
    artistname = artist.withColumn('artist_name',regexp_replace(col('artist_name'),r"Artist ","Artist-"))
    dim_artist = artistname.fillna({'genre':'Unknown'})
    return dim_artist






# COMMAND ----------

processed_dimartist = transform_dimartist(dimartist)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS SPOTIFY_CATA.SILVER.dimartist(
# MAGIC    artist_key BIGINT,
# MAGIC    artist_id STRING,
# MAGIC    artist_name STRING,
# MAGIC    genre STRING,
# MAGIC    popularity integer, 
# MAGIC    created_at timestamp,
# MAGIC    updated_at timestamp,
# MAGIC    watermark_ts timestamp,
# MAGIC    ingestion_date date
# MAGIC )
# MAGIC using DELTA
# MAGIC location 'abfss://silver@spotifyashishstorage.dfs.core.windows.net/dimartist/'

# COMMAND ----------

def upsert_to_silver(MicroBatchDF, batchID):
    window = Window.partitionBy('artist_id').orderBy(col('watermark_ts').desc())
    dim_dedup = MicroBatchDF.withColumn('rank',row_number().over(window)).filter(col('rank')==1).drop(col('rank'))
    silver_table = DeltaTable.forName(
        spark,'spotify_cata.silver.dimartist'
    )
    silver_table.alias('target').merge(
        dim_dedup.alias('source'),'target.artist_id = source.artist_id'
    ).whenMatchedUpdateAll(condition='source.watermark_ts > target.watermark_ts').whenNotMatchedInsertAll().execute()

# COMMAND ----------

query = processed_dimartist.writeStream.foreachBatch(upsert_to_silver).option('checkpointLocation',checkpoint_location).\
    trigger(availableNow=True).start()
query.awaitTermination()

# COMMAND ----------

# artist = spark.table('spotify_cata.bronze.dimartist_raw')
# display(artist)

# COMMAND ----------

# artist.printSchema()

# COMMAND ----------

# artis = artist.select(
#     col('artist_key').cast('long'),
#     col('artist_id').cast('string'),
#     col('artist_name').cast('string'),
#     col('genre').cast('string'),
#     col('popularity').cast('integer'),
#     col('created_at').cast('timestamp'),
#     col('updated_at').cast('timestamp'),
#     col('watermark_ts').cast('timestamp'),
#     col('ingestion_date').cast('date')
# )
# artis.printSchema()

# COMMAND ----------

# count_records = artis.filter(col('popularity').isNull()).count()
# print(count_records)

# COMMAND ----------

# artistname = artis.withColumn('artist_name',regexp_replace(col('artist_name'),r"Artist ","Artist-"))
# display(artistname)

# COMMAND ----------

# dim_artist = artistname.fillna({'genre':'Unknown'})
# display(dim_artist)

# COMMAND ----------

# window = Window.partitionBy('artist_id').orderBy(col('watermark_ts').desc())
# final_dimartist = dim_artist.withColumn('rank',row_number().over(window)).filter(col('rank')==1).drop(col('rank'))
# display(final_dimartist)

# COMMAND ----------

# %sql
# CREATE TABLE IF NOT EXISTS SPOTIFY_CATA.SILVER.dimartist(
#    artist_key BIGINT,
#    artist_id STRING,
#    artist_name STRING,
#    genre STRING,
#    popularity integer, 
#    created_at timestamp,
#    updated_at timestamp,
#    watermark_ts timestamp,
#    ingestion_date date
# )
# using DELTA
# location 'abfss://silver@spotifyashishstorage.dfs.core.windows.net/dimartist/'

# COMMAND ----------

# silver_table = DeltaTable.forName(
#     spark,'spotify_cata.silver.dimartist'
# )
# silver_table.alias('target').merge(
#     final_dimartist.alias('source'),
#     'target.artist_id = source.artist_id'
# ).whenMatchedUpdateAll(condition = 'source.watermark_ts > target.watermark_ts').whenNotMatchedInsertAll().execute()