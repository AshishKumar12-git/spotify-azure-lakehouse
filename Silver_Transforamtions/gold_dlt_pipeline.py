# Databricks notebook source
import dlt
from pyspark.sql.functions import *  

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimUser 

# COMMAND ----------

@dlt.view 
def dim_user_source():
  return spark.read.table('spotify_cata.silver.dimuser')

# COMMAND ----------

dlt.apply_changes(
    target = 'dim_user',
    source = 'dim_user_source',
    keys = ['user_id'],
    sequence_by = col('watermark_ts'),
    stored_as_scd_type = '2',
    except_column_list = ['watermark_ts','signup_date','created_at','updated_at','ingestion_date']
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimArtist

# COMMAND ----------

@dlt.view
def dim_artist_source():
    return spark.read.table('spotify_cata.silver.dimartist')

# COMMAND ----------

dlt.apply_changes(
  target = 'dim_artist',
  source = 'dim_artist_source',
  keys = ['artist_id'],
  sequence_by= col('watermark_ts'),
  stored_as_scd_type = '2',
  except_column_list = ['watermark_ts','popularity','created_at','updated_at','ingestion_date']
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimTrack

# COMMAND ----------

@dlt.view
def dim_track_source():
    return spark.read.table('spotify_cata.silver.dimtrack')

# COMMAND ----------

dlt.apply_changes(
    target = 'dim_track',
    source = 'dim_track_source',
    keys = ['track_id'],
    sequence_by= col('watermark_ts'),
    stored_as_scd_type = '2',
    except_column_list= ['watermark_ts','release_date','created_at','updated_at','ingestion_date']
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimDate

# COMMAND ----------

@dlt.view
def dim_date_source():
    return spark.read.table('spotify_cata.silver.dimdate')
    

# COMMAND ----------

dlt.apply_changes(
    target = 'dim_date',
    source = 'dim_date_source',
    keys = ['date_key'],
    sequence_by= col('ingestion_date'),
    except_column_list = ['ingestion_date']
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### factstream

# COMMAND ----------


@dlt.table(
    name="fact_stream",
    comment="Gold fact table for Spotify streaming events"
)
def fact_stream():

    
    fact = spark.read.table("spotify_cata.silver.factstream_silver")

   
    dim_user = spark.read.table("spotify_cata.gold.dim_user")
    dim_track = spark.read.table("spotify_cata.gold.dim_track")
    dim_artist = spark.read.table("spotify_cata.gold.dim_artist")
    dim_date = spark.read.table("spotify_cata.gold.dim_date")

    fact = fact.withColumn(
        "stream_date",
        col("stream_timestamp").cast("date")
    )

   
    fact = fact.join(
        dim_user.select("user_id", "user_key"),
        "user_id",
        "left"
    )


    fact = fact.join(
        dim_track.select("track_id", "track_key"),
        "track_id",
        "left"
    )

 
    fact = fact.join(
        dim_artist.select("artist_id", "artist_key"),
        "artist_id",
        "left"
    )

  
    fact = fact.join(
        dim_date.select("date_key", "full_date"),
        fact.stream_date == dim_date.full_date,
        "left"
    )

    return fact.select(
        col("stream_id"),
        col("user_key"),
        col("track_key"),
        col("artist_key"),
        col("date_key"),
        col("user_id"),
        col("track_id"),
        col("artist_id"),
        col("play_minutes"),
        col("play_seconds"),
        col("skipped"),
        col("platform")
    )