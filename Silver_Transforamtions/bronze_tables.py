# Databricks notebook source
# MAGIC %sql
# MAGIC drop table spotify_cata.bronze.factstream_raw

# COMMAND ----------

df = spark.read.parquet('abfss://bronze@spotifyashishstorage.dfs.core.windows.net/factStream/')
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table spotify_cata.bronze.factstream_raw
# MAGIC using parquet
# MAGIC location 'abfss://bronze@spotifyashishstorage.dfs.core.windows.net/factStream/'

# COMMAND ----------

# MAGIC %sql
# MAGIC grant select on table spotify_cata.bronze.factstream_raw to `ashishhandles1298@gmail.com`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists spotify_cata.bronze.dimuser_raw
# MAGIC using parquet
# MAGIC location 'abfss://bronze@spotifyashishstorage.dfs.core.windows.net/dimUser/'

# COMMAND ----------

# MAGIC %sql
# MAGIC grant select on spotify_cata.bronze.dimuser_raw to `ashishhandles1298@gmail.com`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists spotify_cata.bronze.dimartist_raw
# MAGIC using parquet
# MAGIC location 'abfss://bronze@spotifyashishstorage.dfs.core.windows.net/dimArtist/'

# COMMAND ----------

# MAGIC %sql
# MAGIC grant select on spotify_cata.bronze.dimartist_raw to `ashishhandles1298@gmail.com`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists spotify_cata.bronze.dimtrack_raw
# MAGIC using parquet
# MAGIC location 'abfss://bronze@spotifyashishstorage.dfs.core.windows.net/dimTrack/'

# COMMAND ----------

# MAGIC %sql
# MAGIC grant select on spotify_cata.bronze.dimtrack_raw to `ashishhandles1298@gmail.com`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spotify_cata.bronze.dimartist_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spotify_cata.bronze.dimtrack_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists spotify_cata.bronze.dimdate_raw
# MAGIC using parquet
# MAGIC location 'abfss://bronze@spotifyashishstorage.dfs.core.windows.net/dimDate/'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS spotify_cata.gold;