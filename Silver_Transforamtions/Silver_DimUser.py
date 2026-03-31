# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

bronze_path = 'abfss://bronze@spotifyashishstorage.dfs.core.windows.net/dimUser'
schema_location = 'abfss://silver@spotifyashishstorage.dfs.core.windows.net/schema/dimuser'
checkpoint_location = 'abfss://silver@spotifyashishstorage.dfs.core.windows.net/checkpoints/dimuser'

# COMMAND ----------

dim_user = spark.readStream.format('cloudFiles').option('cloudFiles.format','parquet').\
    option('cloudFiles.schemaLocation',schema_location).\
        load(bronze_path)

# COMMAND ----------

def transform_dimuser(dim_user):

    dim_frame = dim_user.select(
    col('user_key').cast('long'),
    col('user_id').cast('string'),
    col('country').cast('string'),
    col('subscription_type').cast('string'),
    col('signup_date').cast('date'),
    col('is_active').cast('boolean'),
    col('created_at').cast('timestamp'),
    col('updated_at').cast('timestamp'),
    col('watermark_ts').cast('timestamp'),
    col('ingestion_date').cast('date')
)
    dim = dim_frame.filter(col('user_id').isNotNull())
    dimuser = dim.withColumn('country',upper(col('country'))).withColumn('subscription_type',lower(col('subscription_type'))).\
    fillna({'country':'N/A','subscription_type':'unknown'})
    return dimuser



# COMMAND ----------

dim_processed = transform_dimuser(dim_user)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS SPOTIFY_CATA.SILVER.dimuser(
# MAGIC    user_key BIGINT,
# MAGIC    user_id STRING,
# MAGIC    country STRING,
# MAGIC    subscription_type STRING,
# MAGIC    signup_date DATE,
# MAGIC    is_active boolean, 
# MAGIC    created_at timestamp,
# MAGIC    updated_at timestamp,
# MAGIC    watermark_ts timestamp,
# MAGIC    ingestion_date date
# MAGIC )
# MAGIC using DELTA
# MAGIC location 'abfss://silver@spotifyashishstorage.dfs.core.windows.net/dimuser/'

# COMMAND ----------

def upsert_to_silver(MicroBatchDF, batchID):
    window = Window.partitionBy('user_id').orderBy(col('watermark_ts').desc())
    dim_dedup = MicroBatchDF.withColumn('rank',row_number().over(window)).filter(col('rank')==1).drop(col('rank'))
    silver_table = DeltaTable.forName(
        spark,'spotify_cata.silver.dimuser'
    )
    silver_table.alias('target').merge(
        dim_dedup.alias('source'),'target.user_id = source.user_id'
    ).whenMatchedUpdateAll(condition='source.watermark_ts > target.watermark_ts').whenNotMatchedInsertAll().execute()

# COMMAND ----------

query = dim_processed.writeStream.foreachBatch(upsert_to_silver).option('checkpointLocation',checkpoint_location).\
    trigger(availableNow=True).start()
query.awaitTermination()

# COMMAND ----------

dim_user = spark.table('spotify_cata.bronze.dimuser_raw')
display(dim_user)

# COMMAND ----------

dim_user.printSchema()

# COMMAND ----------

# dim_frame = dim_user.select(
#     col('user_key').cast('long'),
#     col('user_id').cast('string'),
#     col('country').cast('string'),
#     col('subscription_type').cast('string'),
#     col('signup_date').cast('date'),
#     col('is_active').cast('boolean'),
#     col('created_at').cast('timestamp'),
#     col('updated_at').cast('timestamp'),
#     col('watermark_ts').cast('timestamp'),
#     col('ingestion_date').cast('date')
# )
# dim_frame.printSchema()

# COMMAND ----------

# dim = dim_frame.filter(col('user_id').isNotNull())
# dimuser = dim.withColumn('country',upper(col('country'))).withColumn('subscription_type',lower(col('subscription_type'))).\
#     fillna({'country':'N/A','subscription_type':'unknown'})
# display(dimuser)
                                                                     

# COMMAND ----------

# window = Window.partitionBy('user_id').orderBy(col('watermark_ts').desc())
# dim_dedup = dimuser.withColumn('rank',row_number().over(window)).filter(col('rank')==1).drop('rank')

# COMMAND ----------

# display(dim_dedup)

# COMMAND ----------

# %sql
# CREATE TABLE IF NOT EXISTS SPOTIFY_CATA.SILVER.dimuser(
#    user_key BIGINT,
#    user_id STRING,
#    country STRING,
#    subscription_type STRING,
#    signup_date DATE,
#    is_active boolean, 
#    created_at timestamp,
#    updated_at timestamp,
#    watermark_ts timestamp,
#    ingestion_date date
# )
# using DELTA
# location 'abfss://silver@spotifyashishstorage.dfs.core.windows.net/dimuser/'

# COMMAND ----------

# silver_table = DeltaTable.forName(
#     spark, 'spotify_cata.silver.dimuser'
# )
# silver_table.alias('target').merge(
#     dim_dedup.alias('source'), 'target.user_id = source.user_id'
# ).whenMatchedUpdateAll(condition= 'source.watermark_ts > target.watermark_ts').whenNotMatchedInsertAll().execute()
