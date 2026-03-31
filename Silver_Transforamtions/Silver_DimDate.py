# Databricks notebook source
df = spark.table('spotify_cata.bronze.dimdate_raw')
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

df_clean = df.select(
    col("date_key").cast("int").alias("date_key"),
    col("full_date").cast("date").alias("full_date"),
    col("year").cast("int").alias("year"),
    col("quarter").cast("int").alias("quarter"),
    col("month").cast("int").alias("month"),
    col("month_name").cast("string").alias("month_name"),
    col("week_of_year").cast("int").alias("week_of_year"),
    col("day_of_month").cast("int").alias("day_of_month"),
    col("day_name").cast("string").alias("day_name"),
    col("is_weekend").cast("boolean").alias("is_weekend"),
    col("ingestion_date").cast("date").alias("ingestion_date")
)

# COMMAND ----------

df_clean = df_clean.filter(col("date_key").isNotNull())

# COMMAND ----------

df_clean = df_clean.dropDuplicates(["date_key"])

# COMMAND ----------

df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option(
        "path",
        "abfss://silver@spotifyashishstorage.dfs.core.windows.net/dimdate/"
    ) \
    .saveAsTable("spotify_cata.silver.dimdate")