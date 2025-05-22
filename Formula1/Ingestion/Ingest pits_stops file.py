# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

from pyspark.sql.types import StringType, StructType, StructField, IntegerType, TimestampType
pitstop_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

pit_stops_df = spark.read.schema(pitstop_schema) \
.option("multiline",True).json(f"{raw_folder}/{v_file_date}/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import lit

date_ingest = add_ingestion_date(pit_stops_df)

final_pits_df = date_ingest.withColumnRenamed("raceId", "race_id") \
                            .withColumnRenamed("driverId", "driver_id") \
                            .withColumn("data_source", lit(v_data_source)) \
                            .withColumn("file_date", lit(v_file_date))



# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"
merge_delta_table(final_pits_df,'f1_processed','pit_stops',processed_folder,merge_condition,"race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops;