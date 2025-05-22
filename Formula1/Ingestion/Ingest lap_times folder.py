# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/config"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

from pyspark.sql.types import StringType, StructType, StructField, IntegerType, TimestampType
laptimes_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

lapTimes_df = spark.read.schema(laptimes_schema).csv(f"{raw_folder}/{v_file_date}/lap_times")



# COMMAND ----------

from pyspark.sql.functions import lit

date_ingest = add_ingestion_date(lapTimes_df)

lapTimes_df_final = date_ingest.withColumnRenamed("raceId", "race_id") \
                            .withColumnRenamed("driverId", "driver_id") \
                            .withColumn("data_source", lit(v_data_source)) \
                            .withColumn("file_date", lit(v_file_date))



# COMMAND ----------



# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap"
merge_delta_table(lapTimes_df_final,'f1_processed','lap_times',processed_folder,merge_condition,"race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.lap_times;