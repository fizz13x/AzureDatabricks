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

from pyspark.sql.types import StringType, StructType, StructField, IntegerType, FloatType
result_schema = StructType([
  StructField("resultId", IntegerType(), True),
  StructField("raceId", IntegerType(), True),
  StructField("driverId", IntegerType(), True),
  StructField("constructorId", IntegerType(), True),
  StructField("number", IntegerType(), True),
  StructField("grid", IntegerType(), True),
  StructField("position", IntegerType(), True),
  StructField("positionText", StringType(), True),
  StructField("positionOrder", IntegerType(), True),
  StructField("points", FloatType(), True),
  StructField("laps", IntegerType(), True),
  StructField("time", StringType(), True),
  StructField("milliseconds", IntegerType(), True),
  StructField("fastestLap", IntegerType(), True),
  StructField("rank", IntegerType(), True),
  StructField("fastestLapTime", StringType(), True),
  StructField("fastestLapSpeed", StringType(), True),
  StructField("statusId", IntegerType(), True)
])

# COMMAND ----------

result_df=spark.read.schema(result_schema).json(f"{raw_folder}/{v_file_date}/results.json")

# COMMAND ----------

from pyspark.sql.functions import col,lit

date_ingest = add_ingestion_date(result_df)

result_final= date_ingest.withColumnRenamed("resultId","result_id") \
                        .withColumnRenamed("raceId","race_id") \
                        .withColumnRenamed("driverId","driver_id") \
                        .withColumnRenamed("constructorId","constructor_id") \
                        .withColumnRenamed("positionText","position_text") \
                        .withColumnRenamed("positionOrder","position_order") \
                        .withColumnRenamed("fastestLap","fastest_lap") \
                        .withColumnRenamed("fastestLapTime","fastest_lap_time") \
                        .withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
                        .withColumn("data_source",lit(v_data_source)) \
                        .withColumn("file_date",lit(v_file_date)) \
                        .drop(col("statusId"))
                         


# COMMAND ----------

result_deduped = result_final.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ###Method 1 -Incremental Load

# COMMAND ----------

# for race_id_list in result_final.select("race_id").distinct().collect():
#     if spark._jsparkSession.catalog().tableExists("f1_processed.results"):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# result_final.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_table(result_deduped,'f1_processed','results',processed_folder,merge_condition,"race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

dbutils.notebook.exit("Success")