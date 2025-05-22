# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

driver_df=spark.read.format("delta").load(f"{processed_folder}/drivers") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("nationality", "driver_nationality")
 

# COMMAND ----------

constructors_df=spark.read.format("delta").load(f"{processed_folder}/constructors") \
    .withColumnRenamed("name", "team")

# COMMAND ----------

circuit_df=spark.read.format("delta").load(f"{processed_folder}/circuits") \
    .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

race_df=spark.read.format("delta").load(f"{processed_folder}/races") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

race_circuit_df = race_df.join(circuit_df,race_df.circuit_id==circuit_df.circuit_id, "inner") \
    .select(race_df.race_id, race_df.race_year, race_df.race_name, race_df.race_date, circuit_df.circuit_location)

# COMMAND ----------

result_df=spark.read.format("delta").load(f"{processed_folder}/results") \
    .filter(f"file_date='{v_file_date}'") \
    .withColumnRenamed("time", "race_time") \
    .withColumnRenamed("race_id", "result_race_id") \
    .withColumnRenamed("file_date", "result_file_date")


# COMMAND ----------

race_result_df=result_df.join(race_circuit_df, result_df.result_race_id==race_circuit_df.race_id) \
    .join(driver_df, result_df.driver_id==driver_df.driver_id) \
        .join(constructors_df, result_df.constructor_id==constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_race_df = race_result_df.select("race_id","race_year", "race_name", "race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time", "points", "position","result_file_date") \
    .withColumn("created_date", current_timestamp()) \
    .withColumnRenamed("result_file_date", "file_date")


# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_table(final_race_df,'f1_presentation','race_results',presentation_folder,merge_condition,"race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(*)
# MAGIC from f1_presentation.race_results
# MAGIC group by race_id
# MAGIC order by race_id desc;
# MAGIC