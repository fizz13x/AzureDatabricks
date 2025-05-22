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

from pyspark.sql.types import StringType, StructType, StructField, IntegerType
qualifying_schema = StructType([
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option("multiline", "true").json(f"{raw_folder}/{v_file_date}/qualifying")



# COMMAND ----------

from pyspark.sql.functions import lit

date_ingest = add_ingestion_date(qualifying_df)

qualifying_df_final = date_ingest.withColumnRenamed("qualifyId", "qualify_Id") \
                            .withColumnRenamed("raceId", "race_id") \
                            .withColumnRenamed("driverId", "driver_id") \
                            .withColumnRenamed("constructorId", "constructor_id") \
                            .withColumn("data_source", lit(v_data_source)) \
                            .withColumn("file_date", lit(v_file_date))




# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_table(qualifying_df_final,'f1_processed','qualifying',processed_folder,merge_condition,"race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed.qualifying;