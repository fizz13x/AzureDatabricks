# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest race.csv file

# COMMAND ----------

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType,DateType

race_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                   StructField("year", IntegerType(), True),
                                   StructField("round", IntegerType(), True),
                                   StructField("circuitId", IntegerType(), True),
                                   StructField("name", StringType(), True),
                                   StructField("date", DateType(), True),
                                   StructField("time", StringType(), True),
                                   StructField("url", StringType(), True)])



# COMMAND ----------

race_csv = spark.read \
    .option("header",True) \
        .schema(race_schema) \
    .csv(f"{raw_folder}/{v_file_date}/races.csv")

# COMMAND ----------

from pyspark.sql.functions import col

race_select_df = race_csv.select(col("raceId").alias("race_id"),
                                 col("year").alias("race_year"),
                                 col("round"),
                                 col("circuitId").alias("circuit_id"),
                                 col("name"),
                                 col("date"),
                                 col("time"))


# COMMAND ----------

from pyspark.sql.functions import  lit , to_timestamp, concat

date_df = add_ingestion_date(race_select_df)

race_final_df=date_df.withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),"yyyy-MM-dd HH:mm:ss")) \
    .withColumn("data_source",lit(v_data_source)) \
        .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

race_final_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")