# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest circuit.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuit_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                   StructField("circuitRef", StringType(), True),
                                   StructField("name", StringType(), True),
                                   StructField("location", StringType(), True),
                                   StructField("country", StringType(), True),
                                   StructField("lat", DoubleType(), True),
                                   StructField("lng", DoubleType(), True),
                                   StructField("alt", IntegerType(), True),
                                   StructField("url", StringType(), True)])



# COMMAND ----------

circuit_csv = spark.read \
    .option("header",True) \
        .schema(circuit_schema) \
    .csv(f"{raw_folder}/{v_file_date}/circuits.csv")

# COMMAND ----------

from pyspark.sql.functions import col

circuit_select_df = circuit_csv.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat").alias("latitude"), col("lng").alias("longitude"), col("alt"))


# COMMAND ----------

from pyspark.sql.functions import lit

circuit_select_df = (
    circuit_select_df
    .withColumnRenamed("circuitId", "circuit_id")
    .withColumnRenamed("circuitRef", "circuit_ref")
    .withColumnRenamed("alt", "altitude")
    .withColumn("data_source", lit(v_data_source))
    .withColumn("file_date", lit(v_file_date))
)


# COMMAND ----------


circuit_final_df=add_ingestion_date(circuit_select_df)


# COMMAND ----------

circuit_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * from f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")