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

from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DateType

name_schema = StructType([
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)])

driver_schema = StructType([
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

driver_df=spark.read.schema(driver_schema).json(f"{raw_folder}/{v_file_date}/drivers.json")


# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp ,lit

driver_df_with_ingestion = add_ingestion_date(driver_df)

driver_columns = driver_df_with_ingestion \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
    .withColumn("data_source", lit(v_data_source)) \
        .withColumn("file_date", lit(v_file_date))
                         


# COMMAND ----------

driver_final = driver_columns.drop("url")
display(driver_final)


# COMMAND ----------

driver_final.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")