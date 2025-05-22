# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Constructor.json file

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

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"


# COMMAND ----------

constructors_df = spark.read \
.schema(constructor_schema) \
.json(f"{raw_folder}/{v_file_date}/constructors.json")

# COMMAND ----------

constructor_drop = constructors_df.drop("url")
display(constructor_drop)

# COMMAND ----------

from pyspark.sql.functions import lit

constructor_final = constructor_drop.withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref") \
        .withColumn("data_source", lit(v_data_source)) \
            .withColumn("file_date", lit(v_file_date))


# COMMAND ----------

constructor_final=add_ingestion_date(constructor_final)

# COMMAND ----------

constructor_final.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")