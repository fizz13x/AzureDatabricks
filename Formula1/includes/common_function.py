# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
  output_df = input_df.withColumn("created_date", current_timestamp())
  return output_df

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_col):
    column_list = [col_name for col_name in input_df.columns if col_name != partition_col]
    column_list.append(partition_col)
    return input_df.select(column_list)



# COMMAND ----------

def overwrite_partition(input_df,db_name,table_name,partition_column):
    output_df =re_arrange_partition_column(input_df,partition_column)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        #Insert into the existing table if created
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        #create the table
        output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def merge_delta_table(input_df,db_name,table_name,folder_path,merge_condition,partition_column):
    
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    from delta.tables import DeltaTable


    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(
        input_df.alias("src"),
        merge_condition) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    else:
#create the table
        input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC