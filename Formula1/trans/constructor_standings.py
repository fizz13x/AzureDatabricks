# Databricks notebook source
# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# 1. Get filtered DataFrame for given file_date
race_results_df = spark.read.format("delta").load(f"{presentation_folder}/race_results") \
    .filter(f"file_date = '{v_file_date}'")
 

# COMMAND ----------

race_years= re_arrange_partition_column(race_results_df, 'race_year')


# COMMAND ----------

race_year_list = [row.race_year for row in race_years.select("race_year").distinct().collect()]

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.format("delta").load(f"{presentation_folder}/race_results") \
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import when,col,count,sum

constructor_standings_df = race_results_df \
    .groupBy("race_year","team") \
        .agg(sum("points").alias("total_points"),
             count(when(col("position") == 1,True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import  Window
from pyspark.sql.functions import rank, desc

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))

final_df = constructor_standings_df.withColumn("rank",rank().over(constructor_rank_spec))



# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_table(final_df,'f1_presentation','constructor_standings',presentation_folder,merge_condition,"race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_year,count(*)
# MAGIC from f1_presentation.constructor_standings
# MAGIC group by race_year
# MAGIC order by race_year desc;