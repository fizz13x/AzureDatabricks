# Databricks notebook source
v_result = dbutils.notebook.run(
    "Ingest circuit File",
    0,
    {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"},
)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run(
    "Ingest Driver file",
    0,
    {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"},
)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run(
    "Ingest Constructor file",
    0,
    {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"},
)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run(
    "Ingest lap_times folder",
    0,
    {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"},
)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run(
    "Ingest pits_stops file",
    0,
    {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"},
)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run(
    "Ingest qualifying folder",
    0,
    {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"},
)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run(
    "Ingest Race File", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"}
)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run(
    "Ingest Results file",
    0,
    {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"},
)

# COMMAND ----------

v_result