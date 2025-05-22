-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION '/mnt/adbsa11/process';

-- COMMAND ----------

describe database f1_processed;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION '/mnt/adbsa11/presentation';

-- COMMAND ----------

DROP DATABASE f1_presentation CASCADE;