-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits (
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING)
  USING csv
  OPTIONS (
    path '/mnt/adbsa11/raw/circuits.csv',header True)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;

CREATE TABLE IF NOT EXISTS f1_raw.races (
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING)
  USING csv
  OPTIONS (
    path '/mnt/adbsa11/raw/races.csv', header True)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;

CREATE TABLE IF NOT EXISTS f1_raw.constructors (
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING)
  USING json
  OPTIONS (
    path '/mnt/adbsa11/raw/constructors.json')

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers (
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING)
  USING json
  OPTIONS (
    path '/mnt/adbsa11/raw/drivers.json')

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results (
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points DOUBLE,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed DOUBLE,
  statusId INT)
  USING json
  OPTIONS (
    path '/mnt/adbsa11/raw/results.json')

-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops (
  driverId INT,
  raceId INT,
  stop INT,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds INT)
  USING JSON
  OPTIONS (
    path '/mnt/adbsa11/raw/pit_stops.json',
    multiline True)

-- COMMAND ----------

select * from f1_raw.pit_stops;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times (
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT)
  USING csv
  OPTIONS (
    path '/mnt/adbsa11/raw/lap_times',
    header True)

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying (
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING)
  USING json
  OPTIONS (
    path '/mnt/adbsa11/raw/qualifying' , multiline True)


-- COMMAND ----------

select * from f1_raw.qualifying;