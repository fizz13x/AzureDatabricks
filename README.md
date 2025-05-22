# 🏎️ Formula 1 Data Engineering Project with Azure Databricks & Spark

---

## 🎯 Objective
Build a modern data lakehouse pipeline using **Spark on Databricks** to process real-world **Formula 1 racing data**.  
The pipeline covers **batch ingestion**, **incremental loading**, **Delta Lake optimization**, and **presentation layer design**.

- Ingest raw racing data
- Clean, transform, and aggregate into analytical datasets
- Present results via Delta Lake tables for reporting

---

## 🚀 Technologies Used

- **Azure Data Lake Storage Gen2 (ADLS Gen2)**
- **Azure Databricks**
- **Azure Data Factory (ADF)**
- **Delta Lake**
- **Azure Key Vault**

---

## 🛠️ Key Features

- ✅ **Data Ingestion** with schema enforcement
- ✅ **Bronze → Silver → Gold** data layer architecture
- ✅ **Partitioning & Optimization** (Z-Ordering, Vacuum, etc.)
- ✅ **Delta Lake Merge** for incremental loads
- ✅ **Window Functions**, `rank()`, `dense_rank()` for leaderboards
- ✅ **Dynamic Widgets** for parameterized notebook execution
- ✅ **Integration with Azure Data Factory** for orchestration

## 🏗️ Project Architecture

```text
📂 Raw CSV (Landing Zone)
   ↓
🥉 Bronze Layer (Raw Ingestion using Autoloader or manual)
   ↓
🥈 Silver Layer (Cleansed and joined datasets)
   ↓
🥇 Gold Layer (Aggregated facts like driver/constructor standings)
   ↓
📈 Presented as Delta Tables for Analytics
```
---


## 🔄 End-to-End Pipeline Flow


### 💡 Pipeline Trigger
- Tumbling window trigger
- Future scope: Event-based triggers via blob change detection
<img width="750" alt="Trigger Runs" src="https://github.com/user-attachments/assets/66f26405-7a86-4eee-ad7a-51d51566c1b0" />


### 🔐 Security
- All secrets (SAS token, connection strings) are stored securely in Azure Key Vault
- Databricks accesses Key Vault using secret scopes



### 1. **Source File Upload**
- CSV file is uploaded to the `raw` container in ADLS.

### 2. **ADF Pipeline Execution**
- ADF pipeline is created to trigger the Databricks notebooks in sequence:
  - Load to Raw Layer
  - Transform and Load to Curated Layer
  - Final Transformation and Load to Presentation Layer
 
<img width="750" alt="Main Pipeline" src="https://github.com/user-attachments/assets/e4c49f95-5ee5-4e31-948a-414284ad35e7" />


### 3. **Databricks Notebook Process**

#### Process 1: Load to Raw Layer
- Reads the CSV file from blob storage
- Writes the raw data to ADLS Gen2 (`raw` container)
<img width="750" alt="Ingest Raw Data Pipeline" src="https://github.com/user-attachments/assets/199cc7b0-2afc-48cd-8cbd-fa40ee612a26" />



####  Process 2: Raw to Curated
- Reads data from the raw layer
- Cleans and transforms the dataset:
  - Handles null values
  - Type casting
  - Removes duplicates
- Writes cleaned data to `curated` container in Delta format

<img width="750" alt="Ingest Trasform data" src="https://github.com/user-attachments/assets/014c25a7-dfbb-4997-ba9c-687186439873" />


#### Process 3: Curated to Presentation
- Joins fact and dimension tables
- Applies filters and selects final columns
- Writes final dataset to the `presentation` container

---


## ✅ Outcomes

- Final dataset is ready for Power BI or analytics consumption
- Delta format enables fast querying and data lakehouse integration

## 📌 Summary

This project delivers a modular, layered, and secure data engineering pipeline on Azure, showcasing best practices in:

- Cloud storage organization
- PySpark transformations
- Secret management
- Scalable orchestration with ADF

---

