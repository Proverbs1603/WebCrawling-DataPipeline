# **Automated Data Pipeline with AWS Lambda, EventBridge, S3, Snowflake, and Preset**

> **A modern and automated solution for web data scraping, processing, warehousing, and visualization.**

---

## 📖 **Overview**

This repository contains an automated data pipeline using:
- **AWS EventBridge**: Scheduled triggering of tasks.
- **AWS Lambda**: Execution of web scraping and data transformation.
- **AWS S3**: Intermediate storage of processed data in **Parquet format**.
- **Snowflake**: Data warehousing for advanced analytics and reporting.
- **Preset (Apache Superset)**: Interactive dashboards for visualization of processed data.

---

## 📅 **Crawling Execution Flow**

### **1. Event Trigger (AWS EventBridge)**
- **Schedule**: Every Monday at `00:00:10` (Cron-tab).

### **2. Lambda Execution**
- **Function**: `Crawling_Lambda`
- **Library Used**: `selenium`

### **3. Crawling Workflow**
#### **3-1. Crawling_Basic Execution**
- **Purpose**: Extract basic data using `selenium`.
- **Data Extracted**:
  - `accommodation_table`
  - `accommodation_review_table`
  - `accommodation_price_table`
- **Data Processing**:
  - Data is saved in **Parquet format** to a temporary buffer.
  - Parquet files are uploaded to **S3**.

#### **3-2. Crawling_Detail Execution**
- **Purpose**: Perform detailed crawling using `accommodation_table.accommodation_ID`.
- **Additional Data Extracted**:
  - `accommodation_Location_table`
  - `accommodation_Facilities_table`
- **Data Processing**:
  - Data is saved in **Parquet format** and uploaded to **S3**.

### **4. Data Loading to Snowflake**
- **Library Used**: `snowflake.connector`
- **Workflow**:
  1. **S3 Parquet data → snowflake.project2.RAW_DATA**
  2. **RAW_DATA → snowflake.project2.Analytics_tables**

### **5. Data Visualization with Preset**
- **Visualization Tool**: Preset (Apache Superset).
- **Purpose**: Create interactive dashboards and visualizations for:
  - Accommodation trends.
  - Review sentiment analysis.
  - Price comparisons and more.


---

# Local 실행 시 순서
```
> pip install -r requirements.txt
```

매주 월요일 00:00:10분에 실행되도록 cron scheduling 생성 후

```
> python Crawling_Lambda_Script.py
> python Load_Snowflake_Lambda_Script.py
```
=> 실행

# AWS 구성시
1. Event Bridge : Cron 매주 월요일 00:00:10

2. Lambda 구성
![alt text](./Lambda.png)

[Trigger : Event Bridge] -> [Lambda : Crawling_Lambda_Script.py] => [이전 성공 시 : python Load_Snowflake_Lambda_Script.py]

---

## 🔧 **Required Libraries and Versions**

| **Library**                   | **Version**    |
|--------------------------------|----------------|
| `beautifulsoup4`              | 4.12.3         |
| `selenium`                    | 4.26.1         |
| `boto3`                       | 1.35.54        |
| `pandas`                      | 2.0.3          |
| `snowflake-connector-python`  | 3.12.3         |
| `snowflake-sqlalchemy`        | 1.6.1          |
| `apache-superset`             | Latest         |
