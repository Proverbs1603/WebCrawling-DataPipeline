## **프로젝트 개요**

본 프로젝트는 **지역별, 유형별, 날짜별 변화하는 숙박업소(여기어때)의 데이터를 자동으로 수집, 저장, 분석, 및 시각화**하는 데이터 파이프라인 구축을 목표로 진행되었습니다. 주기적으로 데이터를 업데이트하고, 시각화 대시보드를 통해 국내 숙박업에 대한 다양한 인사이트를 제공하고자 하였습니다. 이러한 목적으로 데이터 수집, 저장, 분석, 시각화까지 다양한 기술들을 활용하였습니다.

### **프로젝트 주요 내용**

- **데이터 수집 및 업데이트 자동화**  
  - **Python, BeautifulSoup, Selenium** 등의 웹 스크래핑 라이브러리를 활용해 지역별, 유형별 숙박업소 데이터를 수집.  
  - **AWS EventBridge**와 **AWS Lambda**를 활용한 **cron 스케줄링**으로 데이터 수집 프로세스를 자동화하여 최신화된 데이터를 주기적으로 업데이트.  

- **데이터 저장 및 관리**  (ETL)
  - 수집된 데이터를 **AWS S3**에 저장해 대용량 데이터를 안전하게 보관 및 관리.  
  - **Snowflake**로 데이터를 이관하여 분석 쿼리를 실행하고 다양한 인사이트를 도출.  

- **데이터 분석 및 시각화**  (ELT)
  - 분석된 결과는 **Preset 대시보드**로 시각화하여 직관적인 정보를 제공.  
  - 숙박업소의 **지역별**, **숙박 유형별**, **날짜별 특성**을 분석하고, 수요 변화 경향성을 파악 가능.  

---


# **Automated Data Pipeline with AWS Lambda, EventBridge, S3, Snowflake, and Preset**

![image](https://github.com/user-attachments/assets/82f7e334-3467-430f-822d-c43ce0a2a8dd)



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
