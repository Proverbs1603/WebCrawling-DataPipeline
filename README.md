<img width="603" alt="image" src="https://github.com/user-attachments/assets/7995c4bb-dbd8-4dbb-9e5f-5b91de66c0ee" />

<img width="603" alt="image" src="https://github.com/user-attachments/assets/b9b75855-81ca-4bae-acad-b2b6cb6db674" />

<img width="603" alt="image" src="https://github.com/user-attachments/assets/d2b175bd-f657-4879-9f0c-064ae782e651" />

<img width="603" alt="image" src="https://github.com/user-attachments/assets/224e8676-b2a3-4053-9edc-5421b6a85998" />

<img width="603" alt="image" src="https://github.com/user-attachments/assets/7c28aeca-d268-4060-b11e-5b6d15c068ac" />

<img width="603" alt="image" src="https://github.com/user-attachments/assets/0df70ef7-85f3-4f14-b74c-418fb14f9b3e" />

<img width="603" alt="image" src="https://github.com/user-attachments/assets/3b70e7b7-eadd-4f1c-8075-b77b7237e1a7" />

<img width="603" alt="image" src="https://github.com/user-attachments/assets/9a43745c-918a-40cd-9e51-071805fe3d8c" />

<img width="603" alt="image" src="https://github.com/user-attachments/assets/2f8507c7-356b-437b-9d41-e8e27527af1a" />

<img width="603" alt="image" src="https://github.com/user-attachments/assets/ce96bea1-7ed5-45c6-9e9d-a2ef0c9907f8" />

<img width="603" alt="image" src="https://github.com/user-attachments/assets/a36c09ae-4e3d-44d6-95ef-b7e7197446be" />

<img width="603" alt="image" src="https://github.com/user-attachments/assets/8c024f56-aa75-4384-aa6f-933581bac4b0" />

<img width="603" alt="image" src="https://github.com/user-attachments/assets/09e70c3b-a44f-47be-9bce-322c1438a185" />

<img width="603" alt="image" src="https://github.com/user-attachments/assets/3d51e936-7634-4325-859d-9b1ad414fc37" />

<img width="603" alt="image" src="https://github.com/user-attachments/assets/39b25994-4722-4bc1-8773-fe4c92f3c076" />

<img width="603" alt="image" src="https://github.com/user-attachments/assets/786da6f4-95f2-432d-8cc0-0848dbdf411b" />

<img width="603" alt="image" src="https://github.com/user-attachments/assets/89e9eb76-4625-42d8-8853-dee232471389" />

<img width="603" alt="image" src="https://github.com/user-attachments/assets/f88a49bf-99e7-4e72-b95b-e70d32d703f3" />

<img width="603" alt="image" src="https://github.com/user-attachments/assets/751f6824-9509-4f0e-8af9-405ae98a57a4" />

---

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
