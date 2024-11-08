# Crawling_Script

## Crawling 실행 순서

### 1. Event Bridge를 통한 (Cron-tab : 매주 월요일 00:00:10) Event 발생

### 2. Lambda (Crawling_Lambda)<br>
      | 사용 라이브러리 : selenium<br>

### 3. Crawling_Lambda_Script.py 실행<br>

#### 3-1. Crawling_Basic 실행 : [selenium]을 이용한 크롤링<br>

#### Crawling_Data Load 실행<br>
- accommodation_table, accommodation_review_table, accommdation_price_table => parquet 형태로 buffer에 임시 저장<br><br>
- buffer : parquet 데이터 => S3 UPLOAD<br><br>
- Crawling_Detail 실행 : [selenium]과 [accommodation_table.accommodation_ID] 을 이용한 크롤링<br>

#### Crawling_Data Load 실행<br>
- accommodation_Location_table, accommodation_Facilities_table<br><br>
- buffer : parquet 데이터 => S3 UPLOAD<br>

### 4. 적재 완료시, Data_Load_Snowflake<br>
| 사용 라이브러리<br> : snowflake.connector

- S3/~~.parquet => snowflake.project2.RAW_DATA

- snowflake.project2.RAW_DATA => snowflake.project2.Analytics_tables

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
# 필수 Library 버전
- beautifulsoup4==4.12.3

- selenium==4.26.1

- boto3==1.35.54

- pandas==2.0.3

- snowflake-connector-python==3.12.3

- snowflake-sqlalchemy==1.6.1