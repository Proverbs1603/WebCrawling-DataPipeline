# Crawling_Script

### Crawling 실행 순서

1. Event Bridge를 통한 (Cron-tab : 매주 월요일 00:00:10) Event 발생

2. Lambda (Crawling_Lambda)<br>
   | 사용 라이브러리 : selenium<br>

3. Crawling_Lambda_Script.py 실행<br>

3-1. Crawling_Basic 실행 : [selenium]을 이용한 크롤링<br>

Crawling_Data Load 실행<br>
3-2. accommodation_table, accommodation_review_table, accommdation_price_table => parquet 형태로 buffer에 임시 저장<br>
3-3. buffer : parquet 데이터 => S3 UPLOAD<br>
3-4. Crawling_Detail 실행 : [selenium]과 [accommodation_table.accommodation_ID] 을 이용한 크롤링<br>

Crawling_Data Load 실행<br>
3-5. accommodation_Location_table, accommodation_Facilities_table<br>
3-6. buffer : parquet 데이터 => S3 UPLOAD<br>

5. 적재 완료시, Data_Load_Snowflake<br>
| 사용 라이브러리<br>
