import Crawling_Basic as CB
import Crawling_Detail as CD
import pandas as pd
import Data_Load as DL
import time
import datetime


list_base_url = "https://www.yeogi.com/domestic-accommodations?"
region_list = ["경기", "제주도", "충남", "인천", "대구", "대전", "서울", "경남", "부산", "전북", "울산", "광주", "강원", "경북", "전남", "충북", "세종"]
Category_Mapping_table = {
    1 : "Motel",
    2 : "Hotel/Resort",
    5 : "Camping"
}
today_str = datetime.now().strftime('%Y-%m-%d')

driver = CB.crawling_setting()

total_accommodation_df, total_price_df, total_review_df = CB.Make_Basic_DataFrame()

# Extracting data from the WEBSITE - BASIC
for Category in Category_Mapping_table.keys():
    # for Category in Category_List:
    accommodation_Weekday_df, price_Weekday_df, review_Weekday_df  = CB.scrape_accommodation(driver, list_base_url, "Weekday", Category, region_list)
    accommodation_Holiday_df, price_Holiday_df, review_Holiday_df  = CB.scrape_accommodation(driver, list_base_url, "Holiday", Category, region_list)# weekday + holiday
    
    # 기본 정보 중복값 제거
    accommodation_df = pd.concat([accommodation_Weekday_df, accommodation_Holiday_df], ignore_index=True)
    accommodation_df = accommodation_df.drop_duplicates(['ACCOMMODATION_ID'])
    accommodation_df = accommodation_df.astype({'ACCOMMODATION_ID': 'int64'})
    total_accommodation_df = pd.concat([total_accommodation_df, accommodation_df], ignore_index=True)

    # weekday + holiday
    # 리뷰 데이터 중복값 제거
    review_df = pd.concat([review_Weekday_df, review_Holiday_df], ignore_index=True)
    review_df = review_df.drop_duplicates(['ACCOMMODATION_ID'])
    review_df = review_df.astype({'ACCOMMODATION_ID': 'int64', 'ACCOMMODATION_REVIEWCOUNT': 'int64'})
    total_review_df = pd.concat([total_review_df, review_df], ignore_index=True)

    # weekday + holiday
    price_df = pd.concat([price_Weekday_df, price_Holiday_df], ignore_index=True)
    price_df = price_df.astype({'ACCOMMODATION_ID': 'int64', 'PRICE': 'int64'})
    total_price_df = pd.concat([total_price_df, price_df], ignore_index=True)
    
    # 적재일 추가
    total_accommodation_df['LOAD_TIMESTAMP'] = today_str
    total_review_df['LOAD_TIMESTAMP'] = today_str
    total_price_df['LOAD_TIMESTAMP'] = today_str

# s3 업로드
DL.load_basic_data(total_accommodation_df, total_price_df, total_review_df)

accommodation_id_category_list = list(zip(total_accommodation_df['ACCOMMODATION_ID'], total_accommodation_df['ACCOMMODATION_MAINCATEGORY']))

# EXTRACTING DATA FROM THE WEBSITE - DETAIL
Location_df, Facilities_df = CD.crawl_detail_page(driver, accommodation_id_category_list)
Location_df = Location_df.astype({'ACCOMMODATION_ID': 'int64'})
Facilities_df = Facilities_df.astype({'ACCOMMODATION_ID': 'int64'})

Location_df['LOAD_TIMESTAMP'] = today_str
Facilities_df['LOAD_TIMESTAMP'] = today_str

DL.load_detail_data(Location_df, Facilities_df)