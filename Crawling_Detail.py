from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException

from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

import time

import requests
import datetime

import os

import pandas as pd

def Make_Detail_DataFrame():
    # Dataframe 셋팅
    accommodations_Location_frame = {
        'ACCOMMODATION_ID': [],
        'ACCOMMODATION_LOCATION_MAJOR': [],
        'ACCOMMODATION_LOCATION_MIDDLE': [],
        'ACCOMMODATION_LOCATION_SUB': [],
        'ACCOMMODATION_LOCATION_DETAIL': []
    }
    Location_df = pd.DataFrame(accommodations_Location_frame)
        
    accommodation_Facilities_frame = {
        'ACCOMMODATION_ID': [],
        'ACCOMMODATION_MAINCATEGORY': [],
        'ACCOMMODATION_FACILITIES': []
    }
    Facilities_df = pd.DataFrame(accommodation_Facilities_frame)
    
    return Location_df, Facilities_df

def extract_accommodation_location(driver):
    address_span = driver.find_element(By.CSS_SELECTOR, "div.css-z8nsir span.css-1t5t2dt")
    # Get the text from the <span> element
    address_text = address_span.text.strip()

    # Split the text by spaces
    parts = address_text.split()

    # Prepare the variables based on the number of parts
    if len(parts) >= 4:
        first_part = parts[0]
        second_part = parts[1]
        third_part = parts[2]
        fourth_part = ' '.join(parts[3:])  # Join remaining parts as fourth_part
    elif len(parts) == 3:
        first_part = parts[0]
        second_part = parts[1]
        third_part = parts[2]
        fourth_part = None  # Or '' if you prefer an empty string
    else:
        # Handle the case with fewer than 3 parts
        first_part = parts[0] if len(parts) > 0 else None
        second_part = parts[1] if len(parts) > 1 else None
        third_part = parts[2] if len(parts) > 2 else None
        fourth_part = None  # Or however you want to handle this case
    return first_part, second_part, third_part, fourth_part

def extract_accommodation_facilities(driver):
    # 모든 "css-i3rab1" 클래스를 가진 요소를 가져옴
    elements = driver.find_elements(By.CLASS_NAME, "css-i3rab1")

    # 각 요소의 텍스트를 가져와서 리스트에 저장
    texts = [element.text for element in elements]

    # 텍스트를 ','로 구분하여 하나의 문자열로 합침
    Facilities_text = ", ".join(texts)
    return Facilities_text

def update_detail_dataframe(accommodation_id, Location_df, Facilities_df, first_part, second_part, third_part, fourth_part, Facilities, Category):
    Location_df = pd.concat([Location_df, pd.DataFrame({'ACCOMMODATION_ID': [accommodation_id], 
                                                        'ACCOMMODATION_LOCATION_MAJOR': [first_part], 
                                                        'ACCOMMODATION_LOCATION_MIDDLE': [second_part], 
                                                        'ACCOMMODATION_LOCATION_SUB': [third_part], 
                                                        'ACCOMMODATION_LOCATION_DETAIL': [fourth_part]})], ignore_index=True)
    
    Facilities_df = pd.concat([Facilities_df, pd.DataFrame({'ACCOMMODATION_ID': [accommodation_id],
                                                            'ACCOMMODATION_MAINCATEGORY' : [Category],
                                                            'ACCOMMODATION_FACILITIES': [Facilities]})], ignore_index=True)
    
    return Location_df, Facilities_df

def crawl_detail_page(driver, accommodation_id_category_list):
    Location_df, Facilities_df = Make_Detail_DataFrame()
    
    for accommodation_id, main_category in accommodation_id_category_list:
        url = f"https://www.yeogi.com/domestic-accommodations/{accommodation_id}" + "?"
        driver.get(url)
        
        time.sleep(2)
        
        print("crawlingurl : ", url)
        # 데이터 추출
        first_part, second_part, third_part, fourth_part = extract_accommodation_location(driver)
        Facilities = extract_accommodation_facilities(driver)
        
        Location_df, Facilities_df = update_detail_dataframe(accommodation_id, Location_df, Facilities_df, first_part, second_part, third_part, fourth_part, Facilities, main_category)
        print("crawlingurl : ", url , "end")
    return Location_df, Facilities_df

# Parquet 파일 저장 함수
def create_parquet_Detail(directory, Location_df, Facilities_df):
    # Accommodation 데이터프레임 저장
    Location_filename = os.path.join(directory, 'accommodation_Location_table.parquet')
    Location_df.to_parquet(Location_filename, engine='pyarrow', index=False)
    print(f"저장 완료: {Location_filename}")
    
    # Review 데이터프레임 저장
    Facilities_filename = os.path.join(directory, 'accommodation_Facilities_table.parquet')
    Facilities_df.to_parquet(Facilities_filename, engine='pyarrow', index=False)
    print(f"저장 완료: {Facilities_filename}")