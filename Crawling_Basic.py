from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from selenium.common.exceptions import NoSuchElementException
import time

import requests
import datetime

import os

import pandas as pd

def weekday_setting_url(base_url, Category, Keyword):
    # cron tab : 0 => 월요일
    monday_date = datetime.datetime.now().strftime("%Y-%m-%d") # cron 시 : datetime.datetime.now().strftime("%Y-%m-%d")
    friday_date = (datetime.datetime.now() + datetime.timedelta(days=4)).strftime("%Y-%m-%d") # cron 시 : (datetime.datetime.now() + datetime.timedelta(days=4)).strftime("%Y-%m-%d")
    
    weekday_parameter = f"keyword={Keyword}&autoKeyword=&checkIn={monday_date}&checkOut={friday_date}&personal=2&freeForm=false&category={Category}&" #page={page}
    
    url = base_url+weekday_parameter
    return url

def holiday_setting_url(base_url, Category, Keyword):
    saturday_date = (datetime.datetime.now() + datetime.timedelta(days=5)).strftime("%Y-%m-%d") # cron 시 : (datetime.datetime.now() + datetime.timedelta(days=5)).strftime("%Y-%m-%d")
    sunday_date = (datetime.datetime.now() + datetime.timedelta(days=6)).strftime("%Y-%m-%d") # cron 시 : (datetime.datetime.now() + datetime.timedelta(days=6)).strftime("%Y-%m-%d")
    
    holiday_parameter = f"keyword={Keyword}&autoKeyword=&checkIn={saturday_date}&checkOut={sunday_date}&personal=2&freeForm=false&category={Category}&" #page={page}
    
    url = base_url+holiday_parameter
    return url

#####################
# Setting Dataframe #
#####################
def Make_Basic_DataFrame():
    # Dataframe 셋팅
    accommodations_frame = {
        'ACCOMMODATION_ID': [],
        'ACCOMMODATION_MAINCATEGORY': [],
        'ACCOMMODATION_SUBCATEGORY': [],
        'ACCOMMODATION_NAME': []
    }
    accommodations_df = pd.DataFrame(accommodations_frame)
        
    price_frame = {
        'ACCOMMODATION_ID': [],
        'DATE_TYPE' : [],
        'PRICE': []
    }
    price_df = pd.DataFrame(price_frame)
        
    review_frame = {
        'ACCOMMODATION_ID': [],
        'ACCOMMODATION_RATING': [],
        'ACCOMMODATION_REVIEWCOUNT' : []
    }
    review_df = pd.DataFrame(review_frame)
    return accommodations_df, price_df, review_df

####################
# Crawling Setting #
####################
def crawling_setting():
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode (without GUI)
    chrome_options.add_argument("--no-sandbox")  # Bypass OS security model
    chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems

    # Initialize the Chrome driver
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    return driver

##################
# Crawling Basic #
##################
def extract_accommodation_price(element):  
    """Extract price from the accommodation element."""
    try:
        price_element = element.find_element(By.CLASS_NAME, "css-5r5920")
        price_text = price_element.text.strip().replace(',', '')  # Remove commas
        return int(price_text)  # Convert to integer
    except NoSuchElementException:
        print("Price element not found, skipping this element.")
        return None  # Return None if not found
    except Exception as e:
        print(f"An error occurred while extracting the price: {e}")
        return None  # Return None if any other error occurs

def extract_accommodation_data(element):
    """Extract various details from an accommodation element."""
    try:
        # Extract Accommodation ID
        href = element.get_attribute("href")
        Accommodation_ID = href.split('/')[-1].split('?')[0]

        # Extract Name
        Accommodation_Name = element.find_element(By.CLASS_NAME, "gc-thumbnail-type-seller-card-title").text.strip()

        # Extract Rating and Review
        review_rating = element.find_element(By.CLASS_NAME, "css-9ml4lz").text.strip()  # Rating
        review_count_text = element.find_element(By.CLASS_NAME, "css-oj6onp").text.strip()  # Review Count
        review_count_numeric = ''.join(filter(str.isdigit, review_count_text))  # Extract only digits
        review_count = int(review_count_numeric) if review_count_numeric else 0  # Default to 0 if empty

        # Extract Accommodation Subcategory
        ul_element = element.find_element(By.CLASS_NAME, "css-19akvy6")
        li_elements = ul_element.find_elements(By.TAG_NAME, "li")
        Accomodation_SubCategory = "_".join([li.text for li in li_elements])

        return Accommodation_ID, Accommodation_Name, review_rating, review_count, Accomodation_SubCategory
    
    except Exception as e:
        print(f"An error occurred while extracting data: {e}")
        return None  # Return None if any error occurs
    
def update_dataframes(accommodations_df, price_df, review_df, temp_accommodations, temp_prices, temp_reviews):
    """Update DataFrames with new data."""
    accommodations_df = pd.concat([accommodations_df, pd.DataFrame(temp_accommodations)], ignore_index=True)
    price_df = pd.concat([price_df, pd.DataFrame(temp_prices)], ignore_index=True)
    review_df = pd.concat([review_df, pd.DataFrame(temp_reviews)], ignore_index=True)
    return accommodations_df, price_df, review_df

def scrape_accommodation(driver, base_url, date_type, Category, Keyword_list):
    accommodation_df, price_df, review_df = Make_Basic_DataFrame()
    
    for Keyword in Keyword_list:
        print("Crawling Start KeyWord : ", Keyword )
        page = 1
        
        if date_type == "Weekday":
            Keyword_base_url = weekday_setting_url(base_url, Category, Keyword)
        elif date_type == "Holiday":
            Keyword_base_url = holiday_setting_url(base_url, Category, Keyword)   
        else:
            print("Date_Type Error")
            return None, None, None
        
        print("Crawling_baseurl : ", base_url)
        
        while True:
            url = Keyword_base_url+f"page={page}"
            print("crawling_url : ", url)
            driver.get(url)

            elements = driver.find_elements(By.CLASS_NAME, "gc-thumbnail-type-seller-card")
            
            if not elements:
                print("No more elements found. Exiting...")
                break
            
            temp_accommodations = []
            temp_prices = []
            temp_reviews = []
            
            for element in elements:
                # Extract price
                price = extract_accommodation_price(element)
                if price is None:
                    continue  # Skip this iteration if price is not found
                
                # Extract accommodation data
                extracted_data = extract_accommodation_data(element)
                if extracted_data is None:
                    continue  # Skip if there was an error extracting data
                
                Accommodation_ID, Accommodation_Name, review_rating, review_count, Accomodation_SubCategory = extracted_data
            
                temp_accommodations.append({
                        'ACCOMMODATION_ID': Accommodation_ID,
                        'ACCOMMODATION_MAINCATEGORY': Category_Mapping_table.get(Category),
                        'ACCOMMODATION_SUBCATEGORY': Accomodation_SubCategory,
                        'ACCOMMODATION_NAME': Accommodation_Name
                })

                temp_prices.append({
                    'ACCOMMODATION_ID': Accommodation_ID,
                    'DATE_TYPE': date_type,  # Use the provided date type
                    'PRICE': price
                })

                temp_reviews.append({
                    'ACCOMMODATION_ID': Accommodation_ID,
                    'ACCOMMODATION_RATING': review_rating,
                    'ACCOMMODATION_REVIEWCOUNT': review_count
                })
                
            accommodation_df, price_df, review_df = update_dataframes(accommodation_df, price_df, review_df, temp_accommodations, temp_prices, temp_reviews)
            page += 1
            
    return accommodation_df, price_df, review_df

# local -> boto3 s3로 변경 필요
def create_dir_if_not_exists():
    now = datetime.datetime.now().strftime("%Y-%m-%d")
    directory = "./" + now + "_Tables"
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory

# Parquet 파일 저장 함수
def create_parquet_Basic(directory, accommodation_df, review_df, price_df):
    # Accommodation 데이터프레임 저장
    accommodation_filename = os.path.join(directory, 'accommodation_table.parquet')
    accommodation_df.to_parquet(accommodation_filename, engine='pyarrow', index=False)
    print(f"저장 완료: {accommodation_filename}")
    
    # Review 데이터프레임 저장
    review_filename = os.path.join(directory, 'review_table.parquet')
    review_df.to_parquet(review_filename, engine='pyarrow', index=False)
    print(f"저장 완료: {review_filename}")
    
    # Price 데이터프레임 저장
    price_filename = os.path.join(directory, 'price_table.parquet')
    price_df.to_parquet(price_filename, engine='pyarrow', index=False)
    print(f"저장 완료: {price_filename}")
