import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import datetime

def load_basic_data(total_accommodation_df, total_price_df, total_review_df):
    s3 = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='')
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    bucket_name = 'accommodation.table.bucket'
    
    # Accommodation 데이터프레임 -> parquet 파일 변환 -> s3 업로드
    buffer = io.BytesIO()
    table = pa.Table.from_pandas(total_accommodation_df)
    pq.write_table(table, buffer)
    buffer.seek(0)
    file_key = date + '_tables' + '/accommodation_table.parquet'
    s3.upload_fileobj(buffer, bucket_name, file_key)
    
    # Price 데이터프레임 -> parquet 파일 변환 -> s3 업로드
    buffer = io.BytesIO()
    table = pa.Table.from_pandas(total_price_df)
    pq.write_table(table, buffer)
    buffer.seek(0)
    file_key = date + '_tables' + '/accommodation_Price_table.parquet'
    s3.upload_fileobj(buffer, bucket_name, file_key)
    
    # Review 데이터프레임 -> parquet 파일 변환 -> s3 업로드
    buffer = io.BytesIO()
    table = pa.Table.from_pandas(total_review_df)
    pq.write_table(table, buffer)
    buffer.seek(0)
    file_key = date + '_tables' + '/accommodation_Review_table.parquet'
    s3.upload_fileobj(buffer, bucket_name, file_key)
    
    return None

def load_detail_data(Location_df, Facilities_df):
    s3 = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='')
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    bucket_name = 'accommodation.table.bucket'
    
    buffer = io.BytesIO()
    table = pa.Table.from_pandas(Location_df)
    pq.write_table(table, buffer)
    buffer.seek(0)
    file_key = date + '_tables' + '/accommodation_Location_table.parquet'
    s3.upload_fileobj(buffer, bucket_name, file_key)
    
    buffer = io.BytesIO()
    table = pa.Table.from_pandas(Facilities_df)
    pq.write_table(table, buffer)
    buffer.seek(0)
    file_key = date + '_tables' + '/accommodation_Facilities_table.parquet'
    s3.upload_fileobj(buffer, bucket_name, file_key)
    
