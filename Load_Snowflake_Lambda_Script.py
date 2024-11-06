import snowflake.connector
from datetime import datetime

# Snowflake 연결 설정
ctx = snowflake.connector.connect(
    user='ehdgml7755',
    password='Dnflsk1!',
    account='uiqmasv-qub95347'
)

# 오늘 날짜를 'YYYY-MM-DD' 형식으로 가져오기
today_date = datetime.today().strftime('%Y-%m-%d')

# 커서 생성
cur = ctx.cursor()

try:
    #############################################
    # RAW_DATA 스키마 내 테이블 삭제 -> 다시 적재 #
    #############################################
    # 스키마 내 모든 테이블 목록 가져오기
    get_tables_query = """
    SELECT TABLE_NAME
    FROM PROJECT2.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'RAW_DATA';
    """

    cur.execute(get_tables_query)
    tables = cur.fetchall()

    # 테이블에 대해 DELETE 실행
    for table in tables:
        table_name = table[0]
        delete_table_query = f"DELETE FROM PROJECT2.RAW_DATA.{table_name};"
        cur.execute(delete_table_query)
        print(f"{table_name}의 데이터가 성공적으로 삭제되었습니다.")

    # 각 테이블에 대해 COPY INTO 실행
    for table in tables:
        table_name = table[0]
        parquet_file = f'{today_date}_Tables/{table_name}.parquet'  # parquet 파일 이름을 테이블 이름과 동일하게 설정

        # COPY INTO 구문 생성
        copy_query = f"""
        COPY INTO PROJECT2.RAW_DATA.{table_name}
        FROM 's3://accommodation.table.bucket/{parquet_file}'
        CREDENTIALS = (
            AWS_KEY_ID = 'AKIA46ZDFAAYUKP4JK6H',
            AWS_SECRET_KEY = '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'
        )
        FILE_FORMAT = (TYPE = 'PARQUET')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """

        # COPY INTO 쿼리 실행
        cur.execute(copy_query)
        print(f"{table_name} 테이블로 데이터가 성공적으로 복사되었습니다.")

    #####################################################
    # Analytics_Tables 스키마 내 테이블 삭제 -> 다시 적재 #
    #####################################################
    

finally:
    # 커서 및 연결 닫기
    cur.close()
    ctx.close()