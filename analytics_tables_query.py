
    # 주중/주말별 평점 개수 (데이터셋 : wh_rate_cnt)
Analytics_1_query = """
    CREATE TABLE project2.analytics_table.wh_rate_cnt as
    SELECT date_type, rating_area, count(*) cnt
    FROM (
    SELECT A.date_type
        , case when B.accommodation_rating >= 0 and B.accommodation_rating <1 then '0~1'
            when B.accommodation_rating >= 1 and B.accommodation_rating <2 then '1~2'
            when B.accommodation_rating >= 2 and B.accommodation_rating <3 then '2~3'
            when B.accommodation_rating >= 3 and B.accommodation_rating <4 then '3~4'
            when B.accommodation_rating >= 4 and B.accommodation_rating <5 then '4~5'
            when B.accommodation_rating >= 5 and B.accommodation_rating <6 then '5~6'
            when B.accommodation_rating >= 6 and B.accommodation_rating <7 then '6~7'
            when B.accommodation_rating >= 7 and B.accommodation_rating <8 then '7~8'
            when B.accommodation_rating >= 8 and B.accommodation_rating <9 then '8~9'
            when B.accommodation_rating >= 9 and B.accommodation_rating <=10 then '9~10'
            end as "RATING_AREA"
    FROM project2.raw_data.accommodation_price A
    JOIN (
        SELECT accommodation_id, accommodation_rating
        FROM project2.raw_data.accommodation_review
        WHERE accommodation_rating is not null
    )B on A.accommodation_id = B.accommodation_id
    )x
    GROUP BY 1,2
    ORDER BY 1,2 desc;
    """
    
    # 주중/주말 지역별 숙박시설 개수, 주중/주말 가격 비교 (wh_loc_cate_price)
Analytics_2_query = """
    CREATE TABLE project2.analytics_table.wh_loc_cate_price as
    SELECT C.accommodation_location_major
        , C.accommodation_location_middle
        , A.date_type
        , B.accommodation_maincategory
        , A.price
    FROM project2.raw_data.accommodation_price A
    JOIN (
        SELECT distinct accommodation_id, accommodation_maincategory
        FROM project2.raw_data.accommodation_table
    )B on A.accommodation_id = B.accommodation_id
    JOIN (
        SELECT distinct accommodation_id
            , accommodation_location_middle
            , case when accommodation_location_major in ('전북', '전북특별자치도') then '전북'
                when accommodation_location_major in ('강원', '강원특별자치도', '강원도') then '강원'
                when accommodation_location_major in ('제주도', '제주특별자치도') then '제주도'
                when accommodation_location_major in ('세종', '세종특별자치시') then '세종'
                when accommodation_location_major in ('경상북도', '경북') then '경북'
                else accommodation_location_major end as accommodation_location_major
        FROM project2.raw_data.accommodation_location
    )C on B.accommodation_id = C.accommodation_id
    ORDER BY 1,2,3,4,5 desc;
    """
    
    # 주중/주말 메인카테고리별 가격 (wh_cate_price)
Analytics_3_query = """
    CREATE TABLE project2.analytics_table.wh_cate_price as
    SELECT A.date_type, B.accommodation_maincategory, round(avg(A.price),2) as avg_price
    FROM project2.raw_data.accommodation_price A
    JOIN (
        SELECT distinct accommodation_id, accommodation_maincategory
        FROM project2.raw_data.accommodation_table
    )B on A.accommodation_id = B.accommodation_id
    GROUP BY 1,2
    ORDER BY 1,2,3;
    """
    
    # 숙소빈도_시각화 (LOC_MAP_CODE_CNT)
Analytics_4_query = """
     SELECT COUNT(*) AS CNT,
        CASE
            WHEN MAJOR_LOCATION = '서울' THEN 'KR-11'
            WHEN MAJOR_LOCATION = '부산' THEN 'KR-26'
            WHEN MAJOR_LOCATION = '대구' THEN 'KR-27'
            WHEN MAJOR_LOCATION = '인천' THEN 'KR-28'
            WHEN MAJOR_LOCATION = '광주' THEN 'KR-29'
            WHEN MAJOR_LOCATION = '대전' THEN 'KR-30'
            WHEN MAJOR_LOCATION = '울산' THEN 'KR-31'
            WHEN MAJOR_LOCATION = '세종' THEN 'KR-50'
            WHEN MAJOR_LOCATION = '경기' THEN 'KR-41'
            WHEN MAJOR_LOCATION = '강원' THEN 'KR-42'
            WHEN MAJOR_LOCATION = '충북' THEN 'KR-43'
            WHEN MAJOR_LOCATION = '충남' THEN 'KR-44'
            WHEN MAJOR_LOCATION = '전북' THEN 'KR-45'
            WHEN MAJOR_LOCATION = '전남' THEN 'KR-46'
            WHEN MAJOR_LOCATION IN ('경상북도', '경북') THEN 'KR-47' -- Grouping '경상북도' and '경북' together
            WHEN MAJOR_LOCATION = '경남' THEN 'KR-48'
            WHEN MAJOR_LOCATION = '제주도' THEN 'KR-49'
            ELSE MAJOR_LOCATION -- in case there's an unlisted location
        END AS LOCATION_CODE
    FROM ANALYTICS_TABLE.FACILITIES_CNT_PRICE
    GROUP BY MAJOR_LOCATION;
    """
    
    # 지역별_카테고리별_리뷰수, 지역별_카테고리별_별점평균(CATE_REVIEW_BY_LOC)
Analytics_5_query = """
    SELECT A.ACCOMMODATION_MAINCATEGORY
        ,ROUND(AVG(B.ACCOMMODATION_RATING),3) AS AVG_RATING
        ,SUM(B.ACCOMMODATION_REVIEWCOUNT) AS SUM_REVIEWCOUNT
        ,C.MAJOR_LOCATION
    FROM RAW_DATA.ACCOMMODATION_TABLE A
    JOIN RAW_DATA.ACCOMMODATION_REVIEW B
    ON A.ACCOMMODATION_ID = B.ACCOMMODATION_ID
    JOIN (
        SELECT DISTINCT T.ID AS ACCOMMODATION_ID
            ,T.MAJOR_LOCATION
        FROM analytics_table.facilities_cnt_price T) C
    ON B.ACCOMMODATION_ID = C.ACCOMMODATION_ID
    GROUP BY C.MAJOR_LOCATION, A.ACCOMMODATION_MAINCATEGORY
    ORDER BY C.MAJOR_LOCATION, A.ACCOMMODATION_MAINCATEGORY
    """
    
    # 메인카테고리별 가격비교 막대그래프(CATE_AVG_PRICE_BY_DATETYPE)
Analytics_6_query = """
    SELECT
        A.ACCOMMODATION_MAINCATEGORY,
        ROUND(AVG(B.PRICE),0) AS PRICE,
        MIN(A.LOAD_TIMESTAMP) AS LOAD_TIMESTAMP,
        B.DATE_TYPE
    FROM RAW_DATA.ACCOMMODATION_TABLE A
    JOIN RAW_DATA.ACCOMMODATION_PRICE B
        ON A.ACCOMMODATION_ID = B.ACCOMMODATION_ID
    JOIN (
        SELECT
            ACCOMMODATION_ID,
            COUNT(*) AS RecordCount
        FROM RAW_DATA.ACCOMMODATION_PRICE
        GROUP BY ACCOMMODATION_ID
        HAVING COUNT(*) = 2
    ) C
        ON B.ACCOMMODATION_ID = C.ACCOMMODATION_ID
    GROUP BY A.ACCOMMODATION_MAINCATEGORY, B.DATE_TYPE
    ORDER BY B.DATE_TYPE DESC, PRICE, A.ACCOMMODATION_MAINCATEGORY;
    """
    
    # 지역 별 모텔 수, 지역 별 호텔 및 리조트 수, 지역 별 캠핑장 수, 총 숙박 시설 수
Analytics_7_query = """
    CREATE TABLE CATEGORY_BY_LOC AS
    SELECT
        SUM(CASE WHEN categories.ACCOMMODATION_MAINCATEGORY = 'Motel' THEN 1 ELSE 0 END) AS MOTEL,
        SUM(CASE WHEN categories.ACCOMMODATION_MAINCATEGORY = 'Hotel/Resort' THEN 1 ELSE 0 END) AS HOTEL_RESORT,
        SUM(CASE WHEN categories.ACCOMMODATION_MAINCATEGORY = 'Camping' THEN 1 ELSE 0 END) AS CAMPING,
        CASE
            WHEN location.ACCOMMODATION_LOCATION_MAJOR IN ('제주특별자치도', '제주도') THEN '제주'
            WHEN location.ACCOMMODATION_LOCATION_MAJOR IN ('세종특별자치시', '세종') THEN '세종'
            WHEN location.ACCOMMODATION_LOCATION_MAJOR IN ('전북특별자치도', '전북') THEN '전북'
            WHEN location.ACCOMMODATION_LOCATION_MAJOR IN ('강원특별자치도', '강원', '강원도') THEN '강원'
            WHEN location.ACCOMMODATION_LOCATION_MAJOR IN ('경상북도', '경북') THEN '경북'
            ELSE location.ACCOMMODATION_LOCATION_MAJOR
        END AS cleaned_location
    FROM
        PROJECT2.RAW_DATA.ACCOMMODATION_TABLE categories
    JOIN
        project2.raw_data.accommodation_location location
    ON
        categories.ACCOMMODATION_ID = location.ACCOMMODATION_ID
    GROUP BY
        cleaned_location;

    alter table category_by_loc add iso_code varchar(20);

    UPDATE category_by_loc SET iso_code = 'KR-41' WHERE cleaned_location = '경기';
    UPDATE category_by_loc SET iso_code = 'KR-49' WHERE cleaned_location = '제주';
    UPDATE category_by_loc SET iso_code = 'KR-44' WHERE cleaned_location = '충남';
    UPDATE category_by_loc SET iso_code = 'KR-50' WHERE cleaned_location = '세종';
    UPDATE category_by_loc SET iso_code = 'KR-28' WHERE cleaned_location = '인천';
    UPDATE category_by_loc SET iso_code = 'KR-27' WHERE cleaned_location = '대구';
    UPDATE category_by_loc SET iso_code = 'KR-30' WHERE cleaned_location = '대전';
    UPDATE category_by_loc SET iso_code = 'KR-11' WHERE cleaned_location = '서울';
    UPDATE category_by_loc SET iso_code = 'KR-43' WHERE cleaned_location = '충북';
    UPDATE category_by_loc SET iso_code = 'KR-48' WHERE cleaned_location = '경남';
    UPDATE category_by_loc SET iso_code = 'KR-26' WHERE cleaned_location = '부산';
    UPDATE category_by_loc SET iso_code = 'KR-45' WHERE cleaned_location = '전북';
    UPDATE category_by_loc SET iso_code = 'KR-31' WHERE cleaned_location = '울산';
    UPDATE category_by_loc SET iso_code = 'KR-29' WHERE cleaned_location = '광주';
    UPDATE category_by_loc SET iso_code = 'KR-42' WHERE cleaned_location = '강원';
    UPDATE category_by_loc SET iso_code = 'KR-47' WHERE cleaned_location = '경북';
    UPDATE category_by_loc SET iso_code = 'KR-46' WHERE cleaned_location = '전남';
    """
    
    # 지역 별 부대 시설 수(facilities_by_loc)
Analytics_8_query = """
    CREATE TABLE FACILITIES_BY_LOC AS
    SELECT
        SUM(CASE WHEN facilities.accommodation_facilities LIKE '%전기사용가능%' THEN 1 ELSE 0 END) AS electric,
        SUM(CASE WHEN facilities.accommodation_facilities LIKE '%사이트주차%' THEN 1 ELSE 0 END) AS parking,
        SUM(CASE WHEN facilities.accommodation_facilities LIKE '%바베큐%' THEN 1 ELSE 0 END) AS barbecue,
        SUM(CASE WHEN facilities.accommodation_facilities LIKE '%개수대%' THEN 1 ELSE 0 END) AS sink,
        SUM(CASE WHEN facilities.accommodation_facilities LIKE '%카드결제%' THEN 1 ELSE 0 END) AS credit_card,
        SUM(CASE WHEN facilities.accommodation_facilities LIKE '%무선인터넷%' THEN 1 ELSE 0 END) AS wifi,
        SUM(CASE WHEN facilities.accommodation_facilities LIKE '%공용샤워실%' THEN 1 ELSE 0 END) AS public_shower,
        SUM(CASE WHEN facilities.accommodation_facilities LIKE '%공용화장실%' THEN 1 ELSE 0 END) AS public_toilet,
        SUM(CASE WHEN facilities.accommodation_facilities LIKE '%에어컨%' THEN 1 ELSE 0 END) AS airconditioner,
        SUM(CASE WHEN facilities.accommodation_facilities LIKE '%반려견동반%' THEN 1 ELSE 0 END) AS with_pet,
        SUM(CASE WHEN facilities.accommodation_facilities LIKE '%물놀이시설%' THEN 1 ELSE 0 END) AS pool,
        SUM(CASE WHEN facilities.accommodation_facilities LIKE '%냉장고%' THEN 1 ELSE 0 END) AS refrigerator,
        SUM(CASE WHEN facilities.accommodation_facilities LIKE '%객실내취사%' THEN 1 ELSE 0 END) AS cook_inside,
        SUM(CASE WHEN facilities.accommodation_facilities LIKE '%카페%' THEN 1 ELSE 0 END) AS cafe,
        SUM(CASE WHEN facilities.accommodation_facilities LIKE '%픽업서비스%' THEN 1 ELSE 0 END) AS pickup_service,
        SUM(CASE WHEN facilities.accommodation_facilities LIKE '%편의점%' THEN 1 ELSE 0 END) AS convenience_store,
        SUM(CASE WHEN facilities.accommodation_facilities LIKE '%캠프파이어%' THEN 1 ELSE 0 END) AS campfire,
        SUM(CASE WHEN facilities.accommodation_facilities LIKE '%금연%' THEN 1 ELSE 0 END) AS cigarette,
        CASE
            WHEN location.ACCOMMODATION_LOCATION_MAJOR IN ('제주특별자치도', '제주도') THEN '제주'
            WHEN location.ACCOMMODATION_LOCATION_MAJOR IN ('세종특별자치시', '세종') THEN '세종'
            WHEN location.ACCOMMODATION_LOCATION_MAJOR IN ('전북특별자치도', '전북') THEN '전북'
            WHEN location.ACCOMMODATION_LOCATION_MAJOR IN ('강원특별자치도', '강원', '강원도') THEN '강원'
            WHEN location.ACCOMMODATION_LOCATION_MAJOR IN ('경상북도', '경북') THEN '경북'
            ELSE location.ACCOMMODATION_LOCATION_MAJOR
        END AS cleaned_location
    FROM
        PROJECT2.raw_data.ACCOMMODATION_FACILITIES facilities
    JOIN 
        PROJECT2.raw_data.ACCOMMODATION_LOCATION location
    ON 
        facilities.accommodation_id = location.accommodation_id
    GROUP BY
        cleaned_location;
    """
    
    # 지역 별 리뷰 평점 평균, 지역 별 카테고리 별 리뷰 평점 평균, 지역 별 리뷰 총 개수(facilities_by_loc)
Analytics_9_query = """
    CREATE TABLE RATING_BY_LOC AS
    SELECT
        CASE
            WHEN location.ACCOMMODATION_LOCATION_MAJOR IN ('제주특별자치도', '제주도') THEN '제주'
            WHEN location.ACCOMMODATION_LOCATION_MAJOR IN ('세종특별자치시', '세종') THEN '세종'
            WHEN location.ACCOMMODATION_LOCATION_MAJOR IN ('전북특별자치도', '전북') THEN '전북'
            WHEN location.ACCOMMODATION_LOCATION_MAJOR IN ('강원특별자치도', '강원', '강원도') THEN '강원'
            WHEN location.ACCOMMODATION_LOCATION_MAJOR IN ('경상북도', '경북') THEN '경북'  
            ELSE location.ACCOMMODATION_LOCATION_MAJOR
        END AS cleaned_location,
        reviews.ACCOMMODATION_RATING,
        categories.ACCOMMODATION_MAINCATEGORY
    FROM
        PROJECT2.RAW_DATA.ACCOMMODATION_LOCATION location
    JOIN
        PROJECT2.RAW_DATA.ACCOMMODATION_REVIEW reviews
    ON
        location.accommodation_id = reviews.accommodation_id
    JOIN
        PROJECT2.RAW_DATA.ACCOMMODATION_TABLE categories
    ON
        location.accommodation_id = categories.accommodation_id;

    alter table RATING_BY_LOC add iso_code varchar(20);

    UPDATE RATING_BY_LOC SET iso_code = 'KR-41' WHERE cleaned_location = '경기';
    UPDATE RATING_BY_LOC SET iso_code = 'KR-49' WHERE cleaned_location = '제주';
    UPDATE RATING_BY_LOC SET iso_code = 'KR-44' WHERE cleaned_location = '충남';
    UPDATE RATING_BY_LOC SET iso_code = 'KR-50' WHERE cleaned_location = '세종';
    UPDATE RATING_BY_LOC SET iso_code = 'KR-28' WHERE cleaned_location = '인천';
    UPDATE RATING_BY_LOC SET iso_code = 'KR-27' WHERE cleaned_location = '대구';
    UPDATE RATING_BY_LOC SET iso_code = 'KR-30' WHERE cleaned_location = '대전';
    UPDATE RATING_BY_LOC SET iso_code = 'KR-11' WHERE cleaned_location = '서울';
    UPDATE RATING_BY_LOC SET iso_code = 'KR-43' WHERE cleaned_location = '충북';
    UPDATE RATING_BY_LOC SET iso_code = 'KR-48' WHERE cleaned_location = '경남';
    UPDATE RATING_BY_LOC SET iso_code = 'KR-26' WHERE cleaned_location = '부산';
    UPDATE RATING_BY_LOC SET iso_code = 'KR-45' WHERE cleaned_location = '전북';
    UPDATE RATING_BY_LOC SET iso_code = 'KR-31' WHERE cleaned_location = '울산';
    UPDATE RATING_BY_LOC SET iso_code = 'KR-29' WHERE cleaned_location = '광주';
    UPDATE RATING_BY_LOC SET iso_code = 'KR-42' WHERE cleaned_location = '강원';
    UPDATE RATING_BY_LOC SET iso_code = 'KR-47' WHERE cleaned_location = '경북';
    UPDATE RATING_BY_LOC SET iso_code = 'KR-46' WHERE cleaned_location = '전남';

    select * from RATING_BY_LOC;
    """