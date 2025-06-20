import requests
import pandas as pd
from datetime import datetime
import json
import os
import psycopg2
from psycopg2.extras import execute_values
import time
from datetime import datetime, timedelta

# API 설정
API_KEY = "3845401692"
API_URL = "https://data.ex.co.kr/openapi/odtraffic/trafficAmountByCongest"

# PostgreSQL 연결 설정
POSTGRES_CONN = {
    'host': 'localhost',
    'port': '5432',
    'database': 'python_dataengineering',
    'user': 'postgres',
    'password': ''
}

def get_postgres_connection():
    """PostgreSQL 연결 생성 함수"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        return conn
    except Exception as e:
        print(f"PostgreSQL 연결 중 오류 발생: {str(e)}")
        raise

def create_table_if_not_exists(conn):
    """테이블이 없으면 생성하는 함수"""
    try:
        with conn.cursor() as cur:
            # public 스키마 사용 명시
            cur.execute("SET search_path TO public")
            
            # 테이블이 없을 경우에만 생성
            cur.execute("""
                CREATE TABLE IF NOT EXISTS highway_traffic (
                    id SERIAL PRIMARY KEY,
                    stddate VARCHAR(10),
                    stdhour VARCHAR(10),
                    vdsid VARCHAR(50),
                    trafficamout INTEGER,
                    speed INTEGER,
                    shareratio FLOAT,
                    timeavg INTEGER,
                    collection_time TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
        print("테이블 생성/확인 완료 (데이터베이스: python_dataengineering, 스키마: public)")
    except Exception as e:
        print(f"테이블 생성 중 오류 발생: {str(e)}")
        raise

def load_to_postgres(df):
    """PostgreSQL에 데이터를 로드하는 함수"""
    try:
        print("PostgreSQL 데이터 로드 시작")
        
        # PostgreSQL 연결
        conn = get_postgres_connection()
        
        # 테이블 생성 확인
        create_table_if_not_exists(conn)
        
        # 데이터 삽입
        with conn.cursor() as cur:
            # DataFrame을 튜플 리스트로 변환
            data = [tuple(x) for x in df.values]
            
            # 컬럼 목록
            columns = df.columns.tolist()
            
            # 중복 체크를 위한 쿼리
            check_duplicate_query = """
                SELECT COUNT(*) 
                FROM highway_traffic 
                WHERE stddate = %s 
                AND stdhour = %s 
                AND vdsid = %s
            """
            
            # 중복되지 않은 데이터만 삽입
            new_records = []
            for record in data:
                cur.execute(check_duplicate_query, (record[0], record[1], record[2]))
                if cur.fetchone()[0] == 0:
                    new_records.append(record)
            
            if new_records:
                # INSERT 쿼리 생성
                insert_query = f"""
                    INSERT INTO highway_traffic ({', '.join(columns)})
                    VALUES %s
                """
                
                # 중복되지 않은 데이터만 삽입
                execute_values(cur, insert_query, new_records)
                conn.commit()
            
            # 전체 레코드 수 조회
            cur.execute("SELECT COUNT(*) FROM highway_traffic")
            total_records = cur.fetchone()[0]
            
            # 데이터 확인을 위한 쿼리 실행
            cur.execute("""
                SELECT COUNT(*) total_count,
                       COUNT(DISTINCT collection_time) unique_times,
                       MIN(collection_time) oldest_record,
                       MAX(collection_time) newest_record
                FROM highway_traffic
            """)
            stats = cur.fetchone()
            print(f"\n=== 데이터베이스 현황 ===")
            print(f"전체 레코드 수: {stats[0]}")
            print(f"고유 수집 시간 수: {stats[1]}")
            print(f"가장 오래된 데이터: {stats[2]}")
            print(f"최신 데이터: {stats[3]}")
        
        print(f"\nPostgreSQL에 데이터 로드 완료: {len(new_records)}개 레코드")
        return f"{len(new_records)}개의 레코드가 추가되었습니다. 현재 총 레코드는 {total_records}개 입니다."
        
    except Exception as e:
        print(f"PostgreSQL 데이터 로드 중 오류 발생: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

def get_highway_data():
    """고속도로 차량 데이터를 가져오는 함수"""
    try:
        print("고속도로 차량 데이터 요청 시작")
        
        # API 요청 파라미터
        params = {
            'key': API_KEY,
            'type': 'json'
        }
        
        response = requests.get(API_URL, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            print(f"데이터 수신 성공: {len(data.get('list', []))}개 레코드")
            return data
        else:
            raise Exception(f"API 요청 실패: {response.status_code}")
            
    except Exception as e:
        print(f"데이터 요청 중 오류 발생: {str(e)}")
        raise

def create_dataframe(data):
    """JSON 데이터를 pandas DataFrame으로 변환하는 함수"""
    try:
        # 데이터 리스트 추출
        traffic_list = data.get('list', [])
        
        # DataFrame 생성
        df = pd.DataFrame(traffic_list)
        
        # 필요한 컬럼만 선택
        selected_columns = ['stdDate', 'stdHour', 'vdsId', 'trafficAmout', 'speed', 'shareRatio', 'timeAvg']
        df = df[selected_columns]
        
        # 컬럼명 소문자로 변환
        df.columns = df.columns.str.lower()
        
        # 현재 수집 시간 추가
        df['collection_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 데이터 타입 변환
        numeric_columns = ['trafficamout', 'speed', 'shareratio', 'timeavg']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        print(f"DataFrame 생성 완료: {len(df)}행 x {len(df.columns)}열")
        print("\n데이터 샘플:")
        print(df.head())
        print("\n데이터 타입:")
        print(df.dtypes)
        
        return df
        
    except Exception as e:
        print(f"DataFrame 생성 중 오류 발생: {str(e)}")
        raise

def save_to_csv(df, filename='highway_traffic.csv'):
    """DataFrame을 CSV 파일로 저장하는 함수"""
    try:
        # output 디렉토리 생성
        output_dir = os.path.join(os.getcwd(), 'airflow', 'output')
        os.makedirs(output_dir, exist_ok=True)
        
        # CSV 파일 저장
        file_path = os.path.join(output_dir, filename)
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"CSV 파일 저장 완료: {file_path}")
    except Exception as e:
        print(f"CSV 파일 저장 중 오류 발생: {str(e)}")
        raise

def crawl_and_save():
    """데이터 수집 및 저장 함수"""
    try:
        current_time = datetime.now()
        print(f"\n=== 데이터 수집 시작: {current_time.strftime('%Y-%m-%d %H:%M:%S')} ===")
        
        # 데이터 가져오기
        data = get_highway_data()
        
        # DataFrame 생성
        df = create_dataframe(data)
        
        # PostgreSQL에 데이터 로드
        result = load_to_postgres(df)
        print(result)
        
        # CSV 파일로 저장
        save_to_csv(df)
        
    except Exception as e:
        print(f"데이터 수집 중 오류 발생: {str(e)}")

def main():
    """메인 실행 함수"""
    print("고속도로 교통량 데이터 수집 시작")
    print("1분 간격으로 실행됩니다. Ctrl+C를 눌러서 종료할 수 있습니다.")
    
    try:
        while True:
            crawl_and_save()
            time.sleep(60)  # 1분 대기
            
    except KeyboardInterrupt:
        print("\n프로그램을 종료합니다.")
    except Exception as e:
        print(f"실행 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()
