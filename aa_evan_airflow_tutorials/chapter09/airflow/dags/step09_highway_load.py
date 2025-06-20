# 고속도로 차량 데이터 로드 
# https://data.ex.co.kr/openapi/apikey/requestKey
# 발급키 : 3845401692
# https://data.ex.co.kr/openapi/odtraffic/trafficAmountByCongest?key=3845401692&type=json

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json
import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import psycopg2
from psycopg2.extras import execute_values

# macOS에서 Airflow 네트워크 요청 문제 해결
os.environ['NO_PROXY'] = '*'

# API 키 설정
API_KEY = "3845401692"
API_URL = "https://data.ex.co.kr/openapi/odtraffic/trafficAmountByCongest"

# DAG의 기본 인자 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

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
                CREATE TABLE IF NOT EXISTS airflow_highway_traffic (
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
        print("테이블 생성/확인 완료 (데이터베이스: postgres, 스키마: public)")
    except Exception as e:
        print(f"테이블 생성 중 오류 발생: {str(e)}")
        raise

def create_output_dir():
    """output 디렉토리 생성"""
    try:
        current_dir = os.getcwd()
        output_dir = os.path.join(current_dir, 'airflow', 'output')
        os.makedirs(output_dir, exist_ok=True)
        print(f"output 디렉토리 생성됨: {output_dir}")
        return output_dir
    except Exception as e:
        print(f"디렉토리 생성 중 오류 발생: {str(e)}")
        raise

def download_highway_data(**context):
    """고속도로 차량 데이터를 다운로드하고 JSON으로 저장하는 함수"""
    try:
        print("고속도로 차량 데이터 다운로드 시작")
        
        # API 요청 파라미터
        params = {
            'key': API_KEY,
            'type': 'json'
        }
        
        response = requests.get(API_URL, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            print(f"데이터 수신 성공: {len(data.get('list', []))}개 레코드")
            
            # output 디렉토리에 JSON 파일 저장
            output_dir = create_output_dir()
            json_file_path = os.path.join(output_dir, 'highway_traffic.json')
            
            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            
            print(f"JSON 데이터 저장 완료: {json_file_path}")
            
            # XCom에 데이터 경로 저장
            context['task_instance'].xcom_push(key='json_file_path', value=json_file_path)
            return json_file_path
        else:
            raise Exception(f"API 요청 실패: {response.status_code}")
            
    except Exception as e:
        print(f"데이터 다운로드 중 오류 발생: {str(e)}")
        raise

def process_dataframe(**context):
    """JSON 데이터를 DataFrame으로 변환하고 CSV로 저장하는 함수"""
    try:
        # XCom에서 JSON 파일 경로 가져오기
        json_file_path = context['task_instance'].xcom_pull(task_ids='download_highway_data', key='json_file_path')
        
        # JSON 파일 읽기
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
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
        
        # CSV 파일로 저장
        output_dir = create_output_dir()
        csv_file_path = os.path.join(output_dir, 'highway_traffic.csv')
        df.to_csv(csv_file_path, index=False, encoding='utf-8-sig')
        print(f"CSV 파일 저장 완료: {csv_file_path}")
        
        # XCom에 DataFrame 정보 저장
        context['task_instance'].xcom_push(key='dataframe_info', value={
            'row_count': len(df),
            'column_count': len(df.columns),
            'csv_path': csv_file_path
        })
        
        return df.to_dict('records')
        
    except Exception as e:
        print(f"DataFrame 처리 중 오류 발생: {str(e)}")
        raise

def load_to_postgres(**context):
    """PostgreSQL에 데이터를 로드하는 함수"""
    try:
        print("PostgreSQL 데이터 로드 시작")
        
        # XCom에서 DataFrame 정보 가져오기
        dataframe_info = context['task_instance'].xcom_pull(task_ids='process_dataframe', key='dataframe_info')
        df = pd.DataFrame(context['task_instance'].xcom_pull(task_ids='process_dataframe'))
        
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
                FROM airflow_highway_traffic 
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
                    INSERT INTO airflow_highway_traffic ({', '.join(columns)})
                    VALUES %s
                """
                
                # 중복되지 않은 데이터만 삽입
                execute_values(cur, insert_query, new_records)
                conn.commit()
            
            # 전체 레코드 수 조회
            cur.execute("SELECT COUNT(*) FROM airflow_highway_traffic")
            total_records = cur.fetchone()[0]
            
            # 데이터 확인을 위한 쿼리 실행
            cur.execute("""
                SELECT COUNT(*) total_count,
                       COUNT(DISTINCT collection_time) unique_times,
                       MIN(collection_time) oldest_record,
                       MAX(collection_time) newest_record
                FROM airflow_highway_traffic
            """)
            stats = cur.fetchone()
            
            # XCom에 통계 정보 저장
            context['task_instance'].xcom_push(key='database_stats', value={
                'total_records': stats[0],
                'unique_times': stats[1],
                'oldest_record': stats[2],
                'newest_record': stats[3],
                'new_records_added': len(new_records)
            })
            
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

def print_data_summary(**context):
    """데이터 처리 결과를 출력하는 함수"""
    try:
        # XCom에서 각 단계의 정보 가져오기
        json_path = context['task_instance'].xcom_pull(task_ids='download_highway_data', key='json_file_path')
        df_info = context['task_instance'].xcom_pull(task_ids='process_dataframe', key='dataframe_info')
        db_stats = context['task_instance'].xcom_pull(task_ids='load_to_postgres', key='database_stats')
        
        print("\n=== 데이터 처리 결과 요약 ===")
        print(f"1. JSON 파일 경로: {json_path}")
        print(f"2. DataFrame 정보:")
        print(f"   - 행 수: {df_info['row_count']}")
        print(f"   - 열 수: {df_info['column_count']}")
        print(f"   - CSV 파일 경로: {df_info['csv_path']}")
        print(f"3. 데이터베이스 통계:")
        print(f"   - 전체 레코드 수: {db_stats['total_records']}")
        print(f"   - 고유 수집 시간 수: {db_stats['unique_times']}")
        print(f"   - 가장 오래된 데이터: {db_stats['oldest_record']}")
        print(f"   - 최신 데이터: {db_stats['newest_record']}")
        print(f"   - 새로 추가된 레코드 수: {db_stats['new_records_added']}")
        
    except Exception as e:
        print(f"데이터 요약 출력 중 오류 발생: {str(e)}")
        raise

# DAG 정의
dag = DAG(
    'step09_highway_load',
    default_args=default_args,
    description='고속도로 교통량 데이터 수집 및 처리 DAG',
    schedule_interval='*/1 * * * *',  # 1분마다 실행
    catchup=False,
    tags=['highway', 'traffic', 'postgres']
)

# 작업 정의
download_task = PythonOperator(
    task_id='download_highway_data',
    python_callable=download_highway_data,
    provide_context=True,
    dag=dag
)

process_task = PythonOperator(
    task_id='process_dataframe',
    python_callable=process_dataframe,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag
)

summary_task = PythonOperator(
    task_id='print_data_summary',
    python_callable=print_data_summary,
    provide_context=True,
    dag=dag
)

# 작업 순서 정의
download_task >> process_task >> load_task >> summary_task

