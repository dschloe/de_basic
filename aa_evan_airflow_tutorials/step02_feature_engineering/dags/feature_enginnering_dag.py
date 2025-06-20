"""
보스턴 주택 데이터를 ETL 프로세스를 통해 가공하여 
스케일링된 데이터를 Airflow 홈 디렉토리의 data 폴더에 저장하는 DAG
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import pandas as pd
import numpy as np
import os  # 파일 및 디렉토리 경로 조작을 위한 표준 라이브러리
import pendulum

def print_hello():
    """간단한 인사 메시지를 출력합니다."""
    print("Hello World!")
    return "Hello World returned!"

def extract_boston_data():
    """
    Extract 단계: 보스턴 데이터를 로드합니다.
    """
    airflow_home = os.path.join(os.environ["AIRFLOW_HOME"], "datasets")
    data_path = os.path.join(airflow_home, "boston.csv")
    
    try:
        df = pd.read_csv(data_path)
        print(f"데이터 로드 완료: {len(df)} 행, {len(df.columns)} 열")
        return df.to_json()
    except Exception as e:
        print(f"데이터 로드 중 오류 발생: {e}")
        raise

def transform_boston_data(**context):
    """
    Transform 단계: 데이터 전처리 및 특성 엔지니어링을 수행합니다.
    1. 이상치 처리
    2. 특성 스케일링
    3. 새로운 특성 생성
    """
    # Extract 태스크에서 데이터 가져오기
    df = pd.read_json(context['task_instance'].xcom_pull(task_ids='extract_task'))
    
    # 1. 이상치 처리 (IQR 방식)
    def remove_outliers(df, column):
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        df[column] = df[column].clip(lower_bound, upper_bound)
        return df

    numeric_columns = df.select_dtypes(include=[np.number]).columns
    for col in numeric_columns:
        df = remove_outliers(df, col)

    # 2. 특성 스케일링 (Min-Max 스케일링)
    def min_max_scaling(df, column):
        min_val = df[column].min()
        max_val = df[column].max()
        df[f'{column}_scaled'] = (df[column] - min_val) / (max_val - min_val)
        return df

    # CHAS를 제외한 모든 수치형 특성에 대해 스케일링 적용
    for col in numeric_columns:
        if col != 'CHAS':  # CHAS는 이진 변수이므로 제외
            df = min_max_scaling(df, col)

    # 3. 새로운 특성 생성
    # 방 개수와 주택 연식의 상호작용 특성
    df['RM_AGE_interaction'] = df['RM'] * df['AGE']
    
    # 거리 관련 특성들의 평균
    df['distance_mean'] = df[['DIS', 'RAD']].mean(axis=1)
    
    # 환경 관련 특성들의 평균
    df['environment_score'] = df[['NOX', 'INDUS']].mean(axis=1)

    print("데이터 변환 완료")
    print(f"변환된 특성 수: {len(df.columns)}")
    
    return df.to_json()

def load_boston_data(**context):
    """
    Load 단계: 변환된 데이터를 CSV 파일로 저장합니다.
    """
    # Transform 태스크에서 데이터 가져오기
    df = pd.read_json(context['task_instance'].xcom_pull(task_ids='transform_task'))
    
    # Airflow 홈 디렉토리에 데이터 저장
    output_dir = os.path.join(os.environ["AIRFLOW_HOME"], "output")
    os.makedirs(output_dir, exist_ok=True)  # 디렉토리가 없으면 생성

    # Airflow 컨텍스트에서 실행 시각(ts_nodash) 가져오기 (예: 20250619T123000)
    execution_time = context['ts_nodash']
    
    # 실행 시각을 파일명에 쓸 수 있도록 포맷 변경 (예: 20250619_123000)
    # formatted_time = datetime.strptime(execution_time, "%Y%m%dT%H%M%S").strftime("%Y%m%d_%H%M%S")
    # 1. 문자열 → datetime 객체 (UTC)
    utc_time = datetime.strptime(execution_time, "%Y%m%dT%H%M%S").replace(tzinfo=pendulum.UTC)

    # 2. UTC → KST 변환
    kst = pendulum.timezone("Asia/Seoul")
    kst_time = utc_time.astimezone(kst)

    # 3. 파일명용 포맷팅
    formatted_time = kst_time.strftime("%Y%m%d_%H%M%S")
    filename = f"result_{formatted_time}_scaled_boston.csv"

    filepath = os.path.join(output_dir, filename)
    # 크롤링한 데이터를 pandas DataFrame으로 변환 후 CSV로 저장
    
    try:
        df.to_csv(filepath, index=False)
        print(f"파일 저장 완료: {filepath}")
        return f"데이터 저장 완료: {filepath}"
    except Exception as e:
        print(f"파일 저장 중 오류 발생: {e}")
        raise

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'step04_feature_engineering_boston',
    default_args=default_args,
    description='보스턴 주택 데이터에 대한 ETL 프로세스를 수행하는 DAG',
    schedule_interval=None,
    catchup=False,
)

# 태스크 정의
hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_boston_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_boston_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_boston_data,
    dag=dag,
)

# 태스크 순서 설정
hello_task >> extract_task >> transform_task >> load_task