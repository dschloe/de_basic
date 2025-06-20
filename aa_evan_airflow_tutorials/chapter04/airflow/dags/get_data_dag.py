"""
가상의 Iris 데이터셋을 생성하여 Airflow 홈 디렉토리의 data 폴더에 저장하는 DAG
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import pandas as pd
import numpy as np

def print_hello():
    """간단한 인사 메시지를 출력합니다."""
    print("Hello World!")
    return "Hello World returned!"

def create_and_save_iris_data():
    """가상의 Iris 데이터셋을 생성하여 Airflow 홈 디렉토리의 data 폴더에 저장합니다."""
    # Airflow 홈 디렉토리 확인
    airflow_home = os.environ.get('AIRFLOW_HOME', '')
    print(f"Airflow 홈 디렉토리: {airflow_home}")
    
    # 현재 작업 디렉토리 확인
    current_dir = os.getcwd()
    print(f"현재 작업 디렉토리: {current_dir}")
    
    # 가상의 Iris 데이터셋 생성
    np.random.seed(42)  # 재현성을 위한 시드 설정
    
    # 각 종(species)별로 50개의 샘플 생성
    n_samples = 150
    species = ['setosa'] * 50 + ['versicolor'] * 50 + ['virginica'] * 50
    
    # 각 종별로 다른 특성 범위를 가지도록 설정
    sepal_length = []
    sepal_width = []
    petal_length = []
    petal_width = []
    
    for s in species:
        if s == 'setosa':
            sepal_length.append(np.random.uniform(4.5, 5.5))
            sepal_width.append(np.random.uniform(3.0, 4.0))
            petal_length.append(np.random.uniform(1.0, 1.7))
            petal_width.append(np.random.uniform(0.1, 0.3))
        elif s == 'versicolor':
            sepal_length.append(np.random.uniform(5.5, 6.5))
            sepal_width.append(np.random.uniform(2.5, 3.0))
            petal_length.append(np.random.uniform(3.5, 4.5))
            petal_width.append(np.random.uniform(1.0, 1.5))
        else:  # virginica
            sepal_length.append(np.random.uniform(6.5, 7.5))
            sepal_width.append(np.random.uniform(3.0, 3.5))
            petal_length.append(np.random.uniform(5.0, 6.0))
            petal_width.append(np.random.uniform(1.5, 2.5))
    
    # 데이터프레임 생성
    iris_data = {
        'sepal_length': sepal_length,
        'sepal_width': sepal_width,
        'petal_length': petal_length,
        'petal_width': petal_width,
        'species': species
    }
    
    iris_df = pd.DataFrame(iris_data)
    print(f"가상 Iris 데이터셋 생성 완료: {len(iris_df)} 행, {len(iris_df.columns)} 열")
    
    # 데이터 헤드 출력
    print("\n=== 가상 Iris 데이터셋 헤드 ===")
    print(iris_df.head())
    
    # Airflow 홈 디렉토리에 data 폴더 생성
    data_dir = os.path.join(airflow_home, "data")
    os.makedirs(data_dir, exist_ok=True)
    
    # iris.csv 파일 저장 경로 설정
    save_path = os.path.join(data_dir, "iris.csv")
    
    try:
        # CSV 파일로 저장
        iris_df.to_csv(save_path, index=False)
        
        # 파일이 성공적으로 저장되었는지 확인
        if os.path.exists(save_path):
            file_size = os.path.getsize(save_path)
            print(f"\n파일이 성공적으로 저장되었습니다: {save_path}")
            print(f"파일 크기: {file_size} 바이트")
            return f"가상 Iris 데이터 저장 완료: {save_path}"
        else:
            print(f"\n경고: 파일이 저장되었지만 {save_path}에서 찾을 수 없습니다.")
            return "파일 저장 실패: 파일을 찾을 수 없음"
    except Exception as e:
        print(f"\n파일 저장 중 오류 발생: {e}")
        return f"오류: {e}"

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
    'get_iris_data',
    default_args=default_args,
    description='Hello World와 가상 Iris 데이터를 저장하는 DAG',
    schedule_interval=None,  # 수동 트리거만 허용
    catchup=False,
)

# 태스크 정의
hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)

get_data_task = PythonOperator(
    task_id='get_iris_data_task',
    python_callable=create_and_save_iris_data,
    dag=dag,
)

# 태스크 순서 설정
hello_task >> get_data_task 