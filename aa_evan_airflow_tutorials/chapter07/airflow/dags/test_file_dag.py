from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

def test_file_creation():
    """간단한 파일 생성 테스트"""
    # Airflow 홈 디렉토리 확인
    airflow_home = os.environ.get('AIRFLOW_HOME', '')
    print(f"Airflow 홈 디렉토리: {airflow_home}")
    
    # 현재 작업 디렉토리 확인
    current_dir = os.getcwd()
    print(f"현재 작업 디렉토리: {current_dir}")
    
    # 저장 위치 설정
    # 1. Airflow 홈 디렉토리
    airflow_data_dir = os.path.join(airflow_home, "data")
    os.makedirs(airflow_data_dir, exist_ok=True)
    
    results = []
    
    # Airflow 홈 디렉토리의 data 폴더에 파일 생성
    try:
        test_path = os.path.join(airflow_data_dir, "airflow_test.txt")
        with open(test_path, 'w') as f:
            f.write("This is a test file created by Airflow in the data directory")
        
        # 파일이 생성되었는지 확인
        if os.path.exists(test_path):
            file_size = os.path.getsize(test_path)
            results.append(f"성공: {test_path} (크기: {file_size} 바이트)")
        else:
            results.append(f"실패: {test_path} (파일이 존재하지 않음)")
    except Exception as e:
        results.append(f"오류: {airflow_data_dir} - {str(e)}")
    
    # 결과 출력
    for result in results:
        print(result)
    
    return "\n".join(results)

dag = DAG(
    'test_file_creation',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
)

test_task = PythonOperator(
    task_id='test_file_creation',
    python_callable=test_file_creation,
    dag=dag
) 