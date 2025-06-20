from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import os
from urllib.parse import urlparse
import urllib.parse
import json

# macOS에서 Airflow 네트워크 요청 문제 해결
os.environ['NO_PROXY'] = '*'

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

def setup_airflow_home():
    """Airflow 홈 디렉토리 설정 및 환경 변수 설정"""
    try:
        # 현재 작업 디렉토리 확인
        current_dir = os.getcwd()
        # Airflow 홈 디렉토리 설정
        airflow_home = os.path.join(current_dir, 'airflow')
        # 환경 변수 설정
        os.environ['AIRFLOW_HOME'] = airflow_home
        print(f"AIRFLOW_HOME 설정됨: {airflow_home}")
        return "AIRFLOW_HOME 환경 변수 설정 완료"
    except Exception as e:
        print(f"AIRFLOW_HOME 설정 중 오류 발생: {str(e)}")
        raise

def get_launch_images():
    """Launch Library 2 API에서 로켓 발사 이미지 URL을 가져오는 함수"""
    api_url = "https://ll.thespacedevs.com/2.2.0/launch/upcoming/?limit=5"
    print(f"API 요청 시작: {api_url}")
    
    try:
        print(f"NO_PROXY 환경변수: {os.environ.get('NO_PROXY')}")
        response = requests.get(api_url, timeout=30)
        print(f"API 응답 받음: 상태 코드 {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            launches = data['results']
            image_urls = [launch['image'] for launch in launches if launch.get('image')]
            print(f"총 {len(image_urls)}개의 이미지 URL 찾음")
            return image_urls
        else:
            raise Exception(f"API 요청 실패: {response.status_code}")
            
    except Exception as e:
        print(f"API 요청 중 오류 발생: {str(e)}")
        return [
            "https://spacelaunchnow-prod-east.nyc3.digitaloceanspaces.com/media/launch_images/falcon2520925_image_20230804070848.jpg",
            "https://spacelaunchnow-prod-east.nyc3.digitaloceanspaces.com/media/launcher_images/falcon_9_block__image_20210506060831.jpg"
        ]

def create_rocket_images_dir():
    """rocket_images 디렉토리 생성 - airflow 디렉토리 내에만 생성"""
    try:
        airflow_home = os.environ.get('AIRFLOW_HOME')
        if not airflow_home:
            raise Exception("AIRFLOW_HOME 환경변수가 설정되지 않았습니다.")
        
        # rocket_images 디렉토리 경로 설정 - airflow 디렉토리 내에 생성
        image_dir = os.path.join(airflow_home, 'rocket_images')
        os.makedirs(image_dir, exist_ok=True)
        print(f"rocket_images 디렉토리 생성됨: {image_dir}")
        return image_dir
    except Exception as e:
        print(f"디렉토리 생성 중 오류 발생: {str(e)}")
        raise

def create_output_dir():
    """output 디렉토리 생성 - airflow 디렉토리 내에만 생성"""
    try:
        airflow_home = os.environ.get('AIRFLOW_HOME')
        if not airflow_home:
            raise Exception("AIRFLOW_HOME 환경변수가 설정되지 않았습니다.")
        
        # output 디렉토리 경로 설정 - airflow 디렉토리 내에 생성
        output_dir = os.path.join(airflow_home, 'output')
        os.makedirs(output_dir, exist_ok=True)
        print(f"output 디렉토리 생성됨: {output_dir}")
        return output_dir
    except Exception as e:
        print(f"디렉토리 생성 중 오류 발생: {str(e)}")
        raise

def download_json_data():
    """Launch Library 2 API에서 JSON 데이터를 다운로드하는 함수"""
    try:
        print("JSON 데이터 다운로드 시작")
        api_url = "https://ll.thespacedevs.com/2.2.0/launch/upcoming/?limit=5"
        response = requests.get(api_url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            # output 디렉토리에 JSON 파일 저장
            output_dir = create_output_dir()
            json_file_path = os.path.join(output_dir, 'launch_data.json')
            
            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            
            print(f"JSON 데이터 저장 완료: {json_file_path}")
            return data
        else:
            raise Exception(f"API 요청 실패: {response.status_code}")
            
    except Exception as e:
        print(f"JSON 데이터 다운로드 중 오류 발생: {str(e)}")
        raise

def download_images():
    """로켓 발사 이미지를 다운로드하는 함수"""
    try:
        print("download_images 함수 시작")
        # JSON 데이터 가져오기
        data = download_json_data()
        launches = data['results']
        
        # AIRFLOW_HOME 환경변수 가져오기
        airflow_home = os.environ.get('AIRFLOW_HOME')
        if not airflow_home:
            raise Exception("AIRFLOW_HOME 환경변수가 설정되지 않았습니다.")
        
        # rocket_images 디렉토리 경로 설정 - airflow 디렉토리 내에 생성
        image_dir = os.path.join(airflow_home, 'rocket_images')
        os.makedirs(image_dir, exist_ok=True)
        print(f"저장 경로: {os.path.abspath(image_dir)}")
        
        downloaded_paths = []
        for idx, launch in enumerate(launches):
            try:
                if not launch.get('image'):
                    continue
                    
                url = launch['image']
                print(f"이미지 다운로드 시도 {idx+1}: {url}")
                response = requests.get(url, timeout=10)
                
                if response.status_code == 200:
                    # 파일 이름 생성 (로켓 이름과 날짜 사용)
                    rocket_name = launch.get('name', f'rocket_{idx + 1}')
                    launch_date = launch.get('net', '').split('T')[0]  # 날짜만 추출
                    safe_name = "".join(c for c in rocket_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
                    file_extension = os.path.splitext(urlparse(url).path)[1] or '.jpg'
                    file_name = f'{safe_name}_{launch_date}{file_extension}'
                    file_path = os.path.join(image_dir, file_name)
                    
                    with open(file_path, 'wb') as f:
                        f.write(response.content)
                    downloaded_paths.append(file_path)
                    print(f"이미지 저장 성공: {file_path}")
                else:
                    print(f"이미지 다운로드 실패 - 상태 코드: {response.status_code}")
            
            except Exception as e:
                print(f"개별 이미지 다운로드 실패 (URL: {url}): {str(e)}")
        
        return f"총 {len(downloaded_paths)}개의 이미지 다운로드 완료"
        
    except Exception as e:
        print(f"전체 프로세스 실패: {str(e)}")
        return f"오류 발생: {str(e)}"

# DAG 정의
dag = DAG(
    'step06_rocket_image_download',
    default_args=default_args,
    description='로켓 발사 이미지 다운로드',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# AIRFLOW_HOME 설정 태스크
setup_env_task = PythonOperator(
    task_id='setup_airflow_home',
    python_callable=setup_airflow_home,
    dag=dag
)

# Hello Airflow 태스크
hello_task = BashOperator(
    task_id='hello_task',
    bash_command='echo "Hello Airflow" && echo "AIRFLOW_HOME: $AIRFLOW_HOME"',
    dag=dag
)

# 디렉토리 생성 태스크
create_dir_task = PythonOperator(
    task_id='create_dir_task',
    python_callable=create_rocket_images_dir,
    dag=dag
)

# JSON 데이터 다운로드 태스크
download_json_task = PythonOperator(
    task_id='download_json_data',
    python_callable=download_json_data,
    dag=dag
)

# 이미지 다운로드 태스크
download_task = PythonOperator(
    task_id='download_rocket_images',
    python_callable=download_images,
    dag=dag
)

# Task 의존성 설정
setup_env_task >> hello_task >> create_dir_task >> download_json_task >> download_task