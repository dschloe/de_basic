# Airflow 및 필요한 라이브러리 임포트
from airflow import DAG  # DAG(Directed Acyclic Graph) 정의를 위한 Airflow 모듈
from airflow.operators.python import PythonOperator  # Python 함수를 태스크로 실행하기 위한 오퍼레이터
from datetime import datetime, timedelta  # 날짜 및 시간 관련 모듈
import requests  # HTTP 요청을 위한 라이브러리
from bs4 import BeautifulSoup  # HTML 파싱을 위한 라이브러리
import pandas as pd  # 데이터프레임 및 CSV 저장을 위한 라이브러리
import os  # 파일 및 디렉토리 경로 조작을 위한 표준 라이브러리
import pendulum

def crawl_to_csv(**context):
    """
    quotes.toscrape.com 사이트에서 명언, 저자, 태그를 크롤링하여
    실행 시각을 반영한 CSV 파일로 저장하는 함수.
    Airflow PythonOperator에서 호출됨.
    """
    # 크롤링 대상 URL 지정
    url = "http://quotes.toscrape.com/"
    # HTTP GET 요청으로 페이지 내용 가져오기
    response = requests.get(url)
    # BeautifulSoup을 사용하여 HTML 파싱
    soup = BeautifulSoup(response.text, "html.parser")
    
    # 모든 명언 블록(.quote) 선택
    quotes = soup.select(".quote")
    data = []  # 크롤링 결과를 저장할 리스트

    # 각 명언 블록에서 텍스트, 저자, 태그 추출
    for quote in quotes:
        text = quote.select_one(".text").text.strip()  # 명언 텍스트
        author = quote.select_one(".author").text.strip()  # 저자명
        tags = [tag.text for tag in quote.select(".tags .tag")]  # 태그 리스트
        # 추출한 정보를 딕셔너리로 저장
        data.append({"text": text, "author": author, "tags": ", ".join(tags)})

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


    # 여기 부분이 핵심임!!!
    # 결과 파일을 저장할 output 디렉토리 경로 생성 (AIRFLOW_HOME/output)
    # 환경변수에 대한 이해 필요
    output_dir = os.path.join(os.environ["AIRFLOW_HOME"], "output")
    os.makedirs(output_dir, exist_ok=True)  # 디렉토리가 없으면 생성

    # 결과 파일명 및 전체 경로 지정
    filename = f"result_{formatted_time}.csv"
    filepath = os.path.join(output_dir, filename)

    # 크롤링한 데이터를 pandas DataFrame으로 변환 후 CSV로 저장
    pd.DataFrame(data).to_csv(filepath, index=False)

# Airflow 문법
# DAG의 기본 인자 설정 (시작일 등)
defaut_args = {
    'start_date' : datetime(2024, 1, 1), # DAG의 시작 날짜
}

# Airflow DAG 정의 
with DAG(
    dag_id='crawl_quotes_to_csv2', # DAG의 고유 ID
    default_args=defaut_args, # 기본인자
    schedule_interval='* * * * *', # 1분마다 실행 (cron 표현식)
    catchup=False, # 과거 진행한 실행분은 무시
    max_active_runs=1, # 동시에 하나의 실행만 허용
    tags=["crawler"] # DAG 태그
) as dag:
    # PythonOperator를 사용하여 크롤링 함수 태스크 정의
    crawl_task = PythonOperator(
        task_id='crawl_to_csv2', # 태스크 ID
        python_callable=crawl_to_csv, # 실행할 함수
        provide_context=True, # Airflow 컨텍스트 함수에 전달
        execution_timeout=timedelta(minutes=10) # 태스크 최대 실행 시간(10분)
    )