from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
import os
import pendulum
import pytz

# 🧠 [함수] 날씨 데이터 수집 및 저장
def get_weather_data(**context):
    # ✅ 1. API 요청을 위한 세션 구성 (캐시 + 재시도)
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # ✅ 2. 요청 파라미터 (서울 기준)
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 37.57,         # 서울 위도
        "longitude": 126.98,       # 서울 경도
        "hourly": "temperature_2m",  # 시간별 2m 기온
        "current": "temperature_2m",
        "past_days": 7,
        "forecast_days": 1
    }

    # ✅ 3. API 요청 및 응답 처리
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]  # 하나의 지역만 처리

    # ✅ 4. 시간별 데이터 변환
    hourly = response.Hourly()
    temperature_2m = hourly.Variables(0).ValuesAsNumpy()

    time_range = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )

    df = pd.DataFrame({
        "date": time_range,
        "temperature_2m": temperature_2m
    })

    # ✅ 5. UTC → KST로 변환
    df["date"] = df["date"].dt.tz_convert("Asia/Seoul")

    # ✅ 6. Airflow 실행 시각 기반 KST 기준 파일명 생성
    execution_time = context['ts_nodash']
    utc_time = datetime.strptime(execution_time, "%Y%m%dT%H%M%S").replace(tzinfo=pendulum.UTC)
    kst_time = utc_time.astimezone(pendulum.timezone("Asia/Seoul"))
    formatted_time = kst_time.strftime("%Y%m%d_%H%M%S")

    # ✅ 7. 저장 경로 및 파일명 설정
    output_dir = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "output")
    os.makedirs(output_dir, exist_ok=True)
    filename = f"result_{formatted_time}_weather.csv"
    filepath = os.path.join(output_dir, filename)

    # ✅ 8. 현재 시각(KST) 기준으로 데이터 필터링
    now = datetime.now(pytz.timezone("Asia/Seoul"))
    filtered_df = df[df["date"] <= now]

    # ✅ 9. CSV 저장
    try:
        filtered_df.to_csv(filepath, index=False)
        print(f"✅ 파일 저장 완료: {filepath}")
        return f"✅ 데이터 저장 완료: {filepath}"
    except Exception as e:
        print(f"❌ 파일 저장 중 오류 발생: {e}")
        raise

# 📌 DAG 기본 설정
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# 📌 DAG 정의
with DAG(
    dag_id='get_weather_data',
    default_args=default_args,
    description='Open-Meteo API로 서울 날씨 수집 후 실행 시각 기준까지 저장',
    schedule_interval='0 * * * *',  # 매시 정각 실행
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'api', 'data_collection', 'raw', 'output']
) as dag:

    # 📌 PythonOperator 정의
    get_weather_task = PythonOperator(
        task_id='get_weather_data_from_api',
        python_callable=get_weather_data,
        provide_context=True  # context['ts_nodash'] 사용을 위함
    )
