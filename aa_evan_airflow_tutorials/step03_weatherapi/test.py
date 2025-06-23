def get_weather_data(**context):
    # 캐시 및 재시도 포함한 세션 설정
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 37.57,
        "longitude": 126.98,
        "hourly": "temperature_2m",
        "current": "temperature_2m",
        "past_days": 7,
        "forecast_days": 1
    }
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    # 시간별 데이터 파싱
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ),
        "temperature_2m": hourly_temperature_2m
    }
    df = pd.DataFrame(data=hourly_data)
    df["date"] = df["date"].dt.tz_convert("Asia/Seoul")

    # KST 변환 및 파일명 생성
    execution_time = context['ts_nodash']
    utc_time = datetime.strptime(execution_time, "%Y%m%dT%H%M%S").replace(tzinfo=pendulum.UTC)
    kst = pendulum.timezone("Asia/Seoul")
    kst_time = utc_time.astimezone(kst)
    formatted_time = kst_time.strftime("%Y%m%d_%H%M%S")
    filename = f"result_{formatted_time}_weather.csv"
    output_dir = os.path.join(os.environ["AIRFLOW_HOME"], "output")
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)

    # 파일명 기준(KST)까지의 데이터만 저장
    kst_time_pd = pd.Timestamp(kst_time)
    filtered_df = df[df["date"] <= kst_time_pd].copy()

    try:
        filtered_df.to_csv(filepath, index=False)
        print(f"✅ 파일 저장 완료: {filepath}")
        return f"✅ 데이터 저장 완료: {filepath}"
    except Exception as e:
        print(f"❌ 파일 저장 중 오류 발생: {e}")
        raise

