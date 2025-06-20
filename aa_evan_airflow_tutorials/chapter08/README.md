# Airflow 설치 가이드 (with uv)

## 1. 프로젝트 환경 설정

1. 프로젝트 초기화 (Python 3.11 지정)
```bash
uv venv -p 3.11
source .venv/bin/activate
```

## 2. Python 버전 확인
 
```bash
python --version 
```

## 3. Airflow 설치
- Airflow 홈 디렉터리 설정 
```bash 
$ export AIRFLOW_HOME=$(pwd)/airflow
$ echo $AIRFLOW_HOME
/Users/evan/Desktop/airflow_tutorial/evan_airflow_tutorial/chapter04/airflow
```

- install_airflow.sh 파일 작성 

```bash
AIRFLOW_VERSION=2.8.0

# Python 버전을 3.11로 고정 설정
PYTHON_VERSION="3.11"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 3.0.0 with python 3.11: https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.11.txt

uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
uv pip install -r requirements.txt
```

- install_airflow.sh 파일 실행
```bash
chmod +x install_airflow.sh
./install_airflow.sh
```

- 설치 확인
```bash
$ airflow version
2.8.0
```

## 4. API 확인 
- 한국도로공사, 고속도로 공공데이터 포털
```bash 
https://data.ex.co.kr/openapi/basicinfo/openApiInfoM?apiId=0406&pn=-1
```

- 인증키 
    + 링크 : https://data.ex.co.kr/openapi/odtraffic/trafficAmountByCongest?key=3845401692&type=json


## 5. Airflow 실행

```
# 초기화
airflow db reset -y
airflow db init

# 사용자 생성
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password 1234

# webserver 실행
airflow webserver -p 8080

# scheduler 실행 (NEW 터미널 열기)
export AIRFLOW_HOME=$(pwd)/airflow
airflow scheduler
```


