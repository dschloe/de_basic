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
export AIRFLOW_HOME=$(pwd)/airflow
```

- install_airflow.sh 파일 작성 

```bash
AIRFLOW_VERSION=3.0.0

# Python 버전을 3.11로 고정 설정
PYTHON_VERSION="3.11"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 3.0.0 with python 3.11: https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.11.txt

uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

- install_airflow.sh 파일 실행
```bash
chmod +x install_airflow.sh
./install_airflow.sh
```

- 설치 확인
```bash
$ airflow version
3.0.0
```

## 4. Airflow 실행
```bash
airflow standalone
```

## 5. DAG 테스트 실행 (예시 코드)
```bash 
# run your first task instance
airflow tasks test example_bash_operator runme_0 2015-01-01

# run a backfill over 2 days
airflow backfill create --dag-id example_bash_operator \
    --start-date 2015-01-01 \
    --end-date 2015-01-02
```

## 6. Airflow 서비스 실행 명령어 (프로젝트 진행 시)
```bash
# 1. 데이터베이스 마이그레이션 실행
airflow db migrate

# 2. 관리자 계정 생성
airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org

# 3. API 서버 실행 (기본 포트: 8080)
airflow api-server --port 8080

# 4. 스케줄러 실행 (DAG 실행 스케줄 관리)
airflow scheduler

# 5. DAG 프로세서 실행 (DAG 파일 처리)
airflow dag-processor

# 6. Triggerer 실행 (비동기 작업 처리)
airflow triggerer

```

## 7. 접속 확인 
- 웹 UI: http://localhost:8080
- 로그인 정보:
  - Username: admin
  - Password: admin

## 주의사항

- 시스템에 Python 3.13.1이 설치되어 있지만, Airflow는 아직 3.13을 지원하지 않습니다.
- uv를 통해 Python 3.11 가상환경을 생성하여 호환성 문제를 해결합니다.
- 항상 가상환경이 활성화된 상태에서 Airflow 명령어를 실행해야 합니다.
- 각 명령어는 순서대로 실행해야 합니다.

