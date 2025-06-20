#!/bin/bash 

# 가상환경 활성화
source .venv/bin/activate

# Airflow 홈 설정 (프로젝트 디렉터리 최상위에서 출발)
export AIRFLOW_HOME=$(pwd)/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False  # 예제 DAG 제거
export AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT=600  # 추가

# 디렉터리 준비
mkdir -p $AIRFLOW_HOME/dags
mkdir -p $AIRFLOW_HOME/logs
mkdir -p $AIRFLOW_HOME/output

# output 파일 저장 할 수 있는 권한 부여
chmod 777 $AIRFLOW_HOME/output 

# 선택사항
# DAG 파일들을 복사 ==> airflow/dags 복사
cp -r dags/* $AIRFLOW_HOME/dags/

# Airflow 초기화 및 계정 생성
airflow db migrate
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com || true

# 스케줄러 실행 (백그라운드)
airflow scheduler &

SCHEDULER_PID=$!

airflow webserver --port 8080 &
WEBSERVER_PID=$!

# 7. 10분간 대기 후 종료
sleep 600
echo "10분 경과: Airflow 종료 중..."

kill $SCHEDULER_PID
kill $WEBSERVER_PID