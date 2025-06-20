FROM python:3.11-slim

# 환경 변수 설정 (run_airflow.sh: export ...)
ENV AIRFLOW_HOME=/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT=600

WORKDIR /airflow

# requirements.txt, install_airflow.sh 복사
COPY requirements.txt install_airflow.sh ./
# dags, airflow 폴더 복사
COPY dags ./dags
COPY airflow ./airflow

# 권한 부여 (run_airflow.sh: chmod 777 $AIRFLOW_HOME/output)
RUN mkdir -p /airflow/dags /airflow/logs /airflow/output \
    && chmod 777 /airflow/output \
    # install_airflow.sh 실행 (run_airflow.sh: install 단계)
    && chmod +x install_airflow.sh \
    && ./install_airflow.sh \
    # dags 복사 (run_airflow.sh: cp -r dags/* $AIRFLOW_HOME/dags/)
    && cp -r /airflow/dags/* /airflow/dags/ || true

# Airflow 초기화 및 계정 생성 (run_airflow.sh: airflow db migrate, airflow users create ...)
RUN airflow db init \
    && airflow users create \
      --username admin \
      --password admin \
      --firstname Admin \
      --lastname User \
      --role Admin \
      --email admin@example.com || true

EXPOSE 8080

# Airflow 스케줄러/웹서버 실행 (run_airflow.sh: airflow scheduler & airflow webserver)
CMD ["bash", "-c", "airflow scheduler & airflow webserver --port 8080"]
