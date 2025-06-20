AIRFLOW_VERSION=2.8.0

# Python 버전을 3.11로 고정 설정
PYTHON_VERSION="3.11"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 3.0.0 with python 3.11: https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.11.txt

uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
# PostgreSQL 제공자 패키지 버전 명시
uv pip install apache-airflow-providers-postgres==5.7.1
uv pip install -r requirements.txt

export AIRFLOW_HOME=$(pwd)/airflow
echo "AIRFLOW_HOME: $AIRFLOW_HOME"