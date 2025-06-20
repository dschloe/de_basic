from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import mysql.connector
import os

# macOS에서 Airflow 네트워크 요청 문제 해결
os.environ['NO_PROXY'] = '*'

# MySQL 연결을 위한 환경 변수 설정
os.environ['AIRFLOW_CONN_MYSQL_DEFAULT'] = 'mysql+pymysql://root:evan1234@localhost/python_dataengineering'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_mysql_table(**context):
    # MySQL 연결 설정
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='evan1234',
        database='airflow_db'
    )
    
    try:
        cursor = connection.cursor()
        # 테이블 생성
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS test_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            value INT
        );
        """
        cursor.execute(create_table_sql)
        connection.commit()
    finally:
        cursor.close()
        connection.close()

def insert_mysql_data(**context):
    # MySQL 연결 설정
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='evan1234',
        database='airflow_db'
    )
    
    try:
        cursor = connection.cursor()
        # 데이터 삽입
        insert_sql = """
        INSERT INTO test_table (name, value) VALUES
        ('test1', 100),
        ('test2', 200),
        ('test3', 300);
        """
        cursor.execute(insert_sql)
        connection.commit()
    finally:
        cursor.close()
        connection.close()

def process_data(**context):
    # Get data from MySQL
    mysql_connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='evan1234',
        database='airflow_db'
    )
    
    try:
        cursor = mysql_connection.cursor()
        cursor.execute("SELECT * FROM test_table")
        mysql_data = cursor.fetchall()
        print("MySQL Data:", mysql_data)
    finally:
        cursor.close()
        mysql_connection.close()
    
    # Get data from PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    postgres_data = postgres_hook.get_records("SELECT * FROM test_table")
    print("PostgreSQL Data:", postgres_data)

with DAG(
    'step10_mysql_postgres_example',
    default_args=default_args,
    description='A DAG demonstrating regular MySQL and PostgreSQL operators',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # MySQL Tasks using PythonOperator
    create_mysql_table_task = PythonOperator(
        task_id='create_mysql_table',
        python_callable=create_mysql_table,
        provide_context=True
    )

    insert_mysql_data_task = PythonOperator(
        task_id='insert_mysql_data',
        python_callable=insert_mysql_data,
        provide_context=True
    )

    # PostgreSQL Tasks
    create_postgres_table = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            value INT
        );
        """
    )

    insert_postgres_data = PostgresOperator(
        task_id='insert_postgres_data',
        postgres_conn_id='postgres_default',
        sql="""
        INSERT INTO test_table (name, value) VALUES
        ('test1', 100),
        ('test2', 200),
        ('test3', 300);
        """
    )

    # Process data from both databases
    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        provide_context=True
    )

    # Define task dependencies
    create_mysql_table_task >> insert_mysql_data_task
    create_postgres_table >> insert_postgres_data
    [insert_mysql_data_task, insert_postgres_data] >> process_data_task 