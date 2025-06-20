import streamlit as st
import pandas as pd
import plotly.express as px
import mysql.connector
import psycopg2
from streamlit_autorefresh import st_autorefresh
import pytz
from datetime import datetime, timedelta

KST = pytz.timezone('Asia/Seoul')

# 1분마다 자동 새로고침
st_autorefresh(interval=60000, key="refresh")

# MySQL 연결 정보
MYSQL_CONN = {
    'host': 'localhost',
    'port': '3306',
    'database': 'airflow_db',
    'user': 'root',
    'password': 'evan1234'
}

# PostgreSQL 연결 정보
POSTGRES_CONN = {
    'host': 'localhost',
    'port': '5432',
    'database': 'python_dataengineering',
    'user': 'postgres',
    'password': ''
}

def load_mysql_traffic_data():
    try:
        conn = mysql.connector.connect(**MYSQL_CONN)
        query = """
            SELECT 
                stddate,
                stdhour,
                AVG(trafficamout) as avg_traffic,
                COUNT(*) as data_count
            FROM airflow_highway_traffic
            WHERE stddate IS NOT NULL 
              AND stdhour IS NOT NULL 
              AND trafficamout IS NOT NULL
            GROUP BY stddate, stdhour
            ORDER BY stddate, stdhour
        """
        df = pd.read_sql(query, conn)
        if df.empty:
            return pd.DataFrame()
        df['datetime'] = pd.to_datetime(df['stddate'], format='%Y%m%d') + \
                         pd.to_timedelta(df['stdhour'].astype(str).str.zfill(4).str[:2].astype(int), unit='h') + \
                         pd.to_timedelta(df['stdhour'].astype(str).str.zfill(4).str[2:].astype(int), unit='m')
        df['datetime'] = df['datetime'].dt.tz_localize('Asia/Seoul')
        df = df.drop(['stddate', 'stdhour'], axis=1)
        df['avg_traffic'] = pd.to_numeric(df['avg_traffic'], errors='coerce')
        df['data_count'] = pd.to_numeric(df['data_count'], errors='coerce')
        df = df.dropna()
        return df
    except Exception as e:
        st.error(f"MySQL 데이터 로드 중 오류 발생: {str(e)}")
        return pd.DataFrame()
    finally:
        if 'conn' in locals():
            conn.close()

def load_postgres_traffic_data():
    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        query = """
            SELECT 
                TO_DATE(stddate, 'YYYYMMDD') as date,
                stdhour,
                AVG(speed) as avg_speed,
                COUNT(*) as data_count
            FROM airflow_highway_traffic
            WHERE stddate IS NOT NULL 
              AND stdhour IS NOT NULL 
              AND speed IS NOT NULL
            GROUP BY stddate, stdhour
            ORDER BY stddate, stdhour
        """
        df = pd.read_sql_query(query, conn)
        if df.empty:
            return pd.DataFrame()
        df['datetime'] = pd.to_datetime(df['date']) + \
                         pd.to_timedelta(df['stdhour'].astype(str).str.zfill(4).str[:2].astype(int), unit='h') + \
                         pd.to_timedelta(df['stdhour'].astype(str).str.zfill(4).str[2:].astype(int), unit='m')
        df['datetime'] = df['datetime'].dt.tz_localize('Asia/Seoul')
        df = df.drop(['date', 'stdhour'], axis=1)
        df['avg_speed'] = pd.to_numeric(df['avg_speed'], errors='coerce')
        df['data_count'] = pd.to_numeric(df['data_count'], errors='coerce')
        df = df.dropna()
        return df
    except Exception as e:
        st.error(f"PostgreSQL 데이터 로드 중 오류 발생: {str(e)}")
        return pd.DataFrame()
    finally:
        if 'conn' in locals():
            conn.close()

# 페이지 제목
st.title('고속도로 교통량 및 속도 모니터링')

# 두 개의 컬럼 생성
col1, col2 = st.columns(2)

# MySQL 데이터 표시
with col1:
    st.subheader('MySQL: 시간대별 평균 교통량')
    df_mysql = load_mysql_traffic_data()
    if not df_mysql.empty:
        fig1 = px.line(df_mysql, 
                      x='datetime', 
                      y='avg_traffic', 
                      title='시간대별 평균 교통량',
                      labels={'datetime': '시간', 'avg_traffic': '평균 교통량'})
        st.plotly_chart(fig1, use_container_width=True)
    else:
        st.info('MySQL 데이터가 없습니다.')

# PostgreSQL 데이터 표시
with col2:
    st.subheader('PostgreSQL: 시간대별 평균 속도')
    df_pg = load_postgres_traffic_data()
    if not df_pg.empty:
        fig2 = px.line(df_pg, 
                      x='datetime', 
                      y='avg_speed', 
                      title='시간대별 평균 속도',
                      labels={'datetime': '시간', 'avg_speed': '평균 속도 (km/h)'})
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info('PostgreSQL 데이터가 없습니다.')

# 페이지 하단에 업데이트 시간 표시
st.sidebar.markdown("---")
st.sidebar.info(f"마지막 업데이트: {datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M:%S')}")
