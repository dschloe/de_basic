import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
from datetime import datetime
import pytz
from streamlit_autorefresh import st_autorefresh  # ✅ 추가

# 한국 시간대 설정
KST = pytz.timezone('Asia/Seoul')

# PostgreSQL 연결 설정
POSTGRES_CONN = {
    'host': 'localhost',
    'port': '5432',
    'database': 'python_dataengineering',
    'user': 'postgres',
    'password': ''
}

def get_postgres_connection():
    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        return conn
    except Exception as e:
        st.error(f"PostgreSQL 연결 중 오류 발생: {str(e)}")
        raise

def load_traffic_data():
    try:
        conn = get_postgres_connection()
        query = """
            SELECT 
                TO_DATE(stddate, 'YYYYMMDD') as date,
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
        df = pd.read_sql_query(query, conn)
        if df.empty:
            st.warning("데이터가 없습니다.")
            return pd.DataFrame()

        df['datetime'] = pd.to_datetime(df['date']) + \
                         pd.to_timedelta(df['stdhour'].astype(str).str.zfill(4).str[:2].astype(int), unit='h') + \
                         pd.to_timedelta(df['stdhour'].astype(str).str.zfill(4).str[2:].astype(int), unit='m')
        df['datetime'] = df['datetime'].dt.tz_localize('Asia/Seoul')
        df = df.drop(['date', 'stdhour'], axis=1)

        df['avg_traffic'] = pd.to_numeric(df['avg_traffic'], errors='coerce')
        df['data_count'] = pd.to_numeric(df['data_count'], errors='coerce')

        null_counts = df.isnull().sum()
        if null_counts.any():
            st.warning("결측치 발견:")
            st.write(null_counts)
            df = df.dropna()
            st.write("결측치 제거 후 데이터 건수:", len(df))
        return df
    except Exception as e:
        st.error(f"데이터 로드 중 오류 발생: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

def format_datetime_kst(dt):
    if pd.isna(dt):
        return ''
    return dt.strftime('%Y-%m-%d %H:%M:%S KST')

def main():
    # ✅ 자동 새로고침 (60,000ms = 60초)
    st_autorefresh(interval=60000, key="refresh")

    st.title('고속도로 교통량 대시보드')

    try:
        df = load_traffic_data()
        if not df.empty:
            st.subheader('데이터 현황')
            st.write(f"기간: {format_datetime_kst(df['datetime'].min())} ~ {format_datetime_kst(df['datetime'].max())}")
            st.write(f"총 데이터 포인트: {len(df):,}개")

            st.subheader('시간대별 평균 교통량')
            fig = px.line(df, 
                         x='datetime', 
                         y='avg_traffic',
                         title='시간대별 평균 교통량 추이',
                         labels={'datetime': '날짜/시간 (KST)', 
                                'avg_traffic': '평균 교통량',
                                'data_count': '데이터 수'})
            fig.update_layout(
                xaxis_title="날짜/시간 (KST)",
                yaxis_title="평균 교통량",
                hovermode='x unified',
                hoverlabel=dict(
                    bgcolor="white",
                    font_size=16,
                    font_family="Rockwell"
                )
            )
            fig.update_traces(
                hovertemplate="<br>".join([
                    "시간 (KST): %{x}",
                    "평균 교통량: %{y:.0f}",
                    "데이터 수: %{customdata[0]:.0f}"
                ]),
                customdata=df[['data_count']]
            )
            st.plotly_chart(fig, use_container_width=True)

            st.subheader('원본 데이터')
            display_df = df.copy()
            display_df['datetime'] = display_df['datetime'].apply(format_datetime_kst)
            st.dataframe(display_df.style.format({
                'avg_traffic': '{:.2f}',
                'data_count': '{:.0f}'
            }))
    except Exception as e:
        st.error(f"애플리케이션 실행 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()
