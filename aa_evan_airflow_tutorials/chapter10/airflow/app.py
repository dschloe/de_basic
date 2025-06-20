from flask import Flask, render_template
import pandas as pd
import mysql.connector
import psycopg2
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

app = Flask(__name__)

# MySQL 연결 설정
MYSQL_CONN = {
    'host': 'localhost',
    'port': '3306',
    'database': 'airflow_db',
    'user': 'root',
    'password': 'evan1234'
}

# PostgreSQL 연결 설정
POSTGRES_CONN = {
    'host': 'localhost',
    'port': '5432',
    'database': 'python_dataengineering',
    'user': 'postgres',
    'password': ''
}

def get_mysql_data():
    """MySQL에서 데이터를 가져오는 함수"""
    try:
        conn = mysql.connector.connect(**MYSQL_CONN)
        query = """
            SELECT stddate, stdhour, vdsid, trafficamout, speed, shareratio, timeavg
            FROM airflow_highway_traffic
            ORDER BY stddate DESC, stdhour DESC
            LIMIT 1000
        """
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print(f"MySQL 데이터 가져오기 중 오류 발생: {str(e)}")
        return pd.DataFrame()
    finally:
        if 'conn' in locals():
            conn.close()

def get_postgres_data():
    """PostgreSQL에서 데이터를 가져오는 함수"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        query = """
            SELECT 
                stddate,
                stdhour,
                AVG(speed) as avg_speed,
                COUNT(*) as record_count
            FROM airflow_highway_traffic
            GROUP BY stddate, stdhour
            ORDER BY stddate DESC, stdhour DESC
            LIMIT 1000
        """
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print(f"PostgreSQL 데이터 가져오기 중 오류 발생: {str(e)}")
        return pd.DataFrame()
    finally:
        if 'conn' in locals():
            conn.close()

def create_mysql_visualization(df):
    """MySQL 데이터 시각화 생성"""
    if df.empty:
        return None
    
    # 시간별 교통량 시각화
    fig1 = px.line(df, x='stdhour', y='trafficamout', 
                  title='시간별 교통량',
                  labels={'stdhour': '시간', 'trafficamout': '교통량'})
    
    # 속도 분포 시각화
    fig2 = px.histogram(df, x='speed', 
                       title='속도 분포',
                       labels={'speed': '속도', 'count': '빈도'})
    
    # 교통량과 속도의 관계 시각화
    fig3 = px.scatter(df, x='trafficamout', y='speed',
                     title='교통량과 속도의 관계',
                     labels={'trafficamout': '교통량', 'speed': '속도'})
    
    return [fig1, fig2, fig3]

def create_postgres_visualization(df):
    """PostgreSQL 데이터 시각화 생성"""
    if df.empty:
        return None
    
    # 시간대별 평균 속도 시각화
    fig = go.Figure()
    
    # 평균 속도 선 그래프
    fig.add_trace(go.Scatter(
        x=df['stdhour'],
        y=df['avg_speed'],
        mode='lines+markers',
        name='평균 속도',
        line=dict(color='blue', width=2)
    ))
    
    # 레코드 수 막대 그래프 (보조 축)
    fig.add_trace(go.Bar(
        x=df['stdhour'],
        y=df['record_count'],
        name='데이터 수',
        yaxis='y2',
        opacity=0.3
    ))
    
    # 레이아웃 설정
    fig.update_layout(
        title='시간대별 평균 속도 및 데이터 수',
        xaxis_title='시간',
        yaxis_title='평균 속도 (km/h)',
        yaxis2=dict(
            title='데이터 수',
            overlaying='y',
            side='right'
        ),
        showlegend=True,
        height=600
    )
    
    return fig

@app.route('/')
def index():
    # MySQL 데이터 가져오기
    mysql_df = get_mysql_data()
    mysql_figs = create_mysql_visualization(mysql_df)
    
    # PostgreSQL 데이터 가져오기
    postgres_df = get_postgres_data()
    postgres_fig = create_postgres_visualization(postgres_df)
    
    # MySQL 시각화를 HTML로 변환
    mysql_plots = []
    if mysql_figs:
        for fig in mysql_figs:
            mysql_plots.append(fig.to_html(full_html=False))
    
    # PostgreSQL 시각화를 HTML로 변환
    postgres_plot = postgres_fig.to_html(full_html=False) if postgres_fig else None
    
    return render_template('index.html', 
                         mysql_plots=mysql_plots,
                         postgres_plot=postgres_plot)

if __name__ == '__main__':
    app.run(debug=True, port=5000) 