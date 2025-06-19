import streamlit as st
import mysql.connector
import pandas as pd
import os
import time

st.title("Streamlit + MySQL Demo (CRUD)")

db_config = {
    "host": os.environ.get("MYSQL_HOST", "localhost"),
    "port": int(os.environ.get("MYSQL_PORT", 3306)),
    "user": os.environ.get("MYSQL_USER", "testuser"),
    "password": os.environ.get("MYSQL_PASSWORD", "testpw"),
    "database": os.environ.get("MYSQL_DATABASE", "testdb"),
}

# MySQL 연결 함수 (재시도 포함)
def get_connection():
    max_tries = 10
    for i in range(max_tries):
        try:
            conn = mysql.connector.connect(**db_config)
            return conn
        except Exception as e:
            time.sleep(2)
    st.error(f"MySQL 연결 실패: {e}")
    return None

# 테이블 생성
def create_table():
    conn = get_connection()
    if conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                age INT
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()
        st.success("테이블(users) 생성 완료!")

# 데이터 추가
def insert_user(name, age):
    conn = get_connection()
    if conn:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO users (name, age) VALUES (%s, %s)", (name, age))
        conn.commit()
        cursor.close()
        conn.close()
        st.success("사용자 추가 완료!")

# 데이터 조회
def fetch_users():
    conn = get_connection()
    if conn:
        df = pd.read_sql("SELECT * FROM users", conn)
        conn.close()
        return df
    return pd.DataFrame()

# 데이터 수정
def update_user(user_id, name, age):
    conn = get_connection()
    if conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET name=%s, age=%s WHERE id=%s", (name, age, user_id))
        conn.commit()
        cursor.close()
        conn.close()
        st.success("사용자 정보 수정 완료!")

# 데이터 삭제
def delete_user(user_id):
    conn = get_connection()
    if conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM users WHERE id=%s", (user_id,))
        conn.commit()
        cursor.close()
        conn.close()
        st.success("사용자 삭제 완료!")

# UI
st.header("1. 테이블 생성")
if st.button("users 테이블 생성"):
    create_table()

st.header("2. 사용자 추가")
with st.form("add_user_form"):
    name = st.text_input("이름")
    age = st.number_input("나이", min_value=0, max_value=120, value=20)
    submitted = st.form_submit_button("추가")
    if submitted and name:
        insert_user(name, age)

st.header("3. 사용자 목록")
df = fetch_users()
st.dataframe(df)

if not df.empty:
    st.header("4. 사용자 정보 수정/삭제")
    selected = st.selectbox("수정/삭제할 사용자 선택", df["id"])
    user_row = df[df["id"] == selected].iloc[0]
    new_name = st.text_input("새 이름", value=user_row["name"], key="edit_name")
    new_age = st.number_input("새 나이", min_value=0, max_value=120, value=int(user_row["age"]), key="edit_age")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("수정"):
            update_user(selected, new_name, new_age)
    with col2:
        if st.button("삭제"):
            delete_user(selected)
