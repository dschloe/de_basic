import streamlit as st
import pandas as pd
import numpy as np

# 페이지 설정
st.set_page_config(page_title="팀 선정 시스템", layout="wide")
st.title("🏆 팀 선정 시스템")

# teams.csv 파일 읽기
@st.cache_data
def load_data():
    return pd.read_csv('teams.csv')

df = load_data()

# 사이드바에 기본 정보 표시

st.header("프로젝트 정보")
st.write(f"프로젝트 총 참여 인원: {len(df)}명")


# 팀 선정 버튼
if st.button("🎯 팀 선정하기", type="primary", use_container_width=True):
    # 나이 기준으로 2등분 (median) 계산
    median_age = df['age'].median()
    
    # 중앙값 기준으로 2등분
    df['age_group'] = df['age'].apply(lambda x: '상위' if x >= median_age else '하위')
    
    # 상위/하위 그룹별로 무작위 팀 구성
    upper_group = df[df['age_group'] == '상위']['name'].tolist()
    lower_group = df[df['age_group'] == '하위']['name'].tolist()
    
    # 상위 그룹에서 4명 1팀, 5명 1팀으로 구성
    np.random.shuffle(upper_group)
    upper_teams = [upper_group[:4], upper_group[4:9]]  # 4명, 5명
    
    # 하위 그룹에서 4명 1팀, 5명 1팀으로 구성
    np.random.shuffle(lower_group)
    lower_teams = [lower_group[:4], lower_group[4:9]]  # 4명, 5명
    
    # 결과 표시

    st.header("🏆 팀 구성 결과")
    st.write(f"**나이 중앙값: {median_age}세**")

    # 상위 그룹 팀
    for i, team in enumerate(upper_teams, 1):
        team_members = ', '.join(team)
        st.write(f"**팀 {i}** ({len(team)}명): {team_members}")

    # 하위 그룹 팀
    for i, team in enumerate(lower_teams, 1):
        team_members = ', '.join(team)
        st.write(f"**팀 {i+2}** ({len(team)}명): {team_members}")

    # 성공 메시지
    st.success("✅ 팀 구성이 완료되었습니다!")

