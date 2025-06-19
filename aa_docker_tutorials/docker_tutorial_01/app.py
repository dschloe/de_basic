import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.datasets import load_iris

# Iris 데이터 로드
iris = load_iris()
df = pd.DataFrame(iris.data, columns=iris.feature_names)
df['target'] = iris.target

# Streamlit UI
st.title("🌸 Iris 데이터 시각화")
target_names = iris.target_names.tolist()
selected_class = st.selectbox("클래스 선택", target_names)

selected_index = target_names.index(selected_class)
filtered_df = df[df['target'] == selected_index]

# 시각화
fig, ax = plt.subplots()
ax.scatter(filtered_df[iris.feature_names[0]], filtered_df[iris.feature_names[1]])
ax.set_xlabel(iris.feature_names[0])
ax.set_ylabel(iris.feature_names[1])
ax.set_title(f'{selected_class} Sample Visualization')
st.pyplot(fig)
