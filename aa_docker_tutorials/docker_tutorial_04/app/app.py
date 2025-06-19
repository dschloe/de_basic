import streamlit as st
import numpy as np
import os
import joblib

MODEL_PATH = "mlartifacts/latest_model.pkl"

st.title("🌸 IRIS 종 분류기")

if not os.path.exists(MODEL_PATH):
    st.error("❌ 모델 파일이 없습니다. 먼저 학습(train.py)을 실행하세요.")
    st.stop()

model = joblib.load(MODEL_PATH)

sepal_length = st.slider("Sepal Length", 4.0, 8.0, 5.0)
sepal_width = st.slider("Sepal Width", 2.0, 4.5, 3.0)
petal_length = st.slider("Petal Length", 1.0, 7.0, 4.0)
petal_width = st.slider("Petal Width", 0.1, 2.5, 1.0)

if st.button("예측하기"):
    data = np.array([[sepal_length, sepal_width, petal_length, petal_width]])
    pred = model.predict(data)[0]
    st.success(f"예측 결과: {pred}번 클래스 (0=setosa, 1=versicolor, 2=virginica)")
