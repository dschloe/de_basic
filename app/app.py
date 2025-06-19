# streamlit 코드
# app/app.py
import streamlit as st
import requests

st.title("Iris Flower Classifier")

features = [st.slider(f"Feature {i+1}", 0.0, 10.0, 5.0) for i in range(4)]
if st.button("Predict"):
    response = requests.post("http://api:8000/predict", json={"features": features})
    st.write("예측 결과:", response.json()["prediction"])
