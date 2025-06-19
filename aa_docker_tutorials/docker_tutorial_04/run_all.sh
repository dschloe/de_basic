#!/bin/bash
mkdir -p app/mlartifacts app/mlflow_db

echo "[1/2] 디렉터리 권한 정리 중..."
sudo chown -R $USER:$USER ./app/mlartifacts ./app/mlflow_db

echo "[2/2] Docker Compose 전체 실행 시작 (mlflow → train → streamlit)..."
sudo docker compose up --build
