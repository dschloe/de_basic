FROM python:3.10-slim

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]
