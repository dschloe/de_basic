from google.cloud import bigquery
import os

# 서비스 계정 키 경로
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/Admin/Desktop/de_basic/gcp_tutorial/lgu6h-project-09e19171e4dd.json"

# BigQuery 클라이언트 생성
client = bigquery.Client()

# BigQuery 테이블 정보
project_id = "lgu6h-project"
dataset_id = "sample_data"
table_id = "creditcard_fraud"
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# 로컬 CSV 파일 경로
file_path = "C:/Users/Admin/Desktop/de_basic/gcp_tutorial/creditcard.csv"

# 스키마 수동 지정 (Time과 Amount을 포함한 모든 컬럼을 FLOAT64로 지정, Class만 INTEGER)
schema = [bigquery.SchemaField("Time", "FLOAT64")] + \
         [bigquery.SchemaField(f"V{i}", "FLOAT64") for i in range(1, 29)] + [
             bigquery.SchemaField("Amount", "FLOAT64"),
             bigquery.SchemaField("Class", "INTEGER"),
         ]

# 업로드 설정
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    schema=schema
)

# 파일 업로드
with open(file_path, "rb") as source_file:
    load_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

print("📤 업로드 중...")
load_job.result()
print("✅ 업로드 완료!")

# 결과 확인
table = client.get_table(table_ref)
print(f"📊 {table.num_rows} rows loaded to {table.project}.{table.dataset_id}.{table.table_id}")
