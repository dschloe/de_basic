from google.cloud import bigquery
import os

# 서비스 계정 키 파일 경로 설정
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/Admin/Desktop/de_basic/gcp_tutorial/lgu6h-project-09e19171e4dd.json"

# BigQuery 클라이언트 생성
client = bigquery.Client()

# 테이블 지정
project_id = "lgu6h-project"  # 실제 GCP 프로젝트 ID로 바꾸기
dataset_id = "sample_data"     # BigQuery 데이터셋 이름
table_id = "iris_from_local"         # 업로드할 테이블 이름

table_ref = f"{project_id}.{dataset_id}.{table_id}"
file_path = "C:/Users/Admin/Desktop/de_basic/gcp_tutorial/iris.csv"  # 업로드할 로컬 파일 경로

# 업로드 설정
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True
)

# 데이터 업로드
with open(file_path, "rb") as source_file:
    load_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

print("업로드 중...")
load_job.result()  # 완료될 때까지 대기
print("업로드 완료!")

# 업로드 결과 출력
destination_table = client.get_table(table_ref)
print(f"📊 {destination_table.num_rows} rows loaded to {table_ref}")
